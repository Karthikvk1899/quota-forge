package dev.quotaforge;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AdmissionIT {
    static final String KEY = "integration-only-key-32-characters-long";
    @Container static GenericContainer<?> redisContainer = new GenericContainer<>("redis:7.4.2-alpine").withExposedPorts(6379);
    @Container static KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka:3.9.1");
    @DynamicPropertySource static void properties(DynamicPropertyRegistry r) {
        r.add("spring.data.redis.host", redisContainer::getHost);
        r.add("spring.data.redis.port", () -> redisContainer.getMappedPort(6379));
        r.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        r.add("app.api-key", () -> KEY);
        r.add("app.limit", () -> 10);
        r.add("app.window-ms", () -> 60000);
        r.add("app.relay-delay-ms", () -> 3600000); // Tests explicitly drive relay/recovery.
    }
    @Autowired AdmissionService service;
    @Autowired StringRedisTemplate redis;
    @Autowired ObjectMapper mapper;
    @Autowired AuditPipeline pipeline;
    @Autowired TestRestTemplate http;
    String subject() { return "test/" + UUID.randomUUID(); }
    AdmissionService custom(int limit, long window, int cap) {
        return new AdmissionService(redis, mapper, new SimpleMeterRegistry(), limit, window, cap);
    }
    String unusedSubject() {
        // Immutable per-shard policy requires a fresh shard for alternate policy tests.
        String value;
        do { value = subject(); } while (Boolean.TRUE.equals(redis.hasKey(
            AdmissionService.prefix(AdmissionService.shard(AdmissionService.hash(value))) + "policy")));
        return value;
    }
    @Test void concurrentRequestsNeverOverspend() throws Exception {
        String subject = subject();
        List<Callable<AdmissionService.Decision>> calls = new ArrayList<>();
        for (int i=0;i<80;i++) calls.add(() -> service.decide(subject, UUID.randomUUID()));
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = pool.invokeAll(calls);
            int allowed = 0;
            for (var future: results) if(future.get().allowed()) allowed++;
            assertThat(allowed).isEqualTo(10);
        }
    }
    @Test void retryReturnsSameDecisionAndOneOutboxEvent() {
        String subject=subject(); UUID id=UUID.randomUUID();
        var first=service.decide(subject,id);
        assertThat(service.decide(subject,id)).isEqualTo(first);
        var events=redis.opsForList().range(AdmissionService.outbox(AdmissionService.shard(first.subject())),0,-1);
        assertThat(events.stream().filter(e->e.contains(first.eventId())).count()).isEqualTo(1);
    }
    @Test void independentSubjectsHaveIndependentBudgets() {
        String a=subject(), b=subject();
        for(int i=0;i<10;i++) assertThat(service.decide(a,UUID.randomUUID()).allowed()).isTrue();
        assertThat(service.decide(a,UUID.randomUUID()).allowed()).isFalse();
        assertThat(service.decide(b,UUID.randomUUID()).allowed()).isTrue();
    }
    @Test void fullBacklogFailsWithoutSpendingPermit() {
        String subject=subject();
        var limited=custom(10,60000,1);
        int shard=AdmissionService.shard(AdmissionService.hash(subject));
        // Existing events may share this shard; drive them through Kafka first.
        pipeline.relay();
        redis.opsForList().rightPush(AdmissionService.outbox(shard),"capacity-test-sentinel");
        try {
            assertThatThrownBy(()->limited.decide(subject,UUID.randomUUID())).isInstanceOf(AdmissionService.Unavailable.class);
            assertThat(redis.opsForZSet().zCard(AdmissionService.prefix(shard)+"window:"+AdmissionService.hash(subject))).isZero();
        } finally { redis.opsForList().remove(AdmissionService.outbox(shard),1,"capacity-test-sentinel"); }
        assertThat(limited.decide(subject,UUID.randomUUID()).remaining()).isEqualTo(9);
    }
    @Test void expiredWindowRestoresCapacity() {
        String subject=unusedSubject();
        var fast=custom(1,150,10000);
        try {
            assertThat(fast.decide(subject,UUID.randomUUID()).allowed()).isTrue();
            assertThat(fast.decide(subject,UUID.randomUUID()).allowed()).isFalse();
            await().atMost(Duration.ofSeconds(3)).untilAsserted(()->assertThat(fast.decide(subject,UUID.randomUUID()).allowed()).isTrue());
        } finally {
            redis.delete(AdmissionService.prefix(AdmissionService.shard(AdmissionService.hash(subject)))+"policy");
        }
    }
    @Test void mixedReplicaPoliciesFailClosed() {
        String subject=subject(); service.decide(subject,UUID.randomUUID());
        assertThatThrownBy(()->custom(11,60000,10000).decide(subject,UUID.randomUUID()))
            .isInstanceOf(AdmissionService.Unavailable.class).hasMessage("POLICY_CONFLICT");
    }
    @Test void kafkaDeliveryAndDuplicateReplayAreIdempotent() throws Exception {
        var d=service.decide(subject(),UUID.randomUUID());
        String seen="qf:{projection}:seen:"+d.eventId();
        pipeline.relay();
        await().atMost(Duration.ofSeconds(30)).until(()->Boolean.TRUE.equals(redis.hasKey(seen)));
        // Simulate relay crashing after Kafka ACK and before Redis removal.
        redis.opsForList().rightPush(AdmissionService.outbox(AdmissionService.shard(d.subject())),mapper.writeValueAsString(d));
        pipeline.relay();
        await().atMost(Duration.ofSeconds(30)).until(()->redis.opsForList().size(AdmissionService.outbox(AdmissionService.shard(d.subject())))==0L);
        pipeline.project(mapper.writeValueAsString(d));
        var rows=redis.opsForList().range(AuditPipeline.RECENT,0,-1);
        assertThat(rows.stream().filter(v->v.contains(d.eventId())).count()).isEqualTo(1);
    }
    @Test void projectionRejectsUnknownEventSchema() throws Exception {
        var decision=service.decide(subject(),UUID.randomUUID());
        String payload=mapper.writeValueAsString(decision).replace("\"schemaVersion\":1", "\"schemaVersion\":2");
        assertThatThrownBy(()->pipeline.project(payload))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported decision schema version: 2");
    }
    @Test void httpAuthenticationValidationAndRateLimitContract() {
        assertThat(http.getForEntity("/api/dashboard",String.class).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        HttpHeaders headers=new HttpHeaders(); headers.setBearerAuth(KEY);headers.setContentType(MediaType.APPLICATION_JSON);
        String subject=subject();
        assertThat(http.postForEntity("/api/decisions",new HttpEntity<>("{\"subject\":\"bad space\",\"requestId\":\""+UUID.randomUUID()+"\"}",headers),String.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        for(int i=0;i<10;i++) service.decide(subject,UUID.randomUUID());
        var response=http.postForEntity("/api/decisions",new HttpEntity<>(new Api.Request(subject,UUID.randomUUID()),headers),String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(response.getHeaders().getFirst("Retry-After")).isNotBlank();
    }
}
