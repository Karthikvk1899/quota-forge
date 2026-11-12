package dev.quotaforge;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

@Component
public class AuditPipeline {
    private static final Logger log = LoggerFactory.getLogger(AuditPipeline.class);
    public static final String STATS = "qf:{projection}:stats";
    public static final String RECENT = "qf:{projection}:recent";
    private final StringRedisTemplate redis;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final MeterRegistry meters;
    private final String topic;
    private final DefaultRedisScript<Long> projection = new DefaultRedisScript<>();
    public AuditPipeline(StringRedisTemplate redis, KafkaTemplate<String, String> kafka,
                         ObjectMapper mapper, MeterRegistry meters, @Value("${app.topic}") String topic) {
        this.redis = redis; this.kafka = kafka; this.mapper = mapper; this.meters = meters; this.topic = topic;
        projection.setLocation(new ClassPathResource("project.lua")); projection.setResultType(Long.class);
        for (int s = 0; s < AdmissionService.SHARDS; s++) {
            final int shard = s;
            meters.gauge("quota_outbox_depth", List.of(io.micrometer.core.instrument.Tag.of("shard", "" + s)),
                this, pipeline -> {
                    try { Long n = redis.opsForList().size(AdmissionService.outbox(shard)); return n == null ? 0 : n; }
                    catch (RuntimeException e) { return Double.NaN; }
                });
        }
    }
    @Bean NewTopic decisionsTopic(@Value("${app.topic}") String topic,
                                  @Value("${app.topic-replicas}") int replicas) {
        return TopicBuilder.name(topic).partitions(16).replicas(replicas)
            .config("retention.ms", String.valueOf(Duration.ofDays(7).toMillis())).build();
    }
    @Bean DefaultErrorHandler kafkaErrorHandler() {
        // Never silently discard an audit event. Poison events block that partition and require operator repair.
        DefaultErrorHandler handler = new DefaultErrorHandler(new FixedBackOff(1000, FixedBackOff.UNLIMITED_ATTEMPTS));
        handler.setClassifications(Map.of(Exception.class, true), true);
        return handler;
    }
    @Scheduled(fixedDelayString = "${app.relay-delay-ms}")
    public void relay() {
        for (int s = 0; s < AdmissionService.SHARDS; s++) {
            try {
                List<String> pending = redis.opsForList().range(AdmissionService.outbox(s), 0, 31);
                if (pending == null) continue;
                for (String payload : pending) {
                    var decision = mapper.readValue(payload, AdmissionService.Decision.class);
                    kafka.send(topic, decision.subject(), payload).get(12, TimeUnit.SECONDS);
                    // Remove exactly the acknowledged payload. Concurrent relays may send duplicates,
                    // but cannot remove a different relay's unsent record (unlike a blind LPOP).
                    redis.opsForList().remove(AdmissionService.outbox(s), 1, payload);
                    meters.counter("quota_audit_published").increment();
                }
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            catch (Exception e) {
                meters.counter("quota_relay_failures").increment();
                log.warn("Audit relay deferred; shard={}, error={}", s, e.getClass().getSimpleName());
                return; // bounded retry cadence during broker outages
            }
        }
    }
    @KafkaListener(topics = "${app.topic}")
    public void project(String payload) throws Exception {
        var decision = mapper.readValue(payload, AdmissionService.Decision.class);
        if (decision.schemaVersion() != 1) {
            throw new IllegalArgumentException("Unsupported decision schema version: " + decision.schemaVersion());
        }
        UUIDCheck.validate(decision.eventId());
        redis.execute(projection, List.of("qf:{projection}:seen:" + decision.eventId(), STATS, RECENT),
            decision.allowed() ? "allowed" : "denied", payload);
    }
    private static class UUIDCheck {
        static void validate(String value) { java.util.UUID.fromString(value); }
    }
}
