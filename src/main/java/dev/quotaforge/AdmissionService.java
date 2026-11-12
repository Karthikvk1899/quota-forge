package dev.quotaforge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

@Service
public class AdmissionService {
    public static final int SHARDS = 16; // Changing this requires an explicit state migration.
    public record Decision(int schemaVersion, String eventId, String requestId, String subject, boolean allowed,
                           int remaining, long retryAfterMs, long timestamp, int limit, long windowMs) {}
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final MeterRegistry meters;
    private final int limit;
    private final long window;
    private final int capacity;
    private final DefaultRedisScript<String> script = new DefaultRedisScript<>();

    public AdmissionService(StringRedisTemplate redis, ObjectMapper mapper, MeterRegistry meters,
            @Value("${app.limit}") int limit, @Value("${app.window-ms}") long window,
            @Value("${app.outbox-capacity}") int capacity) {
        if (limit < 1 || limit > 100000 || window < 100 || window > 3600000 || capacity < 1)
            throw new IllegalArgumentException("Invalid quota configuration");
        this.redis = redis; this.mapper = mapper; this.meters = meters;
        this.limit = limit; this.window = window; this.capacity = capacity;
        script.setLocation(new ClassPathResource("decide.lua")); script.setResultType(String.class);
    }
    public static String hash(String subject) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(subject.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    public static int shard(String hash) { return Integer.parseInt(hash.substring(0, 2), 16) % SHARDS; }
    public static String prefix(int shard) { return "qf:{s" + shard + "}:"; }
    public static String outbox(int shard) { return prefix(shard) + "outbox"; }
    public Decision decide(String subject, UUID requestId) {
        String h = hash(subject), prefix = prefix(shard(h));
        String result = redis.execute(script, List.of(prefix + "window:" + h,
            prefix + "request:" + h + ":" + requestId, outbox(shard(h)), prefix + "policy"),
            String.valueOf(limit), String.valueOf(window), String.valueOf(capacity),
            requestId.toString(), h, UUID.randomUUID().toString());
        if ("BACKLOG_FULL".equals(result) || "POLICY_CONFLICT".equals(result))
            throw new Unavailable(result);
        if (result == null) throw new Unavailable("EMPTY_REDIS_RESULT");
        try {
            Decision decision = mapper.readValue(result, Decision.class);
            meters.counter("quota_requests", "outcome", decision.allowed() ? "allowed" : "denied").increment();
            return decision;
        } catch (JsonProcessingException e) { throw new IllegalStateException("Invalid decision state", e); }
    }
    public int limit() { return limit; }
    public long window() { return window; }
    public int capacity() { return capacity; }
    public static final class Unavailable extends RuntimeException {
        public Unavailable(String reason) { super(reason); }
    }
}
