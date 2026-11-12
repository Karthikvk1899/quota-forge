package dev.quotaforge;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class Api {
    public record Request(@NotNull @Pattern(regexp="[A-Za-z0-9._:@/-]{1,128}") String subject,
                          @NotNull UUID requestId) {}
    private final AdmissionService service;
    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    public Api(AdmissionService service, StringRedisTemplate redis, ObjectMapper mapper) {
        this.service = service; this.redis = redis; this.mapper = mapper;
    }
    @PostMapping("/decisions")
    public ResponseEntity<AdmissionService.Decision> decide(@Valid @RequestBody Request request) {
        var d = service.decide(request.subject(), request.requestId());
        var response = ResponseEntity.status(d.allowed() ? 200 : 429)
            .header("Cache-Control", "no-store")
            .header("X-RateLimit-Limit", "" + d.limit())
            .header("X-RateLimit-Remaining", "" + d.remaining());
        if (!d.allowed()) response.header(HttpHeaders.RETRY_AFTER, "" + Math.max(1, (d.retryAfterMs() + 999) / 1000));
        return response.body(d);
    }
    @GetMapping("/dashboard")
    public Map<String,Object> dashboard() throws JsonProcessingException {
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("policy", Map.of("limit", service.limit(), "windowMs", service.window(), "outboxCapacityPerShard", service.capacity()));
        result.put("totals", redis.opsForHash().entries(AuditPipeline.STATS));
        List<Long> backlog = new ArrayList<>();
        for (int s=0; s<AdmissionService.SHARDS; s++) backlog.add(redis.opsForList().size(AdmissionService.outbox(s)));
        result.put("backlog", backlog);
        List<AdmissionService.Decision> recent = new ArrayList<>();
        List<String> rows = redis.opsForList().range(AuditPipeline.RECENT, 0, 99);
        if (rows != null) for (String row : rows) recent.add(mapper.readValue(row, AdmissionService.Decision.class));
        result.put("recent", recent);
        return result;
    }
    @ExceptionHandler({DataAccessException.class, AdmissionService.Unavailable.class})
    ResponseEntity<Map<String,String>> unavailable(RuntimeException ex) {
        return ResponseEntity.status(503).header("Retry-After", "1")
            .body(Map.of("error", "Admission unavailable; retry with the same requestId"));
    }
}
