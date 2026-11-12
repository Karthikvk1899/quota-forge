package dev.quotaforge;

import jakarta.servlet.FilterChain;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.filter.OncePerRequestFilter;

@Configuration
public class Security {
    @Bean SecurityFilterChain securityFilterChain(HttpSecurity http, @Value("${app.api-key}") String key) throws Exception {
        if (key.length() < 32) throw new IllegalArgumentException("QUOTA_API_KEY must contain at least 32 characters");
        byte[] expected = key.getBytes(StandardCharsets.UTF_8);
        OncePerRequestFilter bearer = new OncePerRequestFilter() {
            @Override protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
                    throws ServletException, IOException {
                String header = req.getHeader("Authorization");
                if (header != null && header.startsWith("Bearer ") && MessageDigest.isEqual(expected,
                        header.substring(7).getBytes(StandardCharsets.UTF_8))) {
                    var context = SecurityContextHolder.createEmptyContext();
                    context.setAuthentication(new UsernamePasswordAuthenticationToken("trusted-gateway", null,
                        List.of(new SimpleGrantedAuthority("ROLE_GATEWAY"))));
                    SecurityContextHolder.setContext(context);
                }
                chain.doFilter(req, res);
            }
        };
        return http.csrf(c -> c.disable()) // Stateless header token; no cookie-based authentication.
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(a -> a.dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                .anyRequest().authenticated())
            .exceptionHandling(e -> e.authenticationEntryPoint((req,res,ex) -> res.sendError(401)))
            .addFilterBefore(bearer, UsernamePasswordAuthenticationFilter.class).build();
    }
}
