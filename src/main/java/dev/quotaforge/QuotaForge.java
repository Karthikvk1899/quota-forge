package dev.quotaforge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class QuotaForge {
    public static void main(String[] args) { SpringApplication.run(QuotaForge.class, args); }
}
