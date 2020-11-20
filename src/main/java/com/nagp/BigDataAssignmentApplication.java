package com.nagp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.nagp.big.data.config.ConfigProperties;

@SpringBootApplication
public class BigDataAssignmentApplication {

    public static void main(String[] args) {
        SpringApplication.run(BigDataAssignmentApplication.class, args);
    }

    @Bean
    public ConfigProperties configProperties() {
        return new ConfigProperties();
    }
}
