package com.nagp.big.data.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Autowired
    private ConfigProperties properties;

    @Bean
    public ExecutorService getExecutorService() {
        return Executors.newFixedThreadPool(properties.getThreadCount());
    }
}
