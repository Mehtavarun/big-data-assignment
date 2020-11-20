package com.nagp.big.data.config;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class KafkaConfig {

    @Autowired
    private ConfigProperties properties;

    @Bean
    public AdminClient adminClient() throws InterruptedException, ExecutionException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafkaUrl());
        AdminClient adminClient = AdminClient.create(configs);
        checkTopics(adminClient);
        return adminClient;
    }

    private void checkTopics(AdminClient adminClient) throws InterruptedException, ExecutionException {
        Map<String, TopicListing> availableTopics = adminClient.listTopics().namesToListings().get();
        properties.getTopics().forEach(topic -> {
            if (availableTopics.containsKey(topic)) {
                log.info("topic {} exists", topic);
            } else {
                throw new KafkaException("topic " + topic + " not found.");
            }
        });
    }

}
