package com.nagp.big.data.config;

import java.util.List;

import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;

@Getter
@ConfigurationProperties(prefix = "")
public class ConfigProperties {

    @Value("${executor.thread-count}")
    private int threadCount;

    @Value("${spark.app-name}")
    private String sparkAppName;

    @Value("${file.path:#{systemProperties.get('user.dir')}}")
    private String filePath;

    @Value("${file.name}")
    private String filename;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaUrl;

    @Value("#{${kafka.topics}}")
    private List<String> topics;

    @Value("${kafka.producer.key-serializer}")
    private Class<Serializer<?>> keySerializer;

    @Value("${kafka.producer.value-serializer}")
    private Class<Serializer<?>> valueSerializer;
}
