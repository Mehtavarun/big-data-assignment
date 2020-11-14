package com.nagp.big.data.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;

@Getter
@ConfigurationProperties(prefix = "")
public class ConfigProperties {

    @Value("${spark.app-name}")
    private String sparkAppName;

    @Value("${file.path:#{systemProperties.get('user.dir')}}")
    private String filePath;

    @Value("${file.name}")
    private String filename;

}
