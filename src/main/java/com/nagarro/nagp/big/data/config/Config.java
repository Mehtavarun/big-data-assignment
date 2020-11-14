package com.nagarro.nagp.big.data.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class Config {

    @Autowired
    private ConfigProperties properties;

    @Bean
    public ConfigProperties configProperties() {
        return new ConfigProperties();
    }

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf().setMaster("local[*]").setAppName(properties.getSparkAppName());
    }

    @Bean
    @DependsOn({ "sparkConf" })
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }
}
