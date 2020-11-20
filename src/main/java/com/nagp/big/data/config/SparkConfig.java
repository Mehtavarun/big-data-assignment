package com.nagp.big.data.config;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class SparkConfig {

    @Autowired
    private ConfigProperties properties;

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf().setMaster("local[*]").setAppName(properties.getSparkAppName())
                .set("spark.sql.legacy.timeParserPolicy", "LEGACY");
    }

    @Bean
    @DependsOn({ "sparkConf" })
    public SparkContext sparkContext() {
        return new SparkContext(sparkConf());
    }

    @Bean
    @DependsOn("sparkContext")
    public SparkSession sparkSession() {
        return new SparkSession(sparkContext());
    }
}
