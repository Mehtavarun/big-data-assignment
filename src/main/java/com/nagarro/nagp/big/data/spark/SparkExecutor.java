package com.nagarro.nagp.big.data.spark;

import java.io.File;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.nagarro.nagp.big.data.config.ConfigProperties;

@Component
public class SparkExecutor implements ApplicationRunner {

    @Autowired
    private JavaSparkContext javaSparkContext;

    @Autowired
    private ConfigProperties properties;

    private void run() {
        JavaRDD<String> textFile = javaSparkContext
                .textFile(properties.getFilePath() + File.separator + properties.getFilename());
        System.out.println(textFile.first());
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        run();
        System.exit(1);
    }

}
