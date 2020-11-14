package com.nagp.big.data.spark;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.nagp.big.data.config.ConfigProperties;

@Component
public class SparkExecutor implements ApplicationRunner {

    @Autowired
    private SparkSession spark;

    @Autowired
    private ConfigProperties properties;

    private void run() throws FileNotFoundException {
        String filepath = properties.getFilePath() + File.separator + properties.getFilename();
        Dataset<Row> invoices = spark.read().format("csv").option("sep", ";").option("inferSchema", "true")
                .option("header", "true").load(filepath);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        run();
        System.exit(1);
    }

}
