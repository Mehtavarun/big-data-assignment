package com.nagp.big.data.spark;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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
        Dataset<Row> invoicesCsv = getCsv();
        invoicesCsv.createOrReplaceTempView("invoices");
        Dataset<Row> invoicesData = spark.sql("SELECT InvoiceNo, InvoiceVendorName FROM INVOICES").limit(1);
        invoicesData.show();
//        writeToParquet(invoicesData, "test.parquet");
        Dataset<Row> testParquet = spark.read().parquet("test.parquet");
        testParquet.createOrReplaceTempView("testInvoicesParquet");
        spark.sql("SELECT InvoiceNo, InvoiceVendorName FROM testInvoicesParquet").limit(1).show();

    }

    private void writeToParquet(Dataset<Row> invoicesData, String filename) {
        invoicesData.write().format("parquet").mode(SaveMode.Overwrite)
                .save(properties.getFilePath() + File.separator + filename);
    }

    private Dataset<Row> getCsv() {
        String filepath = properties.getFilePath() + File.separator + properties.getFilename();
        Dataset<Row> invoicesCsv = spark.read().format("csv").option("header", "true").load(filepath);
        return invoicesCsv;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        run();
        System.exit(1);
    }

}
