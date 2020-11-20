package com.nagp.big.data.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
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
        invoicesCsv.createOrReplaceTempView("inv");
        Dataset<Row> invoicesData = spark.sql(
                "SELECT * FROM Inv I1 WHERE I1.InvoiceTotal > (SELECT AVG(I2.InvoiceTotal) FROM INV I2 WHERE I1.InvoiceVendorName = I2.InvoiceVendorName)");
        invoicesData.show();
        writeToParquet(invoicesData, "invoices_with_amnt_gt_invtotal.parquet");
//        Dataset<Row> testParquet = spark.read().parquet("test.parquet");
//        testParquet.createOrReplaceTempView("testInvoicesParquet");
//        spark.sql("SELECT InvoiceNo, InvoiceVendorName FROM testInvoicesParquet").limit(1).show();
    }

    private void performUserQuery() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Dataset<Row> testParquet = spark.read().parquet("invoices.parquet");
        testParquet.createOrReplaceTempView("Inv");
        findInvoicesDateDiffGt1(testParquet);

        System.out.println();
//        while (true) {
//            try {
//                spark.sql(br.readLine()).show();
//            } catch (Exception e) {
//                System.err.println(e.getMessage());
//            }
//            System.out.println();
//        }
    }

    private void findInvoicesDateDiffGt1(Dataset<Row> testParquet) {
        Dataset<Row> dateDiffGt1 = testParquet.toDF()
                .filter(functions
                        .months_between(functions.when(functions
                                .to_date(functions.col("InvoiceRecvdDate"), "MM/dd/yyyy").isNotNull(),
                                functions.to_date(functions.col("InvoiceRecvdDate"), "MM/dd/yyyy"))
                                .otherwise(functions.when(functions
                                        .to_date(functions.col("InvoiceRecvdDate"), "M/d/yyyy").isNotNull(),
                                        functions.to_date(functions.col("InvoiceRecvdDate"), "M/d/yyyy"))
                                        .otherwise(functions.when(
                                                functions.to_date(functions.col("InvoiceRecvdDate"), "MM-dd-yyyy")
                                                        .isNotNull(),
                                                functions.to_date(functions.col("InvoiceRecvdDate"), "MM-dd-yyyy")))),
                                functions
                                        .when(functions.to_date(functions.col("InvoiceDate"), "MM/dd/yyyy")
                                                .isNotNull(),
                                                functions.to_date(functions.col("InvoiceDate"), "MM/dd/yyyy"))
                                        .otherwise(functions
                                                .when(functions.to_date(functions.col("InvoiceDate"),
                                                        "M/d/yyyy").isNotNull(),
                                                        functions.to_date(functions.col("InvoiceDate"), "M/d/yyyy"))
                                                .otherwise(functions.when(
                                                        functions.to_date(functions.col("InvoiceDate"), "MM-dd-yyyy")
                                                                .isNotNull(),
                                                        functions.to_date(functions.col("InvoiceDate"),
                                                                "MM-dd-yyyy")))),
                                true)
                        .divide(12).gt(1.00));
        writeToParquet(dateDiffGt1, "date_diff_gt_by_1.parquet");
    }

    private void writeToParquet(Dataset<Row> invoicesData, String filename) {
        invoicesData.write().format("parquet").mode(SaveMode.Overwrite)
                .save(properties.getFilePath() + File.separator + filename);
    }

    private Dataset<Row> getCsv() {
        String filepath = properties.getFilePath() + File.separator + properties.getFilename();
        Dataset<Row> invoicesCsv = spark.read().format("csv").option("header", "true").load(filepath)
                .withColumn("InvoiceTotal", functions.col("InvoiceTotal").cast("Double"))
                .withColumn("PaidAmt", functions.col("PaidAmt").cast("Double"))
//                .withColumn("InvoiceDate", functions.col("InvoiceDate").cast("Date"))
//                .withColumn("InvoiceRecvdDate", functions.col("InvoiceRecvdDate").cast("Date"))
//                .withColumn("ApprovedDate", functions.col("ApprovedDate").cast("Date"))
//                .withColumn("CreateDate", functions.col("CreateDate").cast("Timestamp"))
        ;
        return invoicesCsv;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
//        run();
        performUserQuery();
        System.exit(1);
    }
}
