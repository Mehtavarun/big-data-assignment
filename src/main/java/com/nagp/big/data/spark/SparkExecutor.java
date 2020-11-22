package com.nagp.big.data.spark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.PreDestroy;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.nagp.big.data.config.ConfigProperties;
import com.nagp.big.data.kafka.publisher.MessagePublisher;
import com.nagp.big.data.model.Invoice;

/**
 * @author varunmehta02
 *
 */
@Component
public class SparkExecutor implements ApplicationRunner {

    @Autowired
    private SparkSession spark;

    @Autowired
    private ConfigProperties properties;

    @Autowired
    private MessagePublisher messagePublisher;

    @Autowired
    private ExecutorService executorService;

    private static final String BIG_DATA_TOPIC = "nagp.bigdata";

    private void run() throws FileNotFoundException {
        Dataset<Invoice> invoicesCsv = encodeToBean(Invoice.class, getCsv());
        invoicesCsv.createOrReplaceTempView("Inv");
        writeToParquet(invoicesCsv, "invoices.parquet");
    }

    private void getInvoicesWithInvAmntGtThenInvAmnt(String outputParquetName)
            throws InterruptedException, ExecutionException {
        Dataset<Row> invoicesRowData = spark.sql(
                "SELECT * FROM Inv I1 WHERE I1.InvoiceTotal > (SELECT AVG(I2.InvoiceTotal) FROM INV I2 WHERE I1.InvoiceVendorName = I2.InvoiceVendorName)");
        Dataset<Invoice> invoiceData = encodeToBean(Invoice.class, invoicesRowData);
        processInvoiceData(() -> writeToParquet(invoiceData, outputParquetName), () -> sendData(invoiceData),
                () -> saveDataInCassandra(invoiceData));
    }

    private void getCountInvWithDateDiffBy1Y(String outputParquetName) throws InterruptedException, ExecutionException {
        Dataset<Row> invoiceParquet = readParquet("invoices.parquet");
        Dataset<Invoice> invoicesDateDiffGt1 = encodeToBean(Invoice.class, findInvoicesDateDiffGt1(invoiceParquet));
        processInvoiceData(() -> writeToParquet(invoicesDateDiffGt1, outputParquetName),
                () -> sendData(invoicesDateDiffGt1), () -> saveDataInCassandra(invoicesDateDiffGt1));
    }

    private void processInvoiceData(Runnable... runnables) throws InterruptedException, ExecutionException {
        List<Future<?>> futures = new LinkedList<>();
        for (Runnable runnable : runnables) {
            futures.add(executorService.submit(() -> runnable.run()));
        }
        for (Future<?> future : futures) {
            future.get();
        }
    }

    private Dataset<Row> findInvoicesDateDiffGt1(Dataset<Row> invoices) {
        return invoices.toDF()
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
    }

    private void saveDataInCassandra(Dataset<Invoice> dataset) {
        Map<String, String> map = new HashMap<>();
        map.put("keyspace", "nagp");
        map.put("table", "invoices");
        RDD<Invoice> rdd = dataset.rdd();
        spark.sqlContext().createDataFrame(rdd, Invoice.class).write().format("org.apache.spark.sql.cassandra")
                .mode(SaveMode.Append).options(map).save();
    }

    private void sendData(Dataset<Invoice> dataSet) {
        dataSet.collectAsList().forEach(invoice -> messagePublisher.sendMessage(BIG_DATA_TOPIC, invoice.toString(),
                UUID.randomUUID().toString()));
    }

    private Dataset<Row> getCsv() {
        String filepath = properties.getFilePath() + File.separator + properties.getFilename();
        Dataset<Row> invoicesCsv = spark.read().format("csv").option("header", "true").load(filepath)
                .withColumn("InvoiceTotal", functions.col("InvoiceTotal").cast("Double"))
                .withColumn("PaidAmt", functions.col("PaidAmt").cast("Double"));
        return invoicesCsv;
    }

    private static <T, R> Dataset<R> encodeToBean(Class<R> beanClass, Dataset<T> dataSet) {
        return dataSet.as(Encoders.bean(beanClass));
    }

    private Dataset<Row> readParquet(String parquetName) {
        return spark.read().parquet(parquetName);
    }

    private void writeToParquet(Dataset<Invoice> invoicesData, String filename) {
        invoicesData.write().format("parquet").mode(SaveMode.Overwrite)
                .save(properties.getFilePath() + File.separator + filename);
    }

    private void performUserQuery() throws IOException {
//        findInvoicesDateDiffGt1(testParquet);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        Dataset<Invoice> testParquet = readParquet("invoices.parquet").as(Encoders.bean(Invoice.class));
        testParquet.createOrReplaceTempView("Inv");
//        while (true) {
//            try {
//                spark.sql(br.readLine()).show();
//            } catch (Exception e) {
//                System.err.println(e.getMessage());
//            }
//            System.out.println();
//        }
        sendData(testParquet);
        saveDataInCassandra(testParquet);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        run();
//        performUserQuery();
        getInvoicesWithInvAmntGtThenInvAmnt("invoices_with_amnt_gt_invtotal.parquet");
        getCountInvWithDateDiffBy1Y("date_diff_gt_by_1.parquet");
//        getTop5HighestTurnoverVendors();
        System.exit(1);
    }

    @PreDestroy
    public void destroy() {
        if (Objects.nonNull(executorService)) {
            executorService.shutdown();
        }
    }
}
