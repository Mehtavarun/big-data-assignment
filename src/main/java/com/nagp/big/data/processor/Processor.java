package com.nagp.big.data.processor;

import java.io.File;
import java.io.FileNotFoundException;
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
import org.apache.spark.sql.Column;
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
public class Processor implements ApplicationRunner {

    @Autowired
    private SparkSession spark;

    @Autowired
    private ConfigProperties properties;

    @Autowired
    private MessagePublisher messagePublisher;

    @Autowired
    private ExecutorService executorService;

    private static final String BIG_DATA_TOPIC = "nagp.bigdata";

    /**
     * reads the csv file and creates a temporay view from it and writes a parquet file of it
     * 
     * @throws FileNotFoundException
     */
    private void run() throws FileNotFoundException {
        Dataset<Invoice> invoicesCsv = encodeToBean(Invoice.class, getCsv());
        invoicesCsv.createOrReplaceTempView("Inv");
        writeToParquet(invoicesCsv, "invoices.parquet");
    }

    /**
     * gets invoices with invoiceAmount greater than average invoices amount for vendor
     * 
     * @param outputParquetName
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void getInvoicesWithInvAmntGtThanAvgInvAmntForVendor(String outputParquetName)
            throws InterruptedException, ExecutionException {
        Dataset<Row> invoicesRowData = spark.sql(
                "SELECT * FROM Inv I1 WHERE I1.InvoiceTotal > (SELECT AVG(I2.InvoiceTotal) FROM INV I2 WHERE I1.InvoiceVendorName = I2.InvoiceVendorName)");
        Dataset<Invoice> invoiceData = encodeToBean(Invoice.class, invoicesRowData);
        processInvoiceData(() -> writeToParquet(invoiceData, outputParquetName), () -> sendData(invoiceData),
                () -> saveDataInCassandra(invoiceData));
    }

    /**
     * gets the count of invoices with date difference by 1 Year between required columns
     * 
     * @param outputParquetName
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void getCountInvWithDateDiffBy1Y(String outputParquetName) throws InterruptedException, ExecutionException {
        Dataset<Row> invoiceParquet = readParquet("invoices.parquet");
        Dataset<Invoice> invoicesDateDiffGt1 = encodeToBean(Invoice.class, findInvoicesDateDiffGt1(invoiceParquet));
        processInvoiceData(() -> writeToParquet(invoicesDateDiffGt1, outputParquetName),
                () -> sendData(invoicesDateDiffGt1), () -> saveDataInCassandra(invoicesDateDiffGt1));
    }

    /**
     * Top 5 Vendors with highest turnover
     */
    private void gtop5Vendors() {
        Dataset<Invoice> testParquet = readParquet("invoices.parquet").as(Encoders.bean(Invoice.class));
        Map<String, Double> map = new HashMap<>();
        List<Row> take = testParquet.select("InvoiceTotal", "InvoiceVendorName", "InvoiceStatusDesc")
                .groupBy("InvoiceVendorName", "InvoiceStatusDesc").sum("InvoiceTotal").toJavaRDD().groupBy((row) -> {
                    return row.getAs("InvoiceVendorName").toString();
                }).map((row) -> {
                    row._2.forEach(r -> {
                        String vendor = r.getString(0);
                        Double value = r.getDouble(2);
                        String status = r.getString(1);
                        if (status.equals("Paid-in-Full")) {
                            map.put(vendor, map.getOrDefault(vendor, 0.0) + value);
                        } else if (status.equals("Rejected") || status.equals("Exception")) {
                            map.put(vendor, map.getOrDefault(vendor, 0.0) - value);
                        }
                        return;
                    });
                    return row._2.iterator().next();
                }).sortBy((r1) -> {
                    return r1.getDouble(2);
                }, false, 4).take(1000);
        take.forEach(e -> {
            System.out.println(e.toString());
        });
    }

    /**
     * executes various runnable methods with multithreading using executorService
     * 
     * @param runnables
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private void processInvoiceData(Runnable... runnables) throws InterruptedException, ExecutionException {
        List<Future<?>> futures = new LinkedList<>();
        for (Runnable runnable : runnables) {
            futures.add(executorService.submit(() -> runnable.run()));
        }
        for (Future<?> future : futures) {
            future.get();
        }
    }

    /**
     * encodes the dataset type to the beanClass provided
     * 
     * @param <T>
     * @param <R>
     * @param beanClass
     * @param dataSet
     * @return dataSet encoded to a particular object
     */
    private static <T, R> Dataset<R> encodeToBean(Class<R> beanClass, Dataset<T> dataSet) {
        return dataSet.as(Encoders.bean(beanClass));
    }

    /**
     * changes the formats of dates of type string given in csv to a particular format(MM-dd-yyyy)
     * 
     * @param invoices
     * @return Dataset for invoices with dates formatted to correct format to be read by spark
     */
    private Dataset<Row> findInvoicesDateDiffGt1(Dataset<Row> invoices) {
        return invoices.toDF()
                .filter(functions
                        .months_between(
                                getDateWithFormatIfExists("InvoiceRecvdDate", "MM/dd/yyyy")
                                        .otherwise(getDateWithFormatIfExists("InvoiceRecvdDate", "M/d/yyyy").otherwise(
                                                getDateWithFormatIfExists("InvoiceRecvdDate", "MM-dd-yyyy"))),
                                getDateWithFormatIfExists("InvoiceDate", "MM/dd/yyyy")
                                        .otherwise(getDateWithFormatIfExists("InvoiceDate", "M/d/yyyy")
                                                .otherwise(getDateWithFormatIfExists("InvoiceDate", "MM-dd-yyyy"))),
                                true)
                        .divide(12).gt(1.00));
    }

    /**
     * creates a conditional function which checks if the date in that column is of given format type. If of given type
     * then returns the value otherwise null
     * 
     * @param columnName
     * @param format
     * @return Column
     */
    private Column getDateWithFormatIfExists(String columnName, String format) {
        return functions.when(functions.to_date(functions.col(columnName), format).isNotNull(),
                functions.to_date(functions.col(columnName), format));
    }

    /**
     * saves the data in cassandra for given dataset
     * 
     * @param dataset
     */
    private void saveDataInCassandra(Dataset<Invoice> dataset) {
        Map<String, String> map = new HashMap<>();
        map.put("keyspace", "nagp");
        map.put("table", "invoices");
        RDD<Invoice> rdd = dataset.rdd();
        spark.sqlContext().createDataFrame(rdd, Invoice.class).write().format("org.apache.spark.sql.cassandra")
                .mode(SaveMode.Append).options(map).save();
    }

    /**
     * publishes the kafka messages for given dataset
     * 
     * @param dataSet
     */
    private void sendData(Dataset<Invoice> dataSet) {
        dataSet.collectAsList().forEach(invoice -> messagePublisher.sendMessage(BIG_DATA_TOPIC, invoice.toString(),
                UUID.randomUUID().toString()));
    }

    /**
     * reads the csv file for the path set in properties. some options are preset for easiness
     * 
     * @return Dataset for csv file read
     */
    private Dataset<Row> getCsv() {
        String filepath = properties.getFilePath() + File.separator + properties.getFilename();
        Dataset<Row> invoicesCsv = spark.read().format("csv").option("header", "true").load(filepath)
                .withColumn("InvoiceTotal", functions.col("InvoiceTotal").cast("Double"))
                .withColumn("PaidAmt", functions.col("PaidAmt").cast("Double"));
        return invoicesCsv;
    }

    /**
     * reads the parquet file for given name
     * 
     * @param parquetName
     * @return Dataset of parquet file read
     */
    private Dataset<Row> readParquet(String parquetName) {
        return spark.read().parquet(parquetName);
    }

    /**
     * Writes dataset to parquet file
     * 
     * @param invoicesData
     * @param filename
     */
    private void writeToParquet(Dataset<Invoice> invoicesData, String filename) {
        invoicesData.write().format("parquet").mode(SaveMode.Overwrite)
                .save(properties.getFilePath() + File.separator + filename);
    }

    /**
     * Method: run() is a dependency for other functions for first time application runs as it creates invoices.parquet
     * file which other methods reads. One can comment any of following methods and run only a single method at a time
     * or multiples
     */
    @Override
    public void run(ApplicationArguments args) throws Exception {
        // this method is required to create invoices.parquet which will be further used by next questions to read and
        // process data
        run();
        getInvoicesWithInvAmntGtThanAvgInvAmntForVendor("invoices_with_amnt_gt_invtotal.parquet");
        getCountInvWithDateDiffBy1Y("date_diff_gt_by_1.parquet");
        gtop5Vendors();
        System.exit(1);
    }

    @PreDestroy
    public void destroy() {
        if (Objects.nonNull(executorService)) {
            executorService.shutdown();
        }
    }
}
