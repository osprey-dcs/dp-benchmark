package com.ospreydcs.dp.benchmark;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import ch.qos.logback.classic.Logger;
import com.influxdb.client.*;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.write.Point;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.exceptions.InfluxException;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.slf4j.LoggerFactory;

public class InfluxDbBenchmark {

    private static Logger LOGGER = (Logger) LoggerFactory.getLogger(InfluxDbBenchmark.class);

    // token for docker influxdb install
    //private static char[] token = "kBhCr542FKn8mQcL9JhUtqqs8mTdSY7mEydJKhtUvvV938q8zScCxjYiJLKXN8aLlKQZ0Bg3kBBGkKznH2d5Fg==".toCharArray();

    // token for full influxdb install
    private static char[] token = "4dh4hmNt8etSi8qDWvUhPkuh8gZa5myS0geYvT3EKLXHWzmnoCxW9kAy3DqNFJ1G5dQiIzCTtWkcIYgEFrsWyA==".toCharArray();

    private static String org = "ospreydcs";

    private static String bucket = "datastore";

    public static List<List<Point>> pointBatchList = new ArrayList<>();

    public static List<List<String>> lineProtocolBatchList = new ArrayList<>();

    static PvPojo[] pojoArray = null;

    public enum ScenarioType {
        POINT,
        LINEPROTOCOL,
        POJO
    }

    @Measurement(name = "pvValue")
    private static class PvPojo {

        @Column(tag = true)
        String pvName;

        @Column
        Double value;

        @Column(timestamp = true)
        Instant time;
    }

    public static class InfluxWriteResult {
        private boolean status;
        private long measurementsWritten = 0;

        public boolean getStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }

        public long getMeasurementsWritten() {
            return measurementsWritten;
        }

        public void setMeasurementsWritten(long measurementsWritten) {
            this.measurementsWritten = measurementsWritten;
        }
    }

    public static List<String> initializePreTestData(int numRecords) {
        List<String> preTestRecords = new ArrayList<>();
        final long timeNowMillis = Instant.now().toEpochMilli();
        for (int i=1 ; i <= numRecords ; i++) {
            Point point = Point.measurement("bucketInit-" + i)
                    .addField("value", i)
                    .time(timeNowMillis, WritePrecision.MS);
            preTestRecords.add(point.toLineProtocol());
        }
        return preTestRecords;
    }

    public static void initializeTestData(ScenarioType scenarioType, int numPvs, int numSamplesPerPv, int batchSize) {

        Instant t0Data = Instant.now();

        final String baseMeasurementName = "pv";
        String[] pvNamesArray = new String[numPvs];
        final String fieldName = "value";
//        final double fieldValue = 42.0;

        final long timeNowMillis = Instant.now().toEpochMilli();
//        long[] milliTimesArray = new long[numSamplesPerPv];
        final long milliTimeIncrement = 1;
        long milliTimeNowPlusIncrement = timeNowMillis;
        LOGGER.info("first milli time: " + milliTimeNowPlusIncrement);

//        if (!usePojo) {
//            lineProtocolArray = new String[numPvs * numSamplesPerPv];
//        } else {
//            pojoArray = new PvPojo[numPvs * numSamplesPerPv];
//        }

        int measurementCount = 0;
        int batchCount = 0;
        List<Point> pointBatch = new ArrayList<>();
        List<String> lineProtocolBatch = new ArrayList<>();
        for (int it = 0 ; it < numSamplesPerPv ; it++) {

            for (int ip = 0; ip < numPvs; ip++) {

                measurementCount = measurementCount + 1;

                // create PV name in first loop of samples and add to name array
                if (it == 0) {
                    pvNamesArray[ip] = baseMeasurementName + (ip + 1);
                }

                if (scenarioType == ScenarioType.POINT || scenarioType == ScenarioType.LINEPROTOCOL) {

                    // create Point
                    final double fieldValue = measurementCount;

                    // create point with unique measurement name for every pv
                    // this seems to run about 75K writes/sec faster than using tag to distinguish PVs
                    Point point = Point.measurement(pvNamesArray[ip])
                            .addField(fieldName, fieldValue)
                            .time(milliTimeNowPlusIncrement, WritePrecision.MS);

//                    // create point with same measurement name for every pv and using tag to distinguish
//                    // this seems to run about 75K writes/sec slower than using measurement name to distinguish PVs
//                    Point point = Point.measurement(baseMeasurementName)
//                            .addTag("name", pvNamesArray[ip])
//                            .addField(fieldName, fieldValue)
//                            .time(milliTimeNowPlusIncrement, WritePrecision.MS);

                    batchCount = batchCount + 1;
                    if (scenarioType == ScenarioType.POINT) {
                        // add Point to batch
                        pointBatch.add(point);
                    } else {
                        lineProtocolBatch.add(point.toLineProtocol());
                    }

                    // if we've reached batch size, save current batch to list and create a new one
                    if (batchCount == batchSize) {
                        if (scenarioType == ScenarioType.POINT) {
                            pointBatchList.add(pointBatch);
                            pointBatch = new ArrayList<>();
                        } else {
                            lineProtocolBatchList.add(lineProtocolBatch);
                            lineProtocolBatch = new ArrayList<>();
                        }
                        batchCount = 0;
                    }

                } else if (scenarioType == ScenarioType.POJO){
//                    PvPojo pojo = new PvPojo();
//                    pojo.pvName = pvNamesArray[ip];
//                    pojo.value = fieldValue;
//                    pojo.time = Instant.ofEpochMilli(milliTimesArray[it]);
//                    pojoArray[measurementCount] = pojo;

                } else {
                    // unhandled scenario type, exit
                    LOGGER.error("Fatal error, unexpected scenario type: " + scenarioType);
                    System.exit(1);
                }
            }

            // increment time for next batch of measurements
            milliTimeNowPlusIncrement = milliTimeNowPlusIncrement + milliTimeIncrement;
        }

        LOGGER.info("last milli time: " + (milliTimeNowPlusIncrement - milliTimeIncrement));

        // add final batch list and display stats for scenario
        if (scenarioType == ScenarioType.POINT) {
            // add final batch to list if necessary
            if (pointBatch.size() > 0) {
                pointBatchList.add(pointBatch);
            }
            List<Point> firstBatch = pointBatchList.get(0);
            List<Point> lastBatch = pointBatchList.get(pointBatchList.size() - 1);
            LOGGER.info("number of Point batches: " + pointBatchList.size());
            LOGGER.info("first Point batch size: " + firstBatch.size() + " last batch size: " + lastBatch.size());
            LOGGER.info("first Point: " + firstBatch.get(0).toLineProtocol());
            LOGGER.info("last Point: " + lastBatch.get(lastBatch.size() - 1).toLineProtocol());

        } else if (scenarioType == ScenarioType.LINEPROTOCOL) {
            // add final batch to list if necessary
            if (lineProtocolBatch.size() > 0) {
                lineProtocolBatchList.add(lineProtocolBatch);
            }
            var firstBatch = lineProtocolBatchList.get(0);
            var lastBatch = lineProtocolBatchList.get(lineProtocolBatchList.size() - 1);
            LOGGER.info("number of line protocol batches: " + lineProtocolBatchList.size());
            LOGGER.info("first line protocol batch size: " + firstBatch.size() + " last batch size: " + lastBatch.size());
            LOGGER.info("first line protocol: " + firstBatch.get(0));
            LOGGER.info("last line protocol: " + lastBatch.get(lastBatch.size() - 1));

        } else if (scenarioType == ScenarioType.POJO) {

//            PvPojo firstPojo = pojoArray[0];
//            PvPojo lastPojo = pojoArray[measurementCount-1];
//            LOGGER.info("first pojo: " + firstPojo.pvName + " : " + firstPojo.value + " : " + firstPojo.time);
//            LOGGER.info("last pojo: " + lastPojo.pvName + " : " + lastPojo.value + " : " + lastPojo.time);
        }

        // calculate and display stats
        Instant t1Data = Instant.now();
        long dtMillisData = t0Data.until(t1Data, ChronoUnit.MILLIS);
        double dtSecondsData = dtMillisData / 1_000.0;
        LOGGER.info("measurement count: " + measurementCount);
        LOGGER.info("seconds to create data for test: " + dtSecondsData);

    }

    public static void benchmarkWriteDataPoints(InfluxDBClient influxDbClient, WriteApiBlocking writeApi, int numPvs, int numSamplesPerPv) {

        // set up test data, 50K batch found to be optimal thus far
        final int batchSize = 50000;
        initializeTestData(ScenarioType.POINT, numPvs, numSamplesPerPv, batchSize);

        // Write by Data Point
        Instant t0Write = Instant.now();
        int saveCount = 0;
        for (List<Point> batch : pointBatchList) {
            try {
                writeApi.writePoints(batch);
                saveCount = saveCount + batch.size();
            } catch (InfluxException ex) {
                LOGGER.error("exception in writeApi.writePoints(): " + ex.getMessage());
                System.exit(1);
            }
        }

        // calculate and display stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = saveCount / dtSecondsWrite;
        LOGGER.info("points saved to influxdb: " + saveCount);
        LOGGER.info("seconds to write data: " + dtSecondsWrite);
        LOGGER.info("rate writes/sec: " + writeRate);
    }

    public static void benchmarkWriteLineProtocol(InfluxDBClient influxDbClient, WriteApiBlocking writeApi, int numPvs, int numSamplesPerPv) {

        // set up test data, 250K batch found to be optimal thus far
        final int batchSize = 250000;
        initializeTestData(ScenarioType.LINEPROTOCOL, numPvs, numSamplesPerPv, batchSize);

        // Write by line protocol
        Instant t0Write = Instant.now();
        int saveCount = 0;
        for (List<String> batch : lineProtocolBatchList) {
            try {
                writeApi.writeRecords(WritePrecision.MS, batch);
                saveCount = saveCount + batch.size();
            } catch (InfluxException ex) {
                LOGGER.error("exception in writeApi.writeRecords(): " + ex.getMessage());
                System.exit(1);
            }
        }

        // calculate and display stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = saveCount / dtSecondsWrite;
        LOGGER.info("line protocol strings saved to influxdb: " + saveCount);
        LOGGER.info("seconds to write data: " + dtSecondsWrite);
        LOGGER.info("rate writes/sec: " + writeRate);
    }

    public static void benchmarkWritePojo(InfluxDBClient influxDbClient, WriteApiBlocking writeApi, int numPvs, int numSamplesPerPv) {

        LOGGER.error("POJO scenario needs to be changed to handle batching.");
        System.exit(1);

        // set up test data
        final int batchSize = 50000;
        initializeTestData(ScenarioType.POJO, numPvs, numSamplesPerPv, batchSize);

        // Write by POJO
        Instant t0Write = Instant.now();
        int saveCount = 0;
        for (PvPojo pojo : pojoArray) {
            try {
                writeApi.writeMeasurement(WritePrecision.NS, pojo);
                saveCount = saveCount + 1;
            } catch (InfluxException ex) {
                LOGGER.error("exception in writeApi.writeMeasurement(): " + ex.getMessage());
                System.exit(1);
            }
        }
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = saveCount / dtSecondsWrite;
        LOGGER.info("points saved to influxdb: " + saveCount);
        LOGGER.info("seconds to write data: " + dtSecondsWrite);
        LOGGER.info("rate writes/sec: " + writeRate);
    }

    static class InfluxWriteTask implements Callable<InfluxWriteResult> {

        private List<String> lineProtocolBatch = null;
        private InfluxDBClient influxClient = null;
        private WriteApiBlocking writeApiBlocking = null;
        private static WriteApi writeApi = null;
        private Boolean isSync = null;
        private Boolean isBatch = null;

        public static InfluxWriteTask syncBatchTask(List<String> batch, InfluxDBClient client) {
            InfluxWriteTask task = new InfluxWriteTask();
            task.writeApiBlocking = client.getWriteApiBlocking();
            task.isSync = true;
            task.isBatch = true;
            task.lineProtocolBatch = batch;
            return task;
        }

        private static void initWriteApi(InfluxDBClient client) {
            // create long-lived static async API shared by all async tasks
            writeApi = client.makeWriteApi(WriteOptions.builder()
                    .batchSize(250_000)
                    .bufferLimit(6_000_000)
                    .flushInterval(30000)
                    .build());
        }

        public static InfluxWriteTask asyncBatchTask(List<String> batch, InfluxDBClient client) {
            InfluxWriteTask task = new InfluxWriteTask();
            if (writeApi == null) {
                initWriteApi(client);
            }
            task.isSync = false;
            task.isBatch = true;
            task.lineProtocolBatch = batch;
            return task;
        }

        public static InfluxWriteTask asyncSingleTask(List<String> batch, InfluxDBClient client) {
            InfluxWriteTask task = new InfluxWriteTask();
            if (writeApi == null) {
                initWriteApi(client);
            }
            task.isSync = false;
            task.isBatch = false;
            task.lineProtocolBatch = batch;
            return task;
        }

        public static List<InfluxWriteTask> generateTaskList(boolean isSync, boolean isBatch, InfluxDBClient client) {
            List<InfluxWriteTask> taskList = new ArrayList<>();
            for (var batch : lineProtocolBatchList) {
                InfluxWriteTask task = null;
                if (isSync && isBatch) {
                    task = InfluxWriteTask.syncBatchTask(batch, client);
                } else if (!isSync && isBatch) {
                    task = InfluxWriteTask.asyncBatchTask(batch, client);
                } else if (!isSync && !isBatch) {
                    task = InfluxWriteTask.asyncSingleTask(batch, client);
                } else {
                    LOGGER.error("Unexpected case encountered generating task list isBatch false with isSync true");
                    System.exit(1);
                }
                taskList.add(task);
            }
            return taskList;
        }

        public InfluxWriteResult call() {
            try {
                if (isBatch && isSync) {
                    this.writeApiBlocking.writeRecords(WritePrecision.MS, lineProtocolBatch);
                } else if (isBatch && !isSync) {
                    this.writeApi.writeRecords(WritePrecision.MS, lineProtocolBatch);
                } else if (!isBatch && !isSync) {
                    for (String record : lineProtocolBatch) {
                        this.writeApi.writeRecord(WritePrecision.MS, record);
                    }
                } else {
                    LOGGER.error("unexpected case in InfluxWriteTask, sync API and single record");
                    System.exit(1);
                }
            } catch (InfluxException ex) {
                LOGGER.error("exception in writeApiBlocking.writeRecords(): " + ex.getMessage());
                System.exit(1);
            }
//            LOGGER.debug("writeApi.writeRecords() wrote batch of size: " + lineProtocolBatch.size());
            InfluxWriteResult result = new InfluxWriteResult();
            result.setMeasurementsWritten(lineProtocolBatch.size());
            result.setStatus(true);
            return result;
        }

        public static void asyncFlushAndClose() {
            if (writeApi != null) {
                writeApi.flush();
                writeApi.close();
            }
        }
    }

    public static void benchmarkMultithreadedWrite(InfluxDBClient influxDbClient, int numPvs, int numSamplesPerPv) {

        // set up threading and batch size

        // Settings for sync batches.
        // Found optimal batch 250K but doesn't seem to be a huge difference when using 7 threads.
        boolean isSync = true;
        boolean isBatch = true;
        final int batchSize = 50_000;
        final int numThreads = 7;

//        // Settings for async batches.
//        // Best performance was 1 thread and 50K batch size, 369K writes/sec
//        boolean isSync = false;
//        boolean isBatch = true;
//        final int batchSize = 50_000;
//        final int numThreads = 1;

//        // Settings for async single record writes.
//        // Best performance was 1 thread and 50K batch size, 369K writes/sec
//        boolean isSync = false;
//        boolean isBatch = false;
//        final int batchSize = 10_000;
//        final int numThreads = 1;

        LOGGER.info("using sync API: " + isSync);
        LOGGER.info("writing records in batches: " + isBatch);
        LOGGER.info("batch size: " + batchSize);
        LOGGER.info("number of threads: " + numThreads);

        initializeTestData(ScenarioType.LINEPROTOCOL, numPvs, numSamplesPerPv, batchSize);

        // set up queue of tasks
        // use appropriate task factory method for specified isSync and isBatch
        var executorService = Executors.newFixedThreadPool(numThreads);
        List<InfluxWriteTask> taskList = InfluxWriteTask.generateTaskList(isSync, isBatch, influxDbClient);

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // submit task list to executor service and handle task results
        List<Future<InfluxWriteResult>> resultList = null;
        boolean success = true;
        long measurementsWritten = 0;
        try {
            resultList = executorService.invokeAll(taskList);
            executorService.shutdown();
            if (executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                for (int i=0 ; i < resultList.size() ; i++) {
                    Future<InfluxWriteResult> future = resultList.get(i);
                    InfluxWriteResult taskResult = future.get();
                    if (!taskResult.getStatus()) {
                        success = false;
                    }
                    measurementsWritten = measurementsWritten + taskResult.getMeasurementsWritten();
                }
                if (!success) {
                    LOGGER.error("fatal error, InfluxBatchWriteTask failed");
                    System.exit(1);
                }
            }
        } catch (InterruptedException | ExecutionException ex) {
            executorService.shutdownNow();
            LOGGER.error("Data transmission interrupted by exception");
            Thread.currentThread().interrupt();
        }

        // for async API, need to force the api to fluxh and close so that all records are written before measuring performance
        if (!isSync) {
            LOGGER.info("flushing and closing async WriteApi");
            InfluxWriteTask.asyncFlushAndClose();
        }

        if (success) {
            // calculate and display stats
            Instant t1Write = Instant.now();
            long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
            double dtSecondsWrite = dtMillisWrite / 1_000.0;
            double writeRate = measurementsWritten / dtSecondsWrite;
            LOGGER.info("line protocol strings saved to influxdb: " + measurementsWritten);
            LOGGER.info("seconds to write data: " + dtSecondsWrite);
            LOGGER.info("rate writes/sec: " + writeRate);
        } else {
            LOGGER.error("multithreaded write scenario failed, performance data invalid");
        }
    }

    public static void main(final String[] args) {

        // set up test scenario
        final int numPvs = 4000;
        final int numSamplesPerPv = 1000;
        final int numMeasurements = numPvs * numSamplesPerPv;

        // create bucket for test
        // I noticed that a new bucket is about 100K writes/sec slower than an old one, not useful for benchmarking!
        boolean createBucket = true;
        final int numPreTestRecords = 1000;
        String bucketName = null;
        String bucketId = null;
        if (createBucket) {
            LOGGER.info("creating connection for token: " + token);
            InfluxDBClient bucketClient = InfluxDBClientFactory.create("http://localhost:8086", token);
            bucketName = "benchmark_" + System.currentTimeMillis();
            String orgId = "54c2c62884bb38a9";
            LOGGER.info("creating bucket for test: " + bucketName);
            Bucket bucket = bucketClient.getBucketsApi().createBucket(bucketName, orgId);
            bucketId = bucket.getId();
            bucketClient.close();

            // write some to initialize bucket overhead and hopefully allow test to run test at full speed
            InfluxDBClient bucketWriteClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucketName);
            WriteApiBlocking bucketWriteApi = bucketWriteClient.getWriteApiBlocking();
            List<String> initRecords = initializePreTestData(numPreTestRecords);
            LOGGER.info("number of pre test records written to initialize bucket: " + numPreTestRecords);
            try {
                bucketWriteApi.writeRecords(WritePrecision.MS, initRecords);
            } catch (InfluxException ex) {
                LOGGER.error("exception in writeApi.writeRecords() while initializing new bucket: " + ex.getMessage());
                System.exit(1);
            }
            bucketWriteClient.close();

            final int sleepMillis = 1000;
            LOGGER.info("sleeping: " + sleepMillis);
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        } else {
            bucketName = bucket;
        }

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", token, org, bucketName);

        LOGGER.info("isGzipEnabled: " + influxDBClient.isGzipEnabled());

//        benchmarkWriteDataPoints(influxDBClient, writeApi, numPvs, numSamplesPerPv);
//        benchmarkWriteLineProtocol(influxDBClient, writeApi, numPvs, numSamplesPerPv);
//        benchmarkWritePojo(influxDBClient, writeApi, numPvs, numSamplesPerPv);
        benchmarkMultithreadedWrite(influxDBClient, numPvs, numSamplesPerPv);

        // if we created new bucket, check bucket size for expected number of measurements
        // we avoid this check for an existing bucket that already contains data
        if (createBucket) {
            String flux = "from(bucket:\"" + bucketName + "\") |> range(start: 0) |> group() |> count()";
            QueryApi queryApi = influxDBClient.getQueryApi();
            List<FluxTable> tables = queryApi.query(flux);
            if (tables.size() != 1) {
                LOGGER.error("unexpected query result size: " + tables.size());
                System.exit(1);
            }
            FluxTable table = tables.get(0);
            List<FluxRecord> records = table.getRecords();
            if (records.size() != 1) {
                LOGGER.error("unexpected table records size: " + records.size());
                System.exit(1);
            }
            FluxRecord record = records.get(0);
            Long bucketMeasurements = (Long) record.getValueByKey("_value");
            LOGGER.info("measurements in bucket: " + bucketMeasurements);

            if (bucketMeasurements != (numMeasurements + numPreTestRecords)) {
                LOGGER.error("bucket: " + bucketName + " does not contain expected number of measurements: " + numMeasurements);
                System.exit(1);
            }
        }

        // close read / write client
        influxDBClient.close();

        // remove the temp bucket
        if (createBucket) {
            InfluxDBClient bucketClient = InfluxDBClientFactory.create("http://localhost:8086", token);
            LOGGER.info("removing test bucket: " + bucketName);
            bucketClient.getBucketsApi().deleteBucket(bucketId);
            bucketClient.close();
        }
    }
}
