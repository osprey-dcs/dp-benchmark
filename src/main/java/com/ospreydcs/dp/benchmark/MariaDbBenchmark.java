package com.ospreydcs.dp.benchmark;

import com.ospreydcs.dp.benchmark.BenchmarkCommon.ScenarioResult;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Callable;

import com.ospreydcs.dp.benchmark.BenchmarkCommon.*;
import static com.ospreydcs.dp.benchmark.BenchmarkCommon.createExecutorServiceAndInvokeTasks;

public class MariaDbBenchmark {

    private static Logger LOGGER = (Logger) LoggerFactory.getLogger(MariaDbBenchmark.class);

    private static final String DB_URL = "jdbc:mariadb://localhost/";
    private static final String DB_USER = "datastore";
    private static final String DB_PASSWORD = "datastore";
    private static final String DB_DATABASE_NAME = "benchmark";
    private static final String DB_TABLE_NAME_JSON = "pv_ts_data_json";
    private static final String DB_TABLE_NAME_POINTS = "pv_ts_data_points";

    protected final static String PV_NAME_BASE = "pv";
    protected final static String BSON_TS_DATA_KEY = "tsData";

    private static class RowContentJson {
        private String pvName = null;
        private Timestamp first = null;
        private Timestamp last = null;
        private String timeseriesDataJson = null;

        public String getPvName() {
            return pvName;
        }

        public void setPvName(String pvName) {
            this.pvName = pvName;
        }

        public Timestamp getFirst() {
            return first;
        }

        public void setFirst(Timestamp first) {
            this.first = first;
        }

        public Timestamp getLast() {
            return last;
        }

        public void setLast(Timestamp last) {
            this.last = last;
        }

        public String getTimeseriesDataJson() {
            return timeseriesDataJson;
        }

        public void setTimeseriesDataJson(String timeseriesDataJson) {
            this.timeseriesDataJson = timeseriesDataJson;
        }
    }

    private static class RowContentPoints {
        private String pvName = null;
        private Timestamp dataTime = null;
        private Double dataValue = null;

        public String getPvName() {
            return pvName;
        }

        public void setPvName(String pvName) {
            this.pvName = pvName;
        }

        public Timestamp getDataTime() {
            return dataTime;
        }

        public void setDataTime(Timestamp dataTime) {
            this.dataTime = dataTime;
        }

        public Double getDataValue() {
            return dataValue;
        }

        public void setDataValue(Double dataValue) {
            this.dataValue = dataValue;
        }
    }

    static class MariaInsertJsonTask extends MongoBenchmarkTask implements Callable<BenchmarkTaskResult> {
        private String tableName = null;
        private List<RowContentJson> batch = null;

        public MariaInsertJsonTask(String tableName, List<RowContentJson> batch) {
            this.tableName = tableName;
            this.batch = batch;
        }

        public static List<MongoBenchmarkTask> generateTaskList(
                String tableName, List<List<RowContentJson>> batches) {

            List<MongoBenchmarkTask> taskList = new ArrayList<>();
            for (var batch : batches) {
                taskList.add(new MariaInsertJsonTask(tableName, batch));
            }
            return taskList;
        }

        public BenchmarkTaskResult call() {
            Connection connection = null;
            BenchmarkTaskResult result = new BenchmarkTaskResult();
            try {
                connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                useDatabase(connection, DB_DATABASE_NAME);
                insertJsonBatch(connection, tableName, batch);
                result.setStatus(true);
                result.setRecordsAffected(batch.size());
                connection.close();
            } catch (SQLException e) {
                LOGGER.error("error connecting to: " + DB_URL + " user: " + DB_USER + " msg: " + e.getMessage());
                System.exit(1);
            }
            return result;
        }
    }

    static class MariaInsertPointsTask extends MongoBenchmarkTask implements Callable<BenchmarkTaskResult> {
        private String tableName = null;
        private List<RowContentPoints> batch = null;

        public MariaInsertPointsTask(String tableName, List<RowContentPoints> batch) {
            this.tableName = tableName;
            this.batch = batch;
        }

        public static List<MongoBenchmarkTask> generateTaskList(
                String tableName, List<List<RowContentPoints>> batches) {

            List<MongoBenchmarkTask> taskList = new ArrayList<>();
            for (var batch : batches) {
                taskList.add(new MariaInsertPointsTask(tableName, batch));
            }
            return taskList;
        }

        public BenchmarkTaskResult call() {
            Connection connection = null;
            BenchmarkTaskResult result = new BenchmarkTaskResult();
            try {
                connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
                useDatabase(connection, DB_DATABASE_NAME);
                insertPointsBatch(connection, tableName, batch);
                result.setStatus(true);
                result.setRecordsAffected(batch.size());
                connection.close();
            } catch (SQLException e) {
                LOGGER.error("error connecting to: " + DB_URL + " user: " + DB_USER + " msg: " + e.getMessage());
                System.exit(1);
            }
            return result;
        }
    }

    private static void executeSqlStatement(Connection connection, String sql, boolean exitOnError) {
        try {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql);
            LOGGER.debug("executed: '" + sql + "'");
        } catch (SQLException e) {
            LOGGER.error("error executing statement: '" + sql + "' msg: " + e.getMessage());
            if (exitOnError) System.exit(1);
        }
    }

    private static void createDatabase(Connection connection, String databaseName) {
        String sql = "CREATE DATABASE " + databaseName;
        executeSqlStatement(connection, sql, true);
    }

    private static void dropDatabase(Connection connection, String databaseName) {
        String sql = "DROP DATABASE " + databaseName;
        executeSqlStatement(connection, sql, false);
    }

    private static void useDatabase(Connection connection, String databaseName) {
        String sql = "USE " + databaseName;
        executeSqlStatement(connection, sql, true);
    }

    private static int getRowCount(Connection connection, String tableName) {
        final String query = "SELECT COUNT(*) from " + tableName;
        int result = -1;
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.first();
            result = resultSet.getInt(1);
        } catch (SQLException e) {
            LOGGER.error("error getting row count for table: " + tableName + " msg: " + e.getMessage());
            System.exit(1);
        }
        return result;
    }

    private static void createTableJson(Connection connection, String tableName) {
        String sql = "CREATE TABLE " + tableName +
                "(id INTEGER NOT NULL AUTO_INCREMENT, " +
                " pv_name VARCHAR(16) NOT NULL, " +
                " first TIMESTAMP(3) NOT NULL, " +
                " last TIMESTAMP(3) NOT NULL, " +
                " ts_data_json JSON NOT NULL, " +
                " PRIMARY KEY ( id ))";
        executeSqlStatement(connection, sql, true);
    }

    private static void createTableIndexJson(Connection connection, String tableName) {
        String pvIndexSql = "ALTER TABLE " + tableName + " ADD INDEX pv_name_index (pv_name);";
        executeSqlStatement(connection, pvIndexSql, true);
        String timeRangeIndexSql = "ALTER TABLE " + tableName + " ADD INDEX pv_time_range_index (pv_name, first, last);";
        executeSqlStatement(connection, timeRangeIndexSql, true);
    }

    protected static List<List<RowContentJson>> generateContentBatchListJson(
            int numPvs, int numSamplesPerBucket, int numBucketsPerPv, int batchSize) {

        // record start time
        Instant t0 = Instant.now();

        // get Date for current epoch seconds
        final Instant currentInstant = Instant.now();
        long currentEpochSecond = currentInstant.getEpochSecond();
        Instant currentSecondInstant = Instant.ofEpochSecond(currentEpochSecond);

        // create list of batches, each a list of row content
        List<List<RowContentJson>> batchList = new ArrayList<>();
        List<RowContentJson> currentBatch = new ArrayList<>();
        int currentBatchCount = 0;
        for (int id = 0 ; id < numBucketsPerPv ; id++) {
            final Instant firstInstant = currentSecondInstant;
            final Date firstDate = Date.from(firstInstant);
            final Instant lastInstant = firstInstant.plusMillis(numSamplesPerBucket-1);
            final Date lastDate = Date.from(lastInstant);
            for (int ip = 1; ip <= numPvs; ip++) {

                currentBatchCount = currentBatchCount + 1;
                String pvName = PV_NAME_BASE + ip;

                RowContentJson content = new RowContentJson();
                content.setPvName(pvName);
                content.setFirst(new Timestamp(firstDate.getTime()));
                content.setLast(new Timestamp(lastDate.getTime()));

                Document tsDataDocument = new Document();
                Double dataValue = 0.0;
                List<Double> dataValues = new ArrayList<>(numSamplesPerBucket);
                for (int is = 0; is < numSamplesPerBucket; is++) {
                    dataValues.add(dataValue/1000.0);
                    dataValue = dataValue + 1;
                }
                tsDataDocument.put(BSON_TS_DATA_KEY, dataValues);
                content.setTimeseriesDataJson(tsDataDocument.toJson());

                // add document to current batch and create next batch if current is full
                currentBatch.add(content);
                if (currentBatchCount == batchSize) {
                    batchList.add(currentBatch);
                    currentBatch = new ArrayList<>();
                    currentBatchCount = 0;
                }
            }

            // increment date to next second for next set of PV documents
            currentEpochSecond = currentEpochSecond + 1;
            currentSecondInstant = Instant.ofEpochSecond(currentEpochSecond);
        }

        // add final batch to batch list
        if (currentBatchCount > 0) {
            batchList.add(currentBatch);
        }

        // calculate and display stats
        Instant t1 = Instant.now();
        long dtMillisWrite = t0.until(t1, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        LOGGER.debug("seconds duration to create PV timeseries data batch list: " + dtSecondsWrite);

        return batchList;
    }

    private static void insertJsonBatch(Connection connection, String tableName, List<RowContentJson> batch) {
        PreparedStatement statement = null;
        try {
            String sql = "INSERT INTO " + tableName
                    + " (pv_name, first, last, ts_data_json)"
                    + " VALUES (?, ?, ?, ?)";
            statement = connection.prepareStatement(sql);
            for (RowContentJson content : batch) {
                statement.setString(1, content.getPvName());
                statement.setTimestamp(2, content.getFirst());
                statement.setTimestamp(3, content.getLast());
                statement.setString(4, content.getTimeseriesDataJson());
                statement.addBatch();
            }
            statement.executeBatch();
            statement.close();
        } catch (SQLException e) {
            LOGGER.error("error inserting batch to table: " + tableName + " msg: " + e.getMessage());
            System.exit(1);
        }
    }

    private static ScenarioResult scenarioCreatePvTimeseriesDataSqlJson(
            Connection connection,
            int numPvs,
            int numSamplesPerBucket,
            int numBucketsPerPv,
            int batchSize,
            int numThreads) {

        final String tableName = DB_TABLE_NAME_JSON + "_" + System.currentTimeMillis();
        createTableJson(connection, tableName);
        createTableIndexJson(connection, tableName);

        // generate list of PV time series data batches with hdf5 file content
        List<List<RowContentJson>> batchList =
                generateContentBatchListJson(numPvs, numSamplesPerBucket, numBucketsPerPv, batchSize);

        // set up tasks for thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = MariaInsertJsonTask.generateTaskList(tableName, batchList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // insert database rows for list of batches, multithreading controlled by numThreads
        long rowsInsertedCount = 0;
        if (numThreads == 0) {
            // simple scenario without threading
            for (var batch : batchList) {
                insertJsonBatch(connection, tableName, batch);
                rowsInsertedCount = rowsInsertedCount + batch.size();
            }

        } else {
            // use multithreading to accomplish scenario
            rowsInsertedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        // calculate stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = rowsInsertedCount / dtSecondsWrite;
        LOGGER.debug("files created: " + rowsInsertedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        // verify correct number of rows inserted
        int actualRowsInsertedCount = getRowCount(connection, tableName);
        if (actualRowsInsertedCount != rowsInsertedCount) {
            LOGGER.error("mismatch actual rows inserted: " + actualRowsInsertedCount + " expected: " + rowsInsertedCount);
            System.exit(1);
        }

        // convert rows inserted to data values written
        long dataValuesWrittenCount = rowsInsertedCount * numSamplesPerBucket;
        writeRate = dataValuesWrittenCount / dtSecondsWrite;
        LOGGER.debug(" dataValuesInsertedCount: " + dataValuesWrittenCount);
        LOGGER.debug("dtSecondsWrite: " + dtSecondsWrite + " writeRate: " + writeRate);

        ScenarioResult result = new ScenarioResult();
        result.setWriteRate(writeRate);
        return result;
    }

    private static void experimentCreatePvTimeseriesDataSqlJson(Connection connection) {

        System.out.println("============================");
        System.out.println("Starting Create PV Time Series Data Experiment (JSON)");
        System.out.println("============================");

        // set up scenario
        final int numPvs = 4_000;
        final int numSamplesPerBucket = 1_000; // one second's data for each pv
        final int numBucketsPerPv = 10; // e.g., number of seconds
        final int[] batchSizeArray = {/*100, 250, 500,*/ 1000, 2000, 5000, 10000};
        final int[] numThreadsArray = {/*0, 1,*/ 2/*, 3, 5, 7*/};

        LOGGER.info("numPvs: " + numPvs);
        LOGGER.info("numSamplesPerBucket: " + numSamplesPerBucket);
        LOGGER.info("numBucketsPerPv: " + numBucketsPerPv);

        // run experiment varying batchSize and numThreads
        Map<Integer, Map<Integer,Double>> writeRateMap = new TreeMap<>();
        for (int batchSize : batchSizeArray) {
            Map<Integer,Double> threadRateMap = new TreeMap<>();
            for (int numThreads : numThreadsArray) {
                LOGGER.info("running create PV time series (JSON) scenario batchSize: "
                        + batchSize + " numThreads: " + numThreads);
                ScenarioResult result =
                        scenarioCreatePvTimeseriesDataSqlJson(
                                connection, numPvs, numSamplesPerBucket, numBucketsPerPv, batchSize, numThreads);
                double writeRate = result.getWriteRate();
                threadRateMap.put(numThreads, writeRate);
            }
            writeRateMap.put(batchSize, threadRateMap);
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("============================");
        System.out.println("Create PV Time Series Experiment Results (JSON)");
        System.out.println("============================");
        for (var batchSizeEntry : writeRateMap.entrySet()) {
            int batchSize = batchSizeEntry.getKey();
            var threadRateMap = batchSizeEntry.getValue();
            for (var threadRateEntry : threadRateMap.entrySet()) {
                int numThreads = threadRateEntry.getKey();
                double writeRate = threadRateEntry.getValue();
                System.out.println("batchSize: " + batchSize + " numThreads: " + numThreads + " writeRate: " + writeRate + " writes/sec");
                if (writeRate > maxRate) {
                    maxRate = writeRate;
                }
                if (writeRate < minRate) {
                    minRate = writeRate;
                }
            }
        }
        System.out.println("max write rate: " + maxRate);
        System.out.println("min write rate: " + minRate);
    }

    private static void createTablePoints(Connection connection, String tableName) {
        String sql = "CREATE TABLE " + tableName +
                "(id INTEGER NOT NULL AUTO_INCREMENT, " +
                " pv_name VARCHAR(16) NOT NULL, " +
                " data_time TIMESTAMP(3) NOT NULL, " +
                " data_value DOUBLE NOT NULL, " +
                " PRIMARY KEY ( id ))";
        executeSqlStatement(connection, sql, true);
    }

    private static void createTableIndexPoints(Connection connection, String tableName) {
        String pvIndexSql = "ALTER TABLE " + tableName + " ADD INDEX pv_name_index (pv_name);";
        executeSqlStatement(connection, pvIndexSql, true);
        String timeIndexSql = "ALTER TABLE " + tableName + " ADD INDEX pv_time_index (pv_name, data_time);";
        executeSqlStatement(connection, timeIndexSql, true);
    }

    protected static List<List<RowContentPoints>> generateContentBatchListPoints(
            int numPvs, int numPointsPerPv, int batchSize) {

        // record start time
        Instant t0 = Instant.now();

        // capture start time for use in setting data timestamps
        final Instant currentInstant = Instant.now();
        final long currentMillis = currentInstant.toEpochMilli();

        // create list of batches, each a list of row content
        List<List<RowContentPoints>> batchList = new ArrayList<>();
        List<RowContentPoints> currentBatch = new ArrayList<>();
        int currentBatchCount = 0;

        for (int ip = 1; ip <= numPvs; ip++) {

            String pvName = PV_NAME_BASE + ip;
            Double dataValue = 0.0;
            long dataTimeMillis = currentMillis;
            for (int is = 0; is < numPointsPerPv; is++) {

                currentBatchCount = currentBatchCount + 1;
                final Date dataTimeDate = Date.from(Instant.ofEpochMilli(dataTimeMillis));

                RowContentPoints content = new RowContentPoints();
                content.setPvName(pvName);
                content.setDataTime(new Timestamp(dataTimeDate.getTime()));
                content.setDataValue(dataValue/1000.0);

                // add content to current batch and create next batch if current is full
                currentBatch.add(content);
                if (currentBatchCount == batchSize) {
                    batchList.add(currentBatch);
                    currentBatch = new ArrayList<>();
                    currentBatchCount = 0;
                }

                dataValue = dataValue + 1;
                dataTimeMillis = dataTimeMillis + 1;
            }
        }

        // add final batch to batch list
        if (currentBatchCount > 0) {
            batchList.add(currentBatch);
        }

        // calculate and display stats
        Instant t1 = Instant.now();
        long dtMillisWrite = t0.until(t1, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        LOGGER.debug("seconds duration to create PV timeseries data batch list: " + dtSecondsWrite);

        return batchList;
    }

    private static void insertPointsBatch(Connection connection, String tableName, List<RowContentPoints> batch) {
        PreparedStatement statement = null;
        try {
            String sql = "INSERT INTO " + tableName
                    + " (pv_name, data_time, data_value)"
                    + " VALUES (?, ?, ?)";
            statement = connection.prepareStatement(sql);
            for (RowContentPoints content : batch) {
                statement.setString(1, content.getPvName());
                statement.setTimestamp(2, content.getDataTime());
                statement.setDouble(3, content.getDataValue());
                statement.addBatch();
            }
            statement.executeBatch();
            statement.close();
        } catch (SQLException e) {
            LOGGER.error("error inserting batch to table: " + tableName + " msg: " + e.getMessage());
            System.exit(1);
        }
    }

    private static ScenarioResult scenarioCreatePvTimeseriesDataSqlPoints(
            Connection connection,
            int numPvs,
            int numPointsPerPv,
            int batchSize,
            int numThreads) {

        final String tableName = DB_TABLE_NAME_POINTS + "_" + System.currentTimeMillis();
        createTablePoints(connection, tableName);
        createTableIndexPoints(connection, tableName);

        // generate list of PV time series data batches with hdf5 file content
        List<List<RowContentPoints>> batchList =
                generateContentBatchListPoints(numPvs, numPointsPerPv, batchSize);

        // set up tasks for thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = MariaInsertPointsTask.generateTaskList(tableName, batchList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // insert database rows for list of batches, multithreading controlled by numThreads
        long rowsInsertedCount = 0;
        if (numThreads == 0) {
            // simple scenario without threading
            for (var batch : batchList) {
                insertPointsBatch(connection, tableName, batch);
                rowsInsertedCount = rowsInsertedCount + batch.size();
            }

        } else {
            // use multithreading to accomplish scenario
            rowsInsertedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        // calculate stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = rowsInsertedCount / dtSecondsWrite;
        LOGGER.debug("files created: " + rowsInsertedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        // verify correct number of rows inserted
        int actualRowsInsertedCount = getRowCount(connection, tableName);
        if (actualRowsInsertedCount != rowsInsertedCount) {
            LOGGER.error("mismatch actual rows inserted: " + actualRowsInsertedCount + " expected: " + rowsInsertedCount);
            System.exit(1);
        }

        ScenarioResult result = new ScenarioResult();
        result.setWriteRate(writeRate);
        return result;
    }

    private static void experimentCreatePvTimeseriesDataSqlPoints(Connection connection) {

        System.out.println("============================");
        System.out.println("Starting Create PV Time Series Data Experiment (Points)");
        System.out.println("============================");

        // set up scenario
        final int numPvs = 4_000;
        final int numPointsPerPv = 1_000; // one second's data for each pv
        final int[] batchSizeArray = {/*100, 250, 500, 1000, 2000, 5000,*/ 10000, 20000, 50000, 100000};
        final int[] numThreadsArray = {/*0, 1,*/ 2/*, 3, 5, 7*/};

        LOGGER.info("numPvs: " + numPvs);
        LOGGER.info("numPointsPerPv: " + numPointsPerPv);

        // run experiment varying batchSize and numThreads
        Map<Integer, Map<Integer,Double>> writeRateMap = new TreeMap<>();
        for (int batchSize : batchSizeArray) {
            Map<Integer,Double> threadRateMap = new TreeMap<>();
            for (int numThreads : numThreadsArray) {
                LOGGER.info("running create PV time series (Points) scenario batchSize: "
                        + batchSize + " numThreads: " + numThreads);
                ScenarioResult result =
                        scenarioCreatePvTimeseriesDataSqlPoints(
                                connection, numPvs, numPointsPerPv, batchSize, numThreads);
                double writeRate = result.getWriteRate();
                threadRateMap.put(numThreads, writeRate);
            }
            writeRateMap.put(batchSize, threadRateMap);
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("============================");
        System.out.println("Create PV Time Series Experiment Results (Points)");
        System.out.println("============================");
        for (var batchSizeEntry : writeRateMap.entrySet()) {
            int batchSize = batchSizeEntry.getKey();
            var threadRateMap = batchSizeEntry.getValue();
            for (var threadRateEntry : threadRateMap.entrySet()) {
                int numThreads = threadRateEntry.getKey();
                double writeRate = threadRateEntry.getValue();
                System.out.println("batchSize: " + batchSize + " numThreads: " + numThreads + " writeRate: " + writeRate + " writes/sec");
                if (writeRate > maxRate) {
                    maxRate = writeRate;
                }
                if (writeRate < minRate) {
                    minRate = writeRate;
                }
            }
        }
        System.out.println("max write rate: " + maxRate);
        System.out.println("min write rate: " + minRate);
    }

    public static void main(final String[] args) {

        Logger logger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc.client.impl.StandardClient");
        logger.setLevel(Level.ERROR); // turn off DEBUG logging for mongodb
        logger = (Logger) LoggerFactory.getLogger("org.mariadb.jdbc.message.server.OkPacket");
        logger.setLevel(Level.ERROR); // turn off DEBUG logging for mongodb

        LOGGER.setLevel(Level.INFO); // set level for this class to INFO

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
        } catch (SQLException e) {
            LOGGER.error("error connecting to: " + DB_URL + " user: " + DB_USER + " msg: " + e.getMessage());
            System.exit(1);
        }

        dropDatabase(connection, DB_DATABASE_NAME);
        createDatabase(connection, DB_DATABASE_NAME);
        useDatabase(connection, DB_DATABASE_NAME);

//        experimentCreatePvTimeseriesDataSqlJson(connection);
        experimentCreatePvTimeseriesDataSqlPoints(connection);

//        dropDatabase(connection, DB_DATABASE_NAME);
    }

}