package com.ospreydcs.dp.benchmark;

// for logging, since we are set up for spring, we are using a slf4j facade with a logback implementation, thus the following imports
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5SimpleWriter;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Callable;

import com.ospreydcs.dp.benchmark.BenchmarkCommon.*;
import static com.ospreydcs.dp.benchmark.BenchmarkCommon.createExecutorServiceAndInvokeTasks;

// extends MongoDbBenchmarkCommon to share some of its features, e.g, keys for field names in documents
public class FileBenchmark extends MongoDbBenchmarkCommon {

    private static Logger LOGGER = (Logger) LoggerFactory.getLogger(FileBenchmark.class);

    private static final String BASE_OUTPUT_DIRECTORY = "/home/craigmcc/datastore-benchmark";

    private static class Hdf5FileContent {
        private String pvName = null;
        private Date first = null;
        private Date last = null;
        private double[] timeseriesDataArray = null;

        public String getPvName() {
            return pvName;
        }

        public void setPvName(String pvName) {
            this.pvName = pvName;
        }

        public Date getFirst() {
            return first;
        }

        public void setFirst(Date first) {
            this.first = first;
        }

        public Date getLast() {
            return last;
        }

        public void setLast(Date last) {
            this.last = last;
        }

        public double[] getTimeseriesDataArray() {
            return timeseriesDataArray;
        }

        public void setTimeseriesDataArray(double[] timeseriesDataArray) {
            this.timeseriesDataArray = timeseriesDataArray;
        }
    }

    private static void writeJsonFileBatch(Path path, List<String> batch) {
        int fileId = 1;
        for (String jsonFileContent : batch) {
            final String filePathString = path.toString() + "/" + fileId + ".json";
            Path filePath = Paths.get(filePathString);
            try {
                Files.writeString(filePath, jsonFileContent);
            } catch (IOException e) {
                LOGGER.error("error writing file to path: " + path.toString() + " msg: " + e.getMessage());
                System.exit(1);
            }
            fileId = fileId + 1;
        }
    }

    private static void writeHdf5FileBatch(Path path, List<Hdf5FileContent> batch) {
        int fileId = 1;
        for (Hdf5FileContent fileContent : batch) {
            final String filePathString = path.toString() + "/" + fileId + ".h5";
            IHDF5SimpleWriter writer = HDF5Factory.open(filePathString);
            writer.writeString(PV_NAME_KEY, fileContent.getPvName());
            writer.writeDate(FIRST_TIME_KEY, fileContent.getFirst());
            writer.writeDate(LAST_TIME_KEY, fileContent.getLast());
            writer.writeDoubleArray(VALUES_KEY, fileContent.getTimeseriesDataArray());
            writer.close();
            fileId = fileId + 1;
        }
    }

    static class JsonCreateTask extends MongoBenchmarkTask implements Callable<BenchmarkTaskResult> {
        private Path path = null;
        private List<String> batch = null;

        public JsonCreateTask(Path path, List<String> batch) {
            this.path = path;
            this.batch = batch;
        }

        public static List<MongoBenchmarkTask> generateTaskList(
                Map<Integer,Path> batchPathMap, List<List<String>> batches) {

            List<MongoBenchmarkTask> taskList = new ArrayList<>();
            int batchId = 1;
            for (var batch : batches) {
                Path batchPath = batchPathMap.get(batchId);
                taskList.add(new JsonCreateTask(batchPath, batch));
                batchId = batchId + 1;
            }
            return taskList;
        }

        public BenchmarkTaskResult call() {
            BenchmarkTaskResult result = new BenchmarkTaskResult();
            writeJsonFileBatch(path, batch);
            result.setStatus(true);
            result.setRecordsAffected(batch.size());
            return result;
        }
    }

    static class Hdf5CreateTask extends MongoBenchmarkTask implements Callable<BenchmarkTaskResult> {
        private Path path = null;
        private List<Hdf5FileContent> batch = null;

        public Hdf5CreateTask(Path path, List<Hdf5FileContent> batch) {
            this.path = path;
            this.batch = batch;
        }

        public static List<MongoBenchmarkTask> generateTaskList(
                Map<Integer,Path> batchPathMap, List<List<Hdf5FileContent>> batches) {

            List<MongoBenchmarkTask> taskList = new ArrayList<>();
            int batchId = 1;
            for (var batch : batches) {
                Path batchPath = batchPathMap.get(batchId);
                taskList.add(new Hdf5CreateTask(batchPath, batch));
                batchId = batchId + 1;
            }
            return taskList;
        }

        public BenchmarkTaskResult call() {
            BenchmarkTaskResult result = new BenchmarkTaskResult();
            writeHdf5FileBatch(path, batch);
            result.setStatus(true);
            result.setRecordsAffected(batch.size());
            return result;
        }
    }

    private static String getScenarioOutputPath() {
        // append a subdirectory named with current timestamp to the base output directory
        Instant timestampInstant = Instant.now();
        long timestampMillis = timestampInstant.toEpochMilli();
        return BASE_OUTPUT_DIRECTORY + "/" + timestampMillis;
    }

    private static Map<Integer,Path> getBatchPathMap(int numBatches, String scenarioOutputPath) {
        Map<Integer,Path> batchPathMap = new TreeMap<>();
        for (int i = 1 ; i <= numBatches ; i++) {
            final int batchId = i;
            final String batchPathString = scenarioOutputPath + "/" + batchId;
            Path batchPath = Paths.get(batchPathString);
            try {
                Files.createDirectories(batchPath);
            } catch (IOException e) {
                LOGGER.error("exception creating scenario output directory: "
                        + batchPathString + " msg: " + e.getMessage());
                System.exit(1);
            }
            batchPathMap.put(batchId, batchPath);
        }
        return batchPathMap;
    }

    private static int getOsFileCount(String scenarioOutputPath, String filePattern) {
        String[] params = {
                "/bin/sh",
                "-c",
                "find " + scenarioOutputPath + " -name " + filePattern + " -print | wc -l"
        };
        int fileCount = -1;
        try {
            Process process = Runtime.getRuntime().exec(params);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            fileCount = Integer.valueOf(reader.readLine().trim());
            reader.close();
        } catch (IOException e) {
            LOGGER.error("error calling find/wc for scenario directory: " + scenarioOutputPath);
            System.exit(1);
        }
        return fileCount;
    }

    private static void removeOsDirectory(String osDirectory) {
        String[] params = {
                "/bin/sh",
                "-c",
                "rm -rf " + osDirectory
        };
        try {
            Process process = Runtime.getRuntime().exec(params);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String commandOutput = reader.readLine();
            reader.close();
            if (commandOutput != null) {
                LOGGER.error("unexpected output removing directory: "
                        + osDirectory + " msg: " + commandOutput);
            }
        } catch (IOException e) {
            LOGGER.error("error calling rm for scenario directory: " + osDirectory);
            System.exit(1);
        }
    }

    protected static List<List<String>> generatePvTimeseriesDocumentBatchListJson(
            int numPvs, int numSamplesPerDocument, int numDocumentsPerPv, int batchSize) {

        // record start time
        Instant t0 = Instant.now();

        // get Date for current epoch seconds
        final Instant currentInstant = Instant.now();
        long currentEpochSecond = currentInstant.getEpochSecond();
        Instant currentSecondInstant = Instant.ofEpochSecond(currentEpochSecond);

        // create list of batches, each a list of json string file content
        List<List<String>> batchList = new ArrayList<>();
        List<String> currentBatch = new ArrayList<>(batchSize);
        int currentBatchCount = 0;
        for (int id = 0 ; id < numDocumentsPerPv ; id++) {
            final Instant firstInstant = currentSecondInstant;
            final Date firstDate = Date.from(firstInstant);
            final Instant lastInstant = firstInstant.plusMillis(numSamplesPerDocument-1);
            final Date lastDate = Date.from(lastInstant);
            for (int ip = 1; ip <= numPvs; ip++) {

                currentBatchCount = currentBatchCount + 1;
                String pvName = PV_NAME_BASE + ip;
                String docId = pvName + "-" + currentEpochSecond;
                Document document = new Document();
                document.put(ID_KEY, docId);
                document.put(PV_NAME_KEY, pvName);
                document.put(FIRST_TIME_KEY, firstDate);
                document.put(LAST_TIME_KEY, lastDate);

                // create Map of timeseries data
                // if numSamplesPerDocument==1000, key is millisecond, value is double data point
                Map<String, Double> timeseriesDataMap = new HashMap<>(numSamplesPerDocument);
                Double dataValue = 0.0;
                for (int is = 0; is < numSamplesPerDocument; is++) {
                    timeseriesDataMap.put(String.valueOf(is), dataValue/1000.0);
                    dataValue = dataValue + 1;
                }
                document.put(VALUES_KEY, timeseriesDataMap);

                // add document to current batch and create next batch if current is full
                currentBatch.add(document.toJson());
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
        LOGGER.debug("seconds duration to create PV timeseries data document list: " + dtSecondsWrite);

        return batchList;
    }

    private static ScenarioResult scenarioCreatePvTimeseriesDataJson(
            int numPvs, int numSamplesPerDocument, int numDocumentsPerPv,
            int batchSize, int numThreads) {

        // generate PV time series data document list for scenario
        List<List<String>> documentList =
                generatePvTimeseriesDocumentBatchListJson(
                        numPvs, numSamplesPerDocument, numDocumentsPerPv, batchSize);

        String scenarioOutputPath = getScenarioOutputPath();
        int numBatches = documentList.size();
        Map<Integer,Path> batchPathMap = getBatchPathMap(numBatches, scenarioOutputPath);

        // set up tasks for thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = JsonCreateTask.generateTaskList(batchPathMap, documentList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // create json files for list of batches, multithreading controlled by numThreads
        long filesCreatedCount = 0;
        if (numThreads == 0) {
            // simple scenario without threading
            int batchId = 1;
            for (var batch : documentList) {
                Path batchPath = batchPathMap.get(batchId);
                writeJsonFileBatch(batchPath, batch);
                filesCreatedCount = filesCreatedCount + batch.size();
                batchId = batchId + 1;
            }

        } else {
            // use multithreading to accomplish scenario
            filesCreatedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        // calculate and display stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = filesCreatedCount / dtSecondsWrite;
        LOGGER.debug("files created: " + filesCreatedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        // verify correct number of files actually created
        int osFilesCount = getOsFileCount(scenarioOutputPath, "\"*.json\"");
        if (osFilesCount != (numPvs * numDocumentsPerPv)) {
            LOGGER.error("number of files generated by scenario: "
                    + osFilesCount + " differs from expected: " + (numPvs * numDocumentsPerPv));
            System.exit(1);
        }
        LOGGER.info("os files created by scenario: " + osFilesCount);

        // convert files created to data values written
        long dataValuesWrittenCount = filesCreatedCount * numSamplesPerDocument;
        writeRate = dataValuesWrittenCount / dtSecondsWrite;
        LOGGER.debug(" dataValuesInsertedCount: " + dataValuesWrittenCount);
        LOGGER.debug("dtSecondsWrite: " + dtSecondsWrite + " writeRate: " + writeRate);

        removeOsDirectory(scenarioOutputPath);

        ScenarioResult result = new ScenarioResult();
        result.setWriteRate(writeRate);
        return result;
    }

    private static void experimentCreatePvTimeseriesDataJson() {

        System.out.println("============================");
        System.out.println("Starting Create PV Time Series Data Experiment (JSON)");
        System.out.println("============================");

        // set up scenario
        final int numPvs = 4_000;
        final int numSamplesPerDocument = 60_000; // one minute's data for each pv
        final int numDocumentsPerPv = 1; // e.g., number of seconds
        final int[] batchSizeArray = {100/*, 250, 500, 750, 1000, 2000*/};
        final int[] numThreadsArray = {/*0, 1,*/ 2/*, 3, 5, 7*/};

        LOGGER.info("numPvs: " + numPvs);
        LOGGER.info("numSamplesPerPv: " + numSamplesPerDocument);
        LOGGER.info("numDocumentsPerPv: " + numDocumentsPerPv);

        // run experiment varying batchSize and numThreads
        Map<Integer, Map<Integer,Double>> writeRateMap = new TreeMap<>();
        for (int batchSize : batchSizeArray) {
            Map<Integer,Double> threadRateMap = new TreeMap<>();
            for (int numThreads : numThreadsArray) {
                LOGGER.info("running JSON create PV time series data benchmark scenario batchSize: "
                        + batchSize + " numThreads: " + numThreads);
                ScenarioResult result =
                        scenarioCreatePvTimeseriesDataJson(
                                numPvs, numSamplesPerDocument, numDocumentsPerPv, batchSize, numThreads);
                double writeRate = result.getWriteRate();
                threadRateMap.put(numThreads, writeRate);
            }
            writeRateMap.put(batchSize, threadRateMap);
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("============================");
        System.out.println("Create PV Time Series Experiment Results");
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

    protected static List<List<Hdf5FileContent>> generateContentBatchListHdf5(
            int numPvs, int numSamplesPerFile, int numFilesPerPv, int batchSize) {

        // record start time
        Instant t0 = Instant.now();

        // get Date for current epoch seconds
        final Instant currentInstant = Instant.now();
        long currentEpochSecond = currentInstant.getEpochSecond();
        Instant currentSecondInstant = Instant.ofEpochSecond(currentEpochSecond);

        // create list of batches, each a list of json string file content
        List<List<Hdf5FileContent>> batchList = new ArrayList<>();
        List<Hdf5FileContent> currentBatch = new ArrayList<>();
        int currentBatchCount = 0;
        for (int id = 0 ; id < numFilesPerPv ; id++) {
            final Instant firstInstant = currentSecondInstant;
            final Date firstDate = Date.from(firstInstant);
            final Instant lastInstant = firstInstant.plusMillis(numSamplesPerFile-1);
            final Date lastDate = Date.from(lastInstant);
            for (int ip = 1; ip <= numPvs; ip++) {

                currentBatchCount = currentBatchCount + 1;
                String pvName = PV_NAME_BASE + ip;

                Hdf5FileContent content = new Hdf5FileContent();
                content.setPvName(pvName);
                content.setFirst(firstDate);
                content.setLast(lastDate);

                // create Map of timeseries data
                // if numSamplesPerDocument==1000, key is millisecond, value is double data point
                Double dataValue = 0.0;
                double[] dataValues = new double[numSamplesPerFile];
                for (int is = 0; is < numSamplesPerFile; is++) {
                    dataValues[is] = dataValue/1000.0;
                    dataValue = dataValue + 1;
                }
                content.setTimeseriesDataArray(dataValues);

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
        LOGGER.debug("seconds duration to create PV timeseries data document list: " + dtSecondsWrite);

        return batchList;
    }

    private static ScenarioResult scenarioCreatePvTimeseriesDataHdf5(
            int numPvs,
            int numSamplesPerDocument,
            int numDocumentsPerPv,
            int batchSize,
            int numThreads) {

        // generate list of PV time series data batches with hdf5 file content
        List<List<Hdf5FileContent>> batchList =
                generateContentBatchListHdf5(numPvs, numSamplesPerDocument, numDocumentsPerPv, batchSize);

        String scenarioOutputPath = getScenarioOutputPath();
        int numBatches = batchList.size();
        Map<Integer,Path> batchPathMap = getBatchPathMap(numBatches, scenarioOutputPath);

        // set up tasks for thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = Hdf5CreateTask.generateTaskList(batchPathMap, batchList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // create hdf5 files for list of batches, multithreading controlled by numThreads
        long filesCreatedCount = 0;
        if (numThreads == 0) {
            // simple scenario without threading
            int batchId = 1;
            for (var batch : batchList) {
                Path batchPath = batchPathMap.get(batchId);
                writeHdf5FileBatch(batchPath, batch);
                filesCreatedCount = filesCreatedCount + batch.size();
                batchId = batchId + 1;
            }

        } else {
            // use multithreading to accomplish scenario
            filesCreatedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        // calculate stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = filesCreatedCount / dtSecondsWrite;
        LOGGER.debug("files created: " + filesCreatedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        // verify correct number of files actually created
        int osFilesCount = getOsFileCount(scenarioOutputPath, "\"*.h5\"");
        if (osFilesCount != (numPvs * numDocumentsPerPv)) {
            LOGGER.error("number of files generated by scenario: "
                    + osFilesCount + " differs from expected: " + (numPvs * numDocumentsPerPv));
            System.exit(1);
        }
        LOGGER.info("os files created by scenario: " + osFilesCount);

        // convert files created to data values written
        long dataValuesWrittenCount = filesCreatedCount * numSamplesPerDocument;
        writeRate = dataValuesWrittenCount / dtSecondsWrite;
        LOGGER.debug(" dataValuesInsertedCount: " + dataValuesWrittenCount);
        LOGGER.debug("dtSecondsWrite: " + dtSecondsWrite + " writeRate: " + writeRate);

        removeOsDirectory(scenarioOutputPath);

        ScenarioResult result = new ScenarioResult();
        result.setWriteRate(writeRate);
        return result;
    }

    private static void experimentCreatePvTimeseriesDataHdf5() {

        System.out.println("============================");
        System.out.println("Starting Create PV Time Series Data Experiment (HDF5)");
        System.out.println("============================");

        // set up scenario
        final int numPvs = 4_000;
        final int numSamplesPerDocument = 60_000; // one minutes's data for each pv
        final int numDocumentsPerPv = 1; // e.g., number of seconds
        final int[] batchSizeArray = {100, 250, 500/*, 1000, 2000, 750, 1000, 2000*/};
        final int[] numThreadsArray = {0, 1, /*2, 3, 5, 7*/};

        LOGGER.info("numPvs: " + numPvs);
        LOGGER.info("numSamplesPerPv: " + numSamplesPerDocument);
        LOGGER.info("numDocumentsPerPv: " + numDocumentsPerPv);

        // run experiment varying batchSize and numThreads
        Map<Integer, Map<Integer,Double>> writeRateMap = new TreeMap<>();
        for (int batchSize : batchSizeArray) {
            Map<Integer,Double> threadRateMap = new TreeMap<>();
            for (int numThreads : numThreadsArray) {
                LOGGER.info("running HDF5 create PV time series data benchmark scenario batchSize: "
                        + batchSize + " numThreads: " + numThreads);
                ScenarioResult result =
                        scenarioCreatePvTimeseriesDataHdf5(
                                numPvs, numSamplesPerDocument, numDocumentsPerPv, batchSize, numThreads);
                double writeRate = result.getWriteRate();
                threadRateMap.put(numThreads, writeRate);
            }
            writeRateMap.put(batchSize, threadRateMap);
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 100_000_000;
        System.out.println("============================");
        System.out.println("Create PV Time Series Experiment Results");
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

        // set log level for this class
        LOGGER.setLevel(Level.DEBUG); // set level for this class to INFO

//        experimentCreatePvTimeseriesDataJson();
        experimentCreatePvTimeseriesDataHdf5();
    }

}
