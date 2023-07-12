package com.ospreydcs.dp.benchmark;

import ch.qos.logback.classic.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

import static com.mongodb.client.model.Updates.set;

public abstract class MongoDbBenchmarkCommon {
    
    // naming constants
    protected final static String PV_NAME_BASE = "pv";
    protected final static String ID_KEY = "_id";
    protected final static String FIRST_TIME_KEY = "first";
    protected final static String LAST_TIME_KEY = "last";
    protected final static String PV_GROUP_KEY = "pvGroup";
    protected final static String TIMESTAMP_SECONDS_KEY = "sec";
    protected final static String VALUES_KEY = "values";
    protected final static String PV_NAME_KEY = "pv";
    protected final static String METADATA_KEY = "metadata";
    protected final static String TIMESTAMP_KEY = "timestamp";
    protected final static String VALUE_KEY = "val";

    private static Logger LOGGER = (Logger) LoggerFactory.getLogger(MongoDbBenchmarkCommon.class);

    public static class BenchmarkUpdateResult {

        private Double writeRate = null;

        public Double getWriteRate() {
            return writeRate;
        }

        public void setWriteRate(Double writeRate) {
            this.writeRate = writeRate;
        }
    }

    protected static class BenchmarkCreateResult {
        private Double writeRate = null;
        private String collectionName = null;
        private List<String> idsInserted = null;

        public Double getWriteRate() {
            return writeRate;
        }

        public void setWriteRate(Double writeRate) {
            this.writeRate = writeRate;
        }

        public String getCollectionName() {
            return collectionName;
        }

        public void setCollectionName(String collectionName) {
            this.collectionName = collectionName;
        }

        public List<String> getIdsInserted() {
            return idsInserted;
        }

        public void setIdsInserted(List<String> idsInserted) {
            this.idsInserted = idsInserted;
        }
    }

    protected static List<List<Document>> generateMetadataCreateBatchList(int numPvs, int numPvGroups, int batchSize) {

        // record start time for performance benchmark
        Instant t0 = Instant.now();

        // simulate metadata for specified number of PVs with first time = now and last time one second later
        final Instant firstInstant = Instant.now();
        final Date firstDate = Date.from(firstInstant);
        final Date lastDate = firstDate;
        LOGGER.debug("first timestamp: " + firstDate);
        LOGGER.debug("last timestamp:" + lastDate);

        // create metadata document list
        List<List<Document>> batchList = new ArrayList<>();
        List<Document> currentBatch = new ArrayList<>();
        int currentBatchCount = 0;
        int pvGroupCount = 0;
        for (int i=1 ; i <= numPvs ; i++) {
            currentBatchCount = currentBatchCount + 1;
            pvGroupCount = pvGroupCount + 1;
            String pvName = PV_NAME_BASE + i;
            Document document = new Document();
            document.put(ID_KEY, pvName);
            document.put(FIRST_TIME_KEY, firstDate);
            document.put(LAST_TIME_KEY, lastDate);
            document.put(PV_GROUP_KEY, pvGroupCount);
            currentBatch.add(document);
            if (currentBatchCount == batchSize) {
                batchList.add(currentBatch);
                currentBatch = new ArrayList<>();
                currentBatchCount = 0;
            }
            if (pvGroupCount == numPvGroups) {
                pvGroupCount = 0;
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
        LOGGER.debug("seconds duration to create metadata document list: " + dtSecondsWrite);

        return batchList;
    }

    protected static List<List<Document>> generatePvTimeseriesDocumentBatchListBucket(
            int numPvs, int numSamplesPerDocument, int numDocumentsPerPv, int batchSize) {

        // record start time
        Instant t0 = Instant.now();

        // get Date for current epoch seconds
        final Instant currentInstant = Instant.now();
        long currentEpochSecond = currentInstant.getEpochSecond();
        Instant currentSecondInstant = Instant.ofEpochSecond(currentEpochSecond);
        Date currentSecondDate = Date.from(currentSecondInstant);
        long currentEpochMillis = currentInstant.toEpochMilli();

        // create list of batches, each a list of Bson documents
        List<List<Document>> batchList = new ArrayList<>();
        List<Document> currentBatch = new ArrayList<>();
        int currentBatchCount = 0;
        for (int id = 0 ; id < numDocumentsPerPv ; id++) {
            final Instant firstInstant = Instant.ofEpochMilli(currentEpochMillis);
            final Date firstDate = Date.from(firstInstant);
            long lastEpochMillis = currentEpochMillis;
            final Instant lastInstant = Instant.ofEpochMilli(lastEpochMillis);
            final Date lastDate = Date.from(lastInstant);
            for (int ip = 1; ip <= numPvs; ip++) {

                currentBatchCount = currentBatchCount + 1;
                String pvName = PV_NAME_BASE + ip;
                String docId = pvName + "-" + currentEpochSecond;
                Document document = new Document();
                document.put(ID_KEY, docId);
                document.put(PV_NAME_KEY, pvName);
                document.put(TIMESTAMP_SECONDS_KEY, currentSecondDate);
                document.put(FIRST_TIME_KEY, firstDate);

                // create Map of timeseries data
                // if numSamplesPerDocument==1000, key is millisecond, value is double data point
                Map<String, Double> timeseriesDataMap = new TreeMap<>();
                Double dataValue = 0.0;
                for (int is = 0; is < numSamplesPerDocument; is++) {
                    timeseriesDataMap.put(String.valueOf(is), dataValue/1000.0);
                    dataValue = dataValue + 1;
                    lastEpochMillis = lastEpochMillis + is;
                }
                document.put(VALUES_KEY, timeseriesDataMap);
                document.put(LAST_TIME_KEY, lastDate);

                // add document to current batch and create next batch if current is full
                currentBatch.add(document);
                if (currentBatchCount == batchSize) {
                    batchList.add(currentBatch);
                    currentBatch = new ArrayList<>();
                    currentBatchCount = 0;
                }
            }

            // increment date to next second for next set of PV documents
            currentEpochSecond = currentEpochSecond + 1;
            currentSecondInstant = Instant.ofEpochSecond(currentEpochSecond);
            currentSecondDate = Date.from(currentSecondInstant);
            currentEpochMillis = currentInstant.toEpochMilli();
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

    protected static List<List<Document>> generatePvTimeseriesDocumentBatchListTsCollection(
            int numPvs, int numPvSamplesPerSecond, int numSeconds, int batchSize) {

        // record start time
        Instant t0 = Instant.now();

        // get Date for current epoch millis
        final Instant startInstant = Instant.now();

        // create batches of documents containing PV time series data
        List<List<Document>> batchList = new ArrayList<>();
        List<Document> currentBatch = new ArrayList<>();
        int currentBatchCount = 0;

        for (int is = 0 ; is < numSeconds ; is++) {

            for (int ip = 1 ; ip <= numPvs ; ip++) {
                long currentEpochMillis = startInstant.toEpochMilli();

                for (int ips = 1; ips <= numPvSamplesPerSecond; ips++) {
                    Double dataValue = ips / 1000.0;
                    Instant currentInstant = Instant.ofEpochMilli(currentEpochMillis);
                    Date currentMilliDate = Date.from(currentInstant);

                    currentBatchCount = currentBatchCount + 1;
                    Document document = new Document();
                    Map<String, String> metadataMap = new TreeMap<>();
                    String pvName = PV_NAME_BASE + ip;
                    metadataMap.put(PV_NAME_KEY, pvName);
                    document.put(METADATA_KEY, metadataMap);
                    document.put(TIMESTAMP_KEY, currentMilliDate);
                    dataValue = dataValue + ip;
                    document.put(VALUE_KEY, dataValue);

                    // add document to current batch and create next batch if current is full
                    currentBatch.add(document);
                    if (currentBatchCount == batchSize) {
                        batchList.add(currentBatch);
                        currentBatch = new ArrayList<>();
                        currentBatchCount = 0;
                    }

                    // increment date to next second for next set of PV documents
                    currentEpochMillis = currentEpochMillis + 1;
                }
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
        LOGGER.debug("seconds duration to create PV timeseries data document list: " + dtSecondsWrite);

        return batchList;
    }

    protected static List<Bson> generateUpdateOperationList(int numUpdatesPerPv) {
        final Instant currentInstant = Instant.now();
        final long updateMillis = currentInstant.toEpochMilli();
        List<Bson> updateOperationList = new ArrayList<>();
        for (int i = 0 ; i < numUpdatesPerPv ; i++) {
            final Instant updateInstant = Instant.ofEpochMilli(updateMillis);
            final Date updateDate = Date.from(updateInstant);
            final Bson updateOperation = set(LAST_TIME_KEY, updateDate);
            updateOperationList.add(updateOperation);
        }
        return updateOperationList;
    }

}
