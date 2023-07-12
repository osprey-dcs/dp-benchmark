package com.ospreydcs.dp.benchmark;

// for logging, since we are set up for spring, we are using a slf4j facade with a logback implementation, thus the following imports
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Indexes;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertManyResult;
import static com.mongodb.client.model.Filters.eq;

import com.mongodb.client.model.TimeSeriesOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.ospreydcs.dp.benchmark.BenchmarkCommon.*;
import static com.ospreydcs.dp.benchmark.BenchmarkCommon.createExecutorServiceAndInvokeTasks;

public class MongoDbSyncBenchmark extends MongoDbBenchmarkCommon {

    private static Logger LOGGER = (Logger) LoggerFactory.getLogger(MongoDbSyncBenchmark.class);

    static class MongoInsertTask extends MongoBenchmarkTask implements Callable<BenchmarkTaskResult> {
        private MongoCollection<Document> collection = null;
        private List<Document> documentBatch = null;

        public MongoInsertTask(MongoCollection<Document> collection, List<Document> batch) {
            this.collection = collection;
            this.documentBatch = batch;
        }

        public static List<MongoBenchmarkTask> generateTaskList(
                MongoCollection<Document> collection, List<List<Document>> batches) {

            List<MongoBenchmarkTask> taskList = new ArrayList<>();
            for (var batch : batches) {
                taskList.add(new MongoInsertTask(collection, batch));
            }
            return taskList;
        }

        public BenchmarkTaskResult call() {
            BenchmarkTaskResult result = new BenchmarkTaskResult();
            InsertManyResult insertManyResult = collection.insertMany(documentBatch);
            if (!insertManyResult.wasAcknowledged()) {
                LOGGER.error("mongodb error in collection.insertMany(), write not acknowledged");
                System.exit(1);
            }
            result.setStatus(true);
            result.setRecordsAffected(insertManyResult.getInsertedIds().size());
            List<String> idsInserted = new ArrayList<>();
            for (var entry : insertManyResult.getInsertedIds().entrySet()) {
                idsInserted.add(entry.getValue().toString());
            }
            result.setIdsInserted(idsInserted);
            return result;
        }
    }

    private static BenchmarkCreateResult benchmarkCreatePvMetadata(MongoDatabase database, int numPvs, int numPvGroups, int batchSize, int numThreads, boolean removeCollection) {

        // generate metadata document list for specified number of pvs
        if (numPvGroups < 1) numPvGroups = 1;
        List<List<Document>> pvMetadataBatchList = generateMetadataCreateBatchList(numPvs, numPvGroups, batchSize);

        // create collection
        final String pvMetadataCollectionName = "pvMetadata-" + numPvs + "-" + batchSize + "-" + System.currentTimeMillis();
        LOGGER.debug("creating collection: " + pvMetadataCollectionName);
        database.createCollection(pvMetadataCollectionName);
        MongoCollection<Document> pvMetadataCollection = database.getCollection(pvMetadataCollectionName);
        pvMetadataCollection.createIndex(Indexes.ascending(PV_GROUP_KEY));

        // set up tasks the thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = MongoInsertTask.generateTaskList(pvMetadataCollection, pvMetadataBatchList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        long recordsInsertedCount = 0;
        List<String> idsInserted = new ArrayList<>();
        if (numThreads == 0) {
            // simple scenario without threading
            for (var batch : pvMetadataBatchList) {
                InsertManyResult insertManyResult = pvMetadataCollection.insertMany(batch);
                recordsInsertedCount = recordsInsertedCount + insertManyResult.getInsertedIds().size();
                if (!insertManyResult.wasAcknowledged()) {
                    LOGGER.error("mongodb error in collection.insertMany(), write not acknowledged");
                    System.exit(1);
                }
                for (var entry : insertManyResult.getInsertedIds().entrySet()) {
                    idsInserted.add(entry.getValue().asString().getValue());
                }
            }

        } else {
            // use multithreading to accomplish scenario
            recordsInsertedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        // calculate and display stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = recordsInsertedCount / dtSecondsWrite;
        LOGGER.debug("metadata records written to mongodb: " + recordsInsertedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        long collectionSize = database.getCollection(pvMetadataCollectionName).countDocuments();
        if (collectionSize != numPvs) {
            LOGGER.error("collection: " + pvMetadataCollectionName + " doesn't contain expected number of documents: " + numPvs + "(" + numPvs + ")");
            System.exit(1);
        }

        BenchmarkCreateResult result = new BenchmarkCreateResult();
        result.setWriteRate(writeRate);
        result.setIdsInserted(idsInserted);

        // remove collection
        if (removeCollection) {
            database.getCollection(pvMetadataCollectionName).drop();
            LOGGER.debug("removed collection: " + pvMetadataCollectionName);
        } else {
            result.setCollectionName(pvMetadataCollectionName);
        }

        return result;
    }

    private static void benchmarkCreatePvMetadataExperiment(MongoDatabase database) {

        System.out.println("============================");
        System.out.println("Starting Create Metadata Experiment");
        System.out.println("============================");

        // set up scenario
        final int[] numPvsArray = {100_000, 250_000, 500_000, 1_000_000};
        final int[] batchSizeArray = {5_000, 10_000, 25_000, 50_000, 100_000};
        final int[] numThreadsArray = {/*0, 1, 2, 3,*/ 5, 7};

        // run batch size experiment for each numPvs value
        Map<Integer,Map<Integer,Map<Integer,Double>>> resultMap = new TreeMap<>();
        for (int numPvs : numPvsArray) {
            Map<Integer,Map<Integer,Double>> writeRateMap = new TreeMap<>();
            for (int batchSize : batchSizeArray) {
                Map<Integer,Double> threadRateMap = new TreeMap<>();
                for (int numThreads : numThreadsArray) {
                    LOGGER.info("running create metadata benchmark scenario numPvs: "
                            + numPvs + " batchSize: " + batchSize + " numThreads: " + numThreads);
                    BenchmarkCreateResult result =
                            benchmarkCreatePvMetadata(database, numPvs, 1, batchSize, numThreads, true);
                    double writeRate = result.getWriteRate();
                    threadRateMap.put(numThreads, writeRate);
                }
                writeRateMap.put(batchSize, threadRateMap);
            }
            resultMap.put(numPvs, writeRateMap);
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 1_000_000;
        System.out.println("============================");
        System.out.println("Create Metadata Experiment Results");
        System.out.println("============================");
        for (var entry : resultMap.entrySet()) {
            int numPvs = entry.getKey();
            var batchSizeMap = entry.getValue();
            for (var batchEntry : batchSizeMap.entrySet()) {
                int batchSize = batchEntry.getKey();
                var writeRateMap = batchEntry.getValue();
                for (var rateEntry : writeRateMap.entrySet()) {
                    int numThreads = rateEntry.getKey();
                    double writeRate = rateEntry.getValue();
                    System.out.println("numPvs: " + numPvs + " batchSize: " + batchSize + " numThreads: " + numThreads + " writeRate: " + writeRate + " writes/sec");
                    if (writeRate > maxRate) {
                        maxRate = writeRate;
                    }
                    if (writeRate < minRate) {
                        minRate = writeRate;
                    }
                }
            }
        }
        System.out.println("max write rate: " + maxRate);
        System.out.println("min write rate: " + minRate);
    }

    static class MongoUpdateOneTask extends MongoBenchmarkTask implements Callable<BenchmarkTaskResult> {
        private MongoCollection<Document> collection = null;
        private Map<String,Bson> pvFilterMap = null;
        private List<Bson> updateOperationList = null;

        public MongoUpdateOneTask(MongoCollection<Document> collection, Map<String,Bson> pvFilterMap, List<Bson> updateOperationList) {
            this.collection = collection;
            this.pvFilterMap = pvFilterMap;
            this.updateOperationList = updateOperationList;
        }

        public static List<MongoBenchmarkTask> generateTaskList(
                MongoCollection<Document> collection, List<Map<String,Bson>> pvGroupList, List<Bson> updateOperationList) {

            List<MongoBenchmarkTask> taskList = new ArrayList<>();
            for (var pvGroup : pvGroupList) {
                taskList.add(new MongoUpdateOneTask(collection, pvGroup, updateOperationList));
            }
            return taskList;
        }

        public BenchmarkTaskResult call() {

            BenchmarkTaskResult result = new BenchmarkTaskResult();

            long recordsUpdatedCount = 0;
            for (Bson updateOperation : updateOperationList) {
                for (var pvFilterMapEntry : pvFilterMap.entrySet()) {
                    String pvId = pvFilterMapEntry.getKey();
                    Bson filter = pvFilterMapEntry.getValue();
                    final com.mongodb.client.result.UpdateResult updateResult = collection.updateOne(filter, updateOperation);
                    final long recordsMatched = updateResult.getMatchedCount();
                    final long recordsUpdated = updateResult.getModifiedCount();
                    final boolean wasAcknowldedged = updateResult.wasAcknowledged();
                    if ((!wasAcknowldedged) || (recordsMatched == 0)) {
                        LOGGER.error("updateOne failed for pv: " + pvId + " wasAcknowledged: " + wasAcknowldedged + " recordsMatched: " + recordsMatched);
                        System.exit(1);
                    }
                    recordsUpdatedCount = recordsUpdatedCount + 1;
                }
            }

            result.setStatus(true);
            result.setRecordsAffected(recordsUpdatedCount);
            return result;
        }
    }

    private static BenchmarkUpdateResult benchmarkUpdateOnePvMetadata(MongoDatabase database, MongoCollection<Document> pvMetadataCollection, List<String> pvIds, int numUpdatesPerPv, int numThreads) {

        // create list of maps with pv name key and mongo filter for pv, list contains one map per thread
        int pvGroupSize = 0;
        if (numThreads == 0) {
            pvGroupSize = pvIds.size();
        } else {
            pvGroupSize = (pvIds.size() / numThreads) + 1;
        }
        LOGGER.debug("updateOne benchmark pvIds.size: " + pvIds.size() + " numUpdatesPerPv: " + numUpdatesPerPv + " numThreads: " + numThreads);
        LOGGER.debug("updateOne benchmark pvGroupSize: " + pvGroupSize);
        List<Map<String,Bson>> pvGroupList = new ArrayList<>();
        Map<String,Bson> pvGroupMap = new TreeMap<>();
        int groupCount = 0;
        for (String pvId : pvIds) {
            groupCount = groupCount + 1;
            final Bson filter = eq("_id", pvId);
            pvGroupMap.put(pvId, filter);
            if (groupCount == pvGroupSize) {
                pvGroupList.add(pvGroupMap);
                pvGroupMap = new TreeMap<>();
                groupCount = 0;
            }
        }
        if (pvGroupMap.size() > 0) {
            pvGroupList.add(pvGroupMap);
        }

        // create list of mongo update operations, one per millisecond for the specified number of updates
        List<Bson> updateOperationList = generateUpdateOperationList(numUpdatesPerPv);
        LOGGER.debug("updateOne benchmark updateOperationList.size: " + updateOperationList.size());

        // set up tasks for thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = MongoUpdateOneTask.generateTaskList(pvMetadataCollection, pvGroupList, updateOperationList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // run update scenario, using numThreads to control multithreading
        long recordsUpdatedCount = 0;
        if (numThreads == 0) {
            // benchmark updateOne without multithreading
            Map<String,Bson> pvFilterMap = pvGroupList.get(0);
            for (Bson updateOperation : updateOperationList) {
                for (var pvFilterMapEntry : pvFilterMap.entrySet()) {
                    String pvId = pvFilterMapEntry.getKey();
                    Bson filter = pvFilterMapEntry.getValue();
                    final com.mongodb.client.result.UpdateResult updateResult = pvMetadataCollection.updateOne(filter, updateOperation);
                    final long recordsMatched = updateResult.getMatchedCount();
                    final long recordsUpdated = updateResult.getModifiedCount();
                    final boolean wasAcknowldedged = updateResult.wasAcknowledged();
                    if ((!wasAcknowldedged) || (recordsMatched == 0)) {
                        LOGGER.error("updateOne failed for pv: " + pvId + " wasAcknowledged: " + wasAcknowldedged + " recordsMatched: " + recordsMatched);
                        System.exit(1);
                    }
                    recordsUpdatedCount = recordsUpdatedCount + 1;
                }
            }
        } else {
            // use multithreading to accomplish scenario
            recordsUpdatedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        int expectedRecordsUpdatedCount = pvIds.size() * numUpdatesPerPv;
        if (recordsUpdatedCount != expectedRecordsUpdatedCount) {
            LOGGER.error("updateOne benchmark recordsUpdatedCount mismatch: "
                    + recordsUpdatedCount + " expected (numPvs*numUpdatesPerPv): " + expectedRecordsUpdatedCount);
            System.exit(1);
        }

        // calculate and display stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = recordsUpdatedCount / dtSecondsWrite;
        LOGGER.debug("metadata records updated: " + recordsUpdatedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        BenchmarkUpdateResult result = new BenchmarkUpdateResult();
        result.setWriteRate(writeRate);
        return result;
    }

    private static void benchmarkUpdateOnePvMetadataExperiment(MongoDatabase database) {

        System.out.println("============================");
        System.out.println("Starting UpdateOne Metadata Experiment");
        System.out.println("============================");

        // set up scenario
        final int numPvs = 4_000;
        final int numUpdatesPerPv = 10;
        final int[] numThreadsArray = {/*0, 1, 2, 3,*/ 5, 7};

        // create collection of PV metadata documents for update experiment
        LOGGER.debug("creating collection of PV metadata documents");
        BenchmarkCreateResult createResult = benchmarkCreatePvMetadata(database, numPvs, 1, numPvs, 0, false);
        String collectionName = createResult.getCollectionName();
        MongoCollection<Document> pvMetadataCollection = database.getCollection(collectionName);
        List<String> pvIds = createResult.getIdsInserted();
        LOGGER.debug("created collection: " + collectionName + " document count: " + pvMetadataCollection.countDocuments());

        // run update experiment, varying threadSize
        Map <Integer,Double> writeRateMap = new TreeMap<>();
        for (int numThreads : numThreadsArray) {
            LOGGER.info("running updateOne benchmark scenario numPvs: " + numPvs
                    + " numUpdatesPerPv: " + numUpdatesPerPv + " numThreads: " + numThreads);
            BenchmarkUpdateResult updateResult =
                    benchmarkUpdateOnePvMetadata(database, pvMetadataCollection, pvIds, numUpdatesPerPv, numThreads);
            double writeRate = updateResult.getWriteRate();
            writeRateMap.put(numThreads, writeRate);
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 1_000_000;
        System.out.println("============================");
        System.out.println("UpdateOne Metadata Experiment Results");
        System.out.println("============================");
        for (var rateEntry : writeRateMap.entrySet()) {
            int numThreads = rateEntry.getKey();
            double writeRate = rateEntry.getValue();
            System.out.println("numPvs: " + numPvs + " numUpdatesPerPv: " + numUpdatesPerPv + " numThreads: " + numThreads + " writeRate: " + writeRate + " writes/sec");
            if (writeRate > maxRate) {
                maxRate = writeRate;
            }
            if (writeRate < minRate) {
                minRate = writeRate;
            }
        }
        System.out.println("max write rate: " + maxRate);
        System.out.println("min write rate: " + minRate);

        // remove PV metadata document collection
        pvMetadataCollection.drop();
        LOGGER.debug("removed collection: " + collectionName);
    }

    static class MongoUpdateManyTask extends MongoBenchmarkTask {
        private MongoCollection<Document> collection = null;
        private Bson pvGroupFilter = null;
        private List<Bson> updateOperationList = null;

        public MongoUpdateManyTask(MongoCollection<Document> collection, Bson pvGroupFilter, List<Bson> updateOperationList) {
            this.collection = collection;
            this.pvGroupFilter = pvGroupFilter;
            this.updateOperationList = updateOperationList;
        }

        public static List<MongoBenchmarkTask> generateTaskList(
                MongoCollection<Document> collection, List<Bson> pvGroupFilterList, List<Bson> updateOperationList) {
            List<MongoBenchmarkTask> taskList = new ArrayList<>();
            for (var pvGroupFilter : pvGroupFilterList) {
                taskList.add(new MongoUpdateManyTask(collection, pvGroupFilter, updateOperationList));
            }
            return taskList;
        }

        public BenchmarkTaskResult call() {

            BenchmarkTaskResult result = new BenchmarkTaskResult();

            long recordsUpdatedCount = 0;
            for (Bson updateOperation : updateOperationList) {
                final com.mongodb.client.result.UpdateResult updateResult = collection.updateMany(pvGroupFilter, updateOperation);
                final long recordsMatched = updateResult.getMatchedCount();
                final long recordsUpdated = updateResult.getModifiedCount();
                final boolean wasAcknowldedged = updateResult.wasAcknowledged();
                if ((!wasAcknowldedged) || (recordsMatched == 0)) {
                    LOGGER.error("updateMany failed for pvGroupFilter: " + pvGroupFilter + " wasAcknowledged: " + wasAcknowldedged + " recordsUpdated: " + recordsUpdated);
                    System.exit(1);
                }
                recordsUpdatedCount = recordsUpdatedCount + recordsUpdated;
            }

            result.setStatus(true);
            result.setRecordsAffected(recordsUpdatedCount);
            return result;
        }
    }

    private static BenchmarkUpdateResult benchmarkUpdateManyPvMetadata(
            MongoDatabase database, int numPvs, int numUpdatesPerPv, int numPvGroups, int numThreads) {

        // create collection of PV metadata documents for scenario with specified numPvGroups for experiment
        LOGGER.debug("updateMany benchmark creating collection of PV metadata documents");
        BenchmarkCreateResult createResult = benchmarkCreatePvMetadata(database, numPvs, numPvGroups, 1, 5, false);
        String collectionName = createResult.getCollectionName();
        MongoCollection<Document> pvMetadataCollection = database.getCollection(collectionName);
        List<String> pvIds = createResult.getIdsInserted();
        LOGGER.debug("updateMany benchmark created collection: " + collectionName + " document count: " + pvMetadataCollection.countDocuments());

        // create list of filter mongodb filters, one for each pvGroup
        List<Bson> pvGroupFilterList = new ArrayList<>();
        for (int i = 1 ; i <= numPvGroups ; i++) {
            Bson pvGroupFilter = eq(PV_GROUP_KEY, i);
            pvGroupFilterList.add(pvGroupFilter);
        }
        LOGGER.debug("updateMany benchmark pvGroupFilterList.size: " + pvGroupFilterList.size());

        // create list of mongo update operations, one per millisecond for the specified number of updates
        List<Bson> updateOperationList = generateUpdateOperationList(numUpdatesPerPv);
        LOGGER.debug("updateMany benchmark updateOperationList.size: " + updateOperationList.size());

        // set up tasks for thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = MongoUpdateManyTask.generateTaskList(pvMetadataCollection, pvGroupFilterList, updateOperationList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // run updateMany scenario, using numThreads to control multithreading
        long recordsUpdatedCount = 0;
        if (numThreads == 0) {
            // benchmark without multithreading
            for (Bson updateOperation : updateOperationList) {
                for (var pvGroupFilter : pvGroupFilterList) {
                    final com.mongodb.client.result.UpdateResult updateResult = pvMetadataCollection.updateMany(pvGroupFilter, updateOperation);
                    final long recordsMatched = updateResult.getMatchedCount();
                    final long recordsUpdated = updateResult.getModifiedCount();
                    final boolean wasAcknowldedged = updateResult.wasAcknowledged();
                    if ((!wasAcknowldedged) || (recordsMatched == 0)) {
                        LOGGER.error("updateMany failed for pv group: "
                                + pvGroupFilter.toString() + " wasAcknowledged: " + wasAcknowldedged + " recordsUpdated: " + recordsUpdated);
                        System.exit(1);
                    }
                    recordsUpdatedCount = recordsUpdatedCount + recordsUpdated;
                }
            }
        } else {
            // use multithreading to accomplish scenario
            recordsUpdatedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        if (recordsUpdatedCount != numPvs) {
            LOGGER.error("updateMany benchmark recordsUpdatedCount mismatch: "
                    + recordsUpdatedCount + " expected (numPvs): " + numPvs);
            System.exit(1);
        }

        // calculate and display stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = recordsUpdatedCount / dtSecondsWrite;
        LOGGER.debug("metadata records updated: " + recordsUpdatedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        // remove PV metadata document collection
        pvMetadataCollection.drop();
        LOGGER.debug("removed collection: " + collectionName);

        BenchmarkUpdateResult result = new BenchmarkUpdateResult();
        result.setWriteRate(writeRate);
        return result;
    }

    private static void benchmarkUpdateManyPvMetadataExperiment(MongoDatabase database) {

        System.out.println("============================");
        System.out.println("Starting UpdateMany Metadata Experiment");
        System.out.println("============================");

        // set up scenario
        final int numPvs = 4_000;
        final int numUpdatesPerPv = 10;
        final int[] numPvGroupsArray = {10, 25, 50, 100, 250, 500};
        final int[] numThreadsArray = {/*0, 1, 2, 3,*/ 5, 7};

        // run update experiment, varying numPvGroups and numThreads
        Map <Integer, Map<Integer,Double>> writeRateMap = new TreeMap<>();
        for (int numPvGroups : numPvGroupsArray) {

            Map<Integer,Double> threadRateMap = new TreeMap<>();
            for (int numThreads : numThreadsArray) {
                LOGGER.info("running updateMany benchmark scenario numPvs: " + numPvs + " numUpdatesPerPv: "
                        + numUpdatesPerPv + "numPvGroups: " + numPvGroups + " numThreads: " + numThreads);
                BenchmarkUpdateResult updateResult =
                        benchmarkUpdateManyPvMetadata(database, numPvs, numUpdatesPerPv, numPvGroups, numThreads);
                double writeRate = updateResult.getWriteRate();
                threadRateMap.put(numThreads, writeRate);
            }
            writeRateMap.put(numPvGroups, threadRateMap);
        }

        // print results summary
        double maxRate = 0.0;
        double minRate = 1_000_000;
        System.out.println("============================");
        System.out.println("UpdateMany Metadata Experiment Results");
        System.out.println("============================");
        for (var groupEntry : writeRateMap.entrySet()) {
            int numGroups = groupEntry.getKey();
            Map<Integer,Double> threadRateMap = groupEntry.getValue();
            for (var rateEntry : threadRateMap.entrySet()) {
                int numThreads = rateEntry.getKey();
                double writeRate = rateEntry.getValue();
                System.out.println("numGroups: " + numGroups + " numThreads: " + numThreads + " writeRate: " + writeRate + " writes/sec");
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

    private static BenchmarkCreateResult benchmarkCreatePvTimeseriesDataBucket(
            MongoDatabase database, int numPvs, int numSamplesPerDocument, int numDocumentsPerPv,
            int batchSize, int numThreads, boolean removeCollection) {

        // generate PV time series data document list for scenario
        List<List<Document>> documentList =
                generatePvTimeseriesDocumentBatchListBucket(numPvs, numSamplesPerDocument, numDocumentsPerPv, batchSize);

        // create collection
        final String collectionName = "pvTsData-" + batchSize + "-" + numThreads + "-" + System.currentTimeMillis();
        LOGGER.debug("creating PV time series data collection: " + collectionName);
        database.createCollection(collectionName);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        // add indexes so that we see how that impacts write performance
        collection.createIndex(Indexes.ascending(PV_NAME_KEY));
        collection.createIndex(Indexes.ascending(PV_NAME_KEY, FIRST_TIME_KEY));
        collection.createIndex(Indexes.ascending(PV_NAME_KEY, LAST_TIME_KEY));

        // set up tasks for thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = MongoInsertTask.generateTaskList(collection, documentList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // insert documents in collection, multithreading controlled by numThreads
        long recordsInsertedCount = 0;
        List<String> idsInserted = new ArrayList<>();
        if (numThreads == 0) {
            // simple scenario without threading
            for (var batch : documentList) {
                InsertManyResult insertManyResult = collection.insertMany(batch);
                recordsInsertedCount = recordsInsertedCount + insertManyResult.getInsertedIds().size();
                if (!insertManyResult.wasAcknowledged()) {
                    LOGGER.error("mongodb error in collection.insertMany(), write not acknowledged");
                    System.exit(1);
                }
                for (var entry : insertManyResult.getInsertedIds().entrySet()) {
                    idsInserted.add(entry.getValue().asString().getValue());
                }
            }

        } else {
            // use multithreading to accomplish scenario
            recordsInsertedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        // calculate and display stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = recordsInsertedCount / dtSecondsWrite;
        LOGGER.debug("records written to mongodb: " + recordsInsertedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        long collectionSize = database.getCollection(collectionName).countDocuments();
        if (collectionSize != (numPvs * numDocumentsPerPv)) {
            LOGGER.error("collection: " + collectionName + " doesn't contain expected number of documents: " + numPvs + "(" + numPvs + ")");
            System.exit(1);
        }

        // convert records inserted to data values inserted
        long dataValuesInsertedCount = recordsInsertedCount * numSamplesPerDocument;
        writeRate = dataValuesInsertedCount / dtSecondsWrite;
        LOGGER.debug("recordsInsertedCount: " + recordsInsertedCount + " dataValuesInsertedCount: " + dataValuesInsertedCount);
        LOGGER.debug("dtSecondsWrite: " + dtSecondsWrite + " writeRate: " + writeRate);

        BenchmarkCreateResult result = new BenchmarkCreateResult();
        result.setWriteRate(writeRate);
        result.setIdsInserted(idsInserted);

        // remove collection
        if (removeCollection) {
            database.getCollection(collectionName).drop();
            LOGGER.debug("removed collection: " + collectionName);
        } else {
            result.setCollectionName(collectionName);
        }

        return result;
    }

    private static void benchmarkCreatePvTimeseriesDataExperimentBucket(MongoDatabase database) {

        System.out.println("============================");
        System.out.println("Starting Create PV Time Series Data Experiment (bucketed data)");
        System.out.println("============================");

        // set up scenario
        final int numPvs = 4_000;
        final int numSamplesPerDocument = 1_000;
        final int numDocumentsPerPv = 10;
        final int[] batchSizeArray = {100, 250, 500, 750, 1000, 2000};
        final int[] numThreadsArray = {/*0, 1, 2, 3,*/ 5, 7};

        // run experiment varying batchSize and numThreads
        Map<Integer,Map<Integer,Double>> writeRateMap = new TreeMap<>();
        for (int batchSize : batchSizeArray) {
            Map<Integer,Double> threadRateMap = new TreeMap<>();
            for (int numThreads : numThreadsArray) {
                LOGGER.info("running create PV time series data benchmark scenario numPvs: "
                        + numPvs + " batchSize: " + batchSize + " numThreads: " + numThreads);
                BenchmarkCreateResult result =
                        benchmarkCreatePvTimeseriesDataBucket(
                                database, numPvs, numSamplesPerDocument, numDocumentsPerPv, batchSize, numThreads, true);
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

    private static BenchmarkCreateResult benchmarkCreatePvTimeseriesDataTsCollection(
            MongoDatabase database, int numPvs, int numPvSamplesPerSecond, int numSeconds,
            int batchSize, int numThreads, boolean removeCollection) {

        // generate PV time series data document list for scenario
        List<List<Document>> documentList =
                generatePvTimeseriesDocumentBatchListTsCollection(
                        numPvs, numPvSamplesPerSecond, numSeconds, batchSize);

        // create time series collection
        final String collectionName = "pvTsData-" + batchSize + "-" + numThreads + "-" + System.currentTimeMillis();
        LOGGER.debug("creating PV time series data collection: " + collectionName);
        TimeSeriesOptions tsOptions = new TimeSeriesOptions(TIMESTAMP_KEY);
        tsOptions.metaField(METADATA_KEY);
        CreateCollectionOptions collOptions = new CreateCollectionOptions().timeSeriesOptions(tsOptions);
        database.createCollection(collectionName, collOptions);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        // set up tasks for thread executor service
        List<MongoBenchmarkTask> taskList = null;
        if (numThreads > 0) {
            taskList = MongoInsertTask.generateTaskList(collection, documentList);
        }

        // record start time for performance benchmark
        Instant t0Write = Instant.now();

        // insert documents in collection, multithreading controlled by numThreads
        long recordsInsertedCount = 0;
        List<String> idsInserted = new ArrayList<>();
        if (numThreads == 0) {
            // simple scenario without threading
            for (var batch : documentList) {
                InsertManyResult insertManyResult = collection.insertMany(batch);
                recordsInsertedCount = recordsInsertedCount + insertManyResult.getInsertedIds().size();
                if (!insertManyResult.wasAcknowledged()) {
                    LOGGER.error("mongodb error in collection.insertMany(), write not acknowledged");
                    System.exit(1);
                }
                for (var entry : insertManyResult.getInsertedIds().entrySet()) {
                    idsInserted.add(entry.getValue().toString());
                }
            }

        } else {
            // use multithreading to accomplish scenario
            recordsInsertedCount = createExecutorServiceAndInvokeTasks(numThreads, taskList);
        }

        // calculate and display stats
        Instant t1Write = Instant.now();
        long dtMillisWrite = t0Write.until(t1Write, ChronoUnit.MILLIS);
        double dtSecondsWrite = dtMillisWrite / 1_000.0;
        double writeRate = recordsInsertedCount / dtSecondsWrite;
        LOGGER.debug("records written to mongodb: " + recordsInsertedCount);
        LOGGER.debug("seconds to write data: " + dtSecondsWrite);
        LOGGER.debug("rate writes/sec: " + writeRate);

        long collectionSize = database.getCollection(collectionName).countDocuments();
        if (collectionSize != (numPvs * numPvSamplesPerSecond * numSeconds)) {
            LOGGER.error("collection: " + collectionName + " doesn't contain expected number of documents: " + numPvs + "(" + numPvs + ")");
            System.exit(1);
        }

        BenchmarkCreateResult result = new BenchmarkCreateResult();
        result.setWriteRate(writeRate);
        result.setIdsInserted(idsInserted);

        // remove collection
        if (removeCollection) {
            database.getCollection(collectionName).drop();
            LOGGER.debug("removed collection: " + collectionName);
        } else {
            result.setCollectionName(collectionName);
        }

        return result;
    }

    private static void benchmarkCreatePvTimeseriesDataExperimentTsCollection(MongoDatabase database) {

        System.out.println("============================");
        System.out.println("Starting Create PV Time Series Data Experiment (MongoDB Time Series Collection)");
        System.out.println("============================");

        // set up scenario
        final int numPvs = 4_000;
        final int numPvSamplesPerSecond = 1_000;
        final int numSeconds = 1;
        final int[] batchSizeArray = {/*100, 250, 500, 750,*/ 1000, 2000};
        final int[] numThreadsArray = {/*0, 1, 2, 3,*/ 5, 7};

        // run experiment varying batchSize and numThreads
        Map<Integer,Map<Integer,Double>> writeRateMap = new TreeMap<>();
        for (int batchSize : batchSizeArray) {
            Map<Integer,Double> threadRateMap = new TreeMap<>();
            for (int numThreads : numThreadsArray) {
                LOGGER.info("running create PV time series data (TS collection) benchmark scenario numPvs: "
                        + numPvs + " batchSize: " + batchSize + " numThreads: " + numThreads);
                BenchmarkCreateResult result =
                        benchmarkCreatePvTimeseriesDataTsCollection(
                                database, numPvs, numPvSamplesPerSecond, numSeconds, batchSize, numThreads, true);
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

        // mongodb debug logging is overwhelming and probably affects performance, so set to error level
        Logger mongoLogger = (Logger) LoggerFactory.getLogger("org.mongodb.driver");
        mongoLogger.setLevel(Level.ERROR); // turn off DEBUG logging for mongodb

        // set log level for this class
        LOGGER.setLevel(Level.INFO); // set level for this class to INFO
        LOGGER.debug("debug test"); // try a debug level message, should be filtered out when level is INFO

//        // uncomment line below to use docker mongo installation
//        final String mongoConnectString = "mongodb://datastore:datastore@localhost:27017/?authSource=admin";

        // uncomment line below to use local mongo installation without authentication
        final String mongoConnectString = "mongodb://localhost:27017/";

        final String mongoBenchmarkDatabase = "benchmark";

        MongoClient mongoClient = MongoClients.create(mongoConnectString);
        MongoDatabase database = mongoClient.getDatabase(mongoBenchmarkDatabase);

//        benchmarkCreatePvMetadataExperiment(database);
//        benchmarkUpdateOnePvMetadataExperiment(database);
        benchmarkUpdateManyPvMetadataExperiment(database);
//        benchmarkCreatePvTimeseriesDataExperimentBucket(database);
//        benchmarkCreatePvTimeseriesDataExperimentTsCollection(database);
    }
}
