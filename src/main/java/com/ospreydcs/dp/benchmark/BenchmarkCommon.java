package com.ospreydcs.dp.benchmark;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

public class BenchmarkCommon {

    private static Logger LOGGER = (Logger) LoggerFactory.getLogger(BenchmarkCommon.class);

    public static class ScenarioResult {
        private Double writeRate = null;

        public Double getWriteRate() {
            return writeRate;
        }

        public void setWriteRate(Double writeRate) {
            this.writeRate = writeRate;
        }
    }

    public static class BenchmarkTaskResult {
        private boolean status;
        private long recordsAffected = 0;
        private List<String> idsInserted = null;

        public boolean getStatus() {
            return status;
        }

        public void setStatus(boolean status) {
            this.status = status;
        }

        public long getRecordsAffected() {
            return recordsAffected;
        }

        public void setRecordsAffected(long recordsAffected) {
            this.recordsAffected = recordsAffected;
        }

        public List<String> getIdsInserted() {
            return idsInserted;
        }

        public void setIdsInserted(List<String> idsInserted) {
            this.idsInserted = idsInserted;
        }
    }

    public static abstract class MongoBenchmarkTask implements Callable<BenchmarkTaskResult> {
    }

    public static long createExecutorServiceAndInvokeTasks(int numThreads, List<MongoBenchmarkTask> taskList) {
        long recordsAffectedCount = 0;
        var executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<BenchmarkTaskResult>> resultList = null;
        boolean success = true;
        try {
            resultList = executorService.invokeAll(taskList);
            executorService.shutdown();
            if (executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                for (int i=0 ; i < resultList.size() ; i++) {
                    Future<BenchmarkTaskResult> future = resultList.get(i);
                    BenchmarkTaskResult taskResult = future.get();
                    if (!taskResult.getStatus()) {
                        success = false;
                    }
                    recordsAffectedCount = recordsAffectedCount + taskResult.getRecordsAffected();
                }
                if (!success) {
                    LOGGER.error("fatal error, thread pool task failed");
                    System.exit(1);
                }
            }
        } catch (InterruptedException | ExecutionException ex) {
            executorService.shutdownNow();
            LOGGER.error("ExecutorService exception invoking tasks: " + ex.getMessage());
            Thread.currentThread().interrupt();
        }
        return recordsAffectedCount;
    }

}
