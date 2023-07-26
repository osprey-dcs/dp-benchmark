package com.ospreydcs.dp.benchmark.grpc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.ospreydcs.dp.benchmark.v1.benchmark.BenchmarkGrpc;
import com.ospreydcs.dp.benchmark.v1.benchmark.Int64Msg;
import com.ospreydcs.dp.benchmark.v1.benchmark.SnapshotDataResponse;
import com.ospreydcs.dp.benchmark.v1.benchmark.SnapshotDataStatus;
import com.ospreydcs.dp.benchmark.v1.common.Data;
import com.ospreydcs.dp.benchmark.v1.ingestion.Ingestion.SnapshotData;
import com.ospreydcs.dp.benchmark.v1.ingestion.Ingestion.SnapshotID;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BenchmarkServer {
    private static Logger LOGGER = (Logger) LoggerFactory.getLogger(BenchmarkServer.class);

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50051;
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new BenchmarkImpl())
                .build()
                .start();
        LOGGER.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    BenchmarkServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {

        // so log levels
        ((Logger) LoggerFactory.getLogger("io.grpc.netty")).setLevel(Level.ERROR);
        ((Logger) LoggerFactory.getLogger("io.netty.util")).setLevel(Level.ERROR);
        LOGGER.setLevel(Level.INFO); // set level for this class to INFO
//        LOGGER.debug("debug test"); // try a debug level message, should be filtered out when level is INFO

        final BenchmarkServer server = new BenchmarkServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class BenchmarkImpl extends BenchmarkGrpc.BenchmarkImplBase {

        @Override
        public void unarySpam(Int64Msg request, StreamObserver<Int64Msg> responseObserver) {
            Int64Msg reply = Int64Msg.newBuilder().setValue(0).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void streamingSpam(Int64Msg request, StreamObserver<Int64Msg> responseObserver) {
            long requestValue = request.getValue();
            for (int n = 0; n < requestValue; n++) {
                Int64Msg response = Int64Msg.newBuilder().setValue(n).build();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
        }

        @Override
        public void unaryIngestion(SnapshotData request, StreamObserver<SnapshotID> responseObserver) {
            List<Data> pvDataList = request.getPvDataList();
            int pvDataListSize = pvDataList.size();
//            LOGGER.info("pvDataList size: " + pvDataList.size());
            SnapshotID reply = SnapshotID.newBuilder().setSnapshotID(1).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<SnapshotData> streamingIngestion(StreamObserver<SnapshotDataResponse> responseObserver) {
            return new StreamObserver<SnapshotData>() {
                int requestCount;
                Instant t0 = Instant.now();

                @Override
                public void onNext(SnapshotData snapshotData) {
                    boolean isError = false;
                    String errorMsg = "";
                    requestCount = requestCount + 1;
                    int colCount = snapshotData.getPvDataList().size();
                    int rowCount = 0;
                    if (colCount > 0) {
                        Data firstColumnData = snapshotData.getPvData(0);
                        Data lastColumnData = snapshotData.getPvData(colCount-1);
                        rowCount = firstColumnData.getDataCount();
                        if (firstColumnData.getData(rowCount-1).getFloatValue() != rowCount) {
                            isError = true;
                            errorMsg = "first column data error, expected value: "
                                    + rowCount + " in last row with index: " + (rowCount-1);
                        } else if (lastColumnData.getData(rowCount-1).getFloatValue() != rowCount) {
                            isError = true;
                            errorMsg = "last column data error, expected value: "
                                    + rowCount + " in last row with index: " + (rowCount-1);
                        }
                    }
                    LOGGER.debug("snapshotData request#: {} rowCount: {} colCount: {}", requestCount, rowCount, colCount);
                    SnapshotDataStatus status =
                            SnapshotDataStatus.newBuilder().setStatusMsg(errorMsg).setDataError(isError).build();
                    responseObserver.onNext(
                            SnapshotDataResponse.newBuilder().setColCount(colCount).setRowCount(rowCount).setStatus(status).build());
                }

                @Override
                public void onError(Throwable throwable) {
                    LOGGER.warn("Encountered error in streamingIngestion(): " + throwable.getMessage());
                }

                @Override
                public void onCompleted() {
                    Instant t1 = Instant.now();
                    responseObserver.onCompleted();
                    long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
                    double dtSeconds = dtMillis / 1_000.0;
                    LOGGER.info("snapshotData requestCount: {} seconds: {}", requestCount, dtSeconds);
                }
            };
        }
    }
}
