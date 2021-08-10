package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.workload.IRealDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public abstract class RealDataClient extends Client implements Runnable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(RealDataClient.class);

    private final OperationController operationController;
    private final IRealDataWorkload realDataWorkload;
    private final DataSchema dataSchema = DataSchema.getInstance();
    private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private long loopIndex;

    public RealDataClient(
            int id, CountDownLatch countDownLatch, CyclicBarrier barrier, IRealDataWorkload workload) {
        super(id, countDownLatch, barrier);
        realDataWorkload = workload;
        operationController = new OperationController(id);
    }

    @Override
    void doTest() {
        String currentThread = Thread.currentThread().getName();
        // actualDeviceFloor equals to device number when REAL_INSERT_RATE = 1
        // actualDeviceFloor = device_number * first_device_index(The first index of this benchmark)
        //                   + device_number * real_insert_rate(Actual number of devices generated)
        double actualDeviceFloor =
                config.getDEVICE_NUMBER() * config.getFIRST_DEVICE_INDEX()
                        + config.getDEVICE_NUMBER() * config.getREAL_INSERT_RATE();

        // print current progress periodically
        service.scheduleAtFixedRate(
                () -> {
                    String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.getLOOP());
                    LOGGER.info("{} {}% syntheticWorkload is done.", currentThread, percent);
                },
                1,
                config.getLOG_PRINT_INTERVAL(),
                TimeUnit.SECONDS);

        while(true){
            try {
                Batch batch = realDataWorkload.getOneBatch();
                if(batch == null){
                    break;
                }
                dbWrapper.insertOneBatch(batch);
            } catch (DBConnectException e) {
                LOGGER.error("Failed to insert one batch data because ", e);
            } catch (Exception e) {
                LOGGER.error("Failed to insert one batch data because ", e);
            }
        }

        service.shutdown();
    }
}
