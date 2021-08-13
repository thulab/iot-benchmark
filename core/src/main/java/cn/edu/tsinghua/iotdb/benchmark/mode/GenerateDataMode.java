package cn.edu.tsinghua.iotdb.benchmark.mode;


import cn.edu.tsinghua.iotdb.benchmark.client.GenerateDataClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.utils.FileUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticDataWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.BaseDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GenerateDataMode extends BaseMode{

    private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataMode.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final BaseDataSchema baseDataSchema = BaseDataSchema.getInstance();

    private static final double NANO_TO_SECOND = 1000000000.0d;

    /**
     * Start benchmark
     */
    @Override
    public void run() {
        if(!writeSchema(baseDataSchema.getAllDeviceSchema())){
            return;
        }

        // create getCLIENT_NUMBER() client threads to do the workloads
        List<GenerateDataClient> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
        CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());
        ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
        LOGGER.info("Generating workload buffer...");
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            GenerateDataClient client = new GenerateDataClient(i, downLatch, barrier, new SyntheticDataWorkload(i));
            clients.add(client);
            executorService.submit(client);
        }
        executorService.shutdown();
        try {
            // wait for all clients finish test
            downLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Exception occurred during waiting for all threads finish.", e);
            Thread.currentThread().interrupt();
        }
        LOGGER.info("Data Location: " + config.getFILE_PATH());
        LOGGER.info("Schema Location: " + FileUtils.union(config.getFILE_PATH(), "schema.txt") );
        LOGGER.info("Generate Info Location: " + FileUtils.union(config.getFILE_PATH(), "info.txt"));
    }

    /**
     * Write Schema to line
     * @param deviceSchemaList
     * @return
     */
    private boolean writeSchema(List<DeviceSchema> deviceSchemaList){
        try{
            // process target
            Path path = Paths.get(config.getFILE_PATH());
            Files.deleteIfExists(path);
            Files.createDirectories(path);

            LOGGER.info("Finish record schema.");

            Path schemaPath = Paths.get(FileUtils.union(config.getFILE_PATH(), Constants.SCHEMA_PATH));
            Files.createFile(schemaPath);
            for(DeviceSchema deviceSchema: deviceSchemaList){
                for(String sensor: deviceSchema.getSensors()){
                    Type type = baseDataSchema.getSensorType(deviceSchema.getDevice(), sensor);
                    String line = deviceSchema.getDevice() + " " + sensor + " " + type.index + "\n";
                    Files.write(schemaPath, line.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                }
            }

            Path infoPath = Paths.get(FileUtils.union(config.getFILE_PATH(), Constants.INFO_PATH));
            Files.createFile(infoPath);
            Files.write(infoPath, config.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE);
            return true;
        }catch (IOException ioException){
            ioException.printStackTrace();
            LOGGER.error("Failed to generate Schema. Please check whether " + config.getFILE_PATH() + " is empty." );
            return false;
        }
    }
}
