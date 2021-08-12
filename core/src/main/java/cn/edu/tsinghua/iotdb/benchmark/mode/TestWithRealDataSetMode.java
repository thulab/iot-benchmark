package cn.edu.tsinghua.iotdb.benchmark.mode;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.RealDatasetClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestWithRealDataSetMode extends BaseMode{

    private static final Logger LOGGER = LoggerFactory.getLogger(TestWithRealDataSetMode.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    /**
     * Start benchmark
     */
    @Override
    public void run() {
        // getBATCH_SIZE() is points number in this mode
        config.setBATCH_SIZE_PER_WRITE(config.getBATCH_SIZE_PER_WRITE() / config.getFIELDS().size());

        File dirFile = new File(config.getFILE_PATH());
        if (!dirFile.exists()) {
            LOGGER.error("{} does not exit", config.getFILE_PATH());
            return;
        }

        LOGGER.info("use dataset: {}", config.getDATA_SET());

        List<String> files = new ArrayList<>();
        getAllFiles(config.getFILE_PATH(), files);
        LOGGER.info("total files: {}", files.size());

        Collections.sort(files);

        // TODO register schema into BaseDataSchema

        List<DeviceSchema> deviceSchemaList = BasicReader.getDeviceSchemaList(files, config);

        Measurement measurement = new Measurement();
        DBWrapper dbWrapper = new DBWrapper(measurement);
        // register schema if needed
        try {
            LOGGER.info("start to init database {}", config.getNET_DEVICE());
            dbWrapper.init();
            if (config.isIS_DELETE_DATA()) {
                try {
                    LOGGER.info("start to clean old data");
                    dbWrapper.cleanup();
                } catch (TsdbException e) {
                    LOGGER.error("Cleanup {} failed because ", config.getNET_DEVICE(), e);
                }
            }
            try {
                // register device schema
                LOGGER.info("start to register schema");
                dbWrapper.registerSchema(deviceSchemaList);
            } catch (TsdbException e) {
                LOGGER.error("Register {} schema failed because ", config.getNET_DEVICE(), e);
            }
        } catch (TsdbException e) {
            LOGGER.error("Initialize {} failed because ", config.getNET_DEVICE(), e);
        } finally {
            try {
                dbWrapper.close();
            } catch (TsdbException e) {
                LOGGER.error("Close {} failed because ", config.getNET_DEVICE(), e);
            }
        }
        CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());

        List<List<String>> threadFiles = new ArrayList<>();
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            threadFiles.add(new ArrayList<>());
        }

        for (int i = 0; i < files.size(); i++) {
            String filePath = files.get(i);
            int thread = i % config.getCLIENT_NUMBER();
            threadFiles.get(thread).add(filePath);
        }

        // create getCLIENT_NUMBER() client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            Client client = new RealDatasetClient(i, downLatch, config, threadFiles.get(i), barrier);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    private static void getAllFiles(String strPath, List<String> files) {
        File f = new File(strPath);
        if (f.isDirectory()) {
            File[] fs = f.listFiles();
            assert fs != null;
            for (File f1 : fs) {
                String fsPath = f1.getAbsolutePath();
                getAllFiles(fsPath, files);
            }
        } else if (f.isFile()) {
            files.add(f.getAbsolutePath());
        }
    }
}
