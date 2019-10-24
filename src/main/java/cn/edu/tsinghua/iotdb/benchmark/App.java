package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.client.QueryRealDatasetClient;
import cn.edu.tsinghua.iotdb.benchmark.client.RealDatasetClient;
import cn.edu.tsinghua.iotdb.benchmark.client.SyntheticClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.FileSize;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.IoUsage;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.MemUsage;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.NetUsage;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.OpenFileNumber;
import cn.edu.tsinghua.iotdb.benchmark.tool.ImportDataFromCSV;
import cn.edu.tsinghua.iotdb.benchmark.tool.MetaDateBuilder;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private static final double NANO_TO_SECOND = 1000000000.0d;

    public static void main(String[] args) throws SQLException {

        CommandCli cli = new CommandCli();
        if (!cli.init(args)) {
            return;
        }
        Config config = ConfigDescriptor.getInstance().getConfig();
        switch (config.BENCHMARK_WORK_MODE.trim()) {
            case Constants.MODE_TEST_WITH_DEFAULT_PATH:
                testWithDefaultPath(config);
                break;
            case Constants.MODE_WRITE_WITH_REAL_DATASET:
                testWithRealDataSet(config);
                break;
            case Constants.MODE_QUERY_WITH_REAL_DATASET:
                queryWithRealDataSet(config);
                break;
            case Constants.MODE_SERVER_MODE:
                serverMode(config);
                break;
            case Constants.MODE_IMPORT_DATA_FROM_CSV:
                importDataFromCSV(config);
                break;
            case Constants.MODE_CLIENT_SYSTEM_INFO:
                clientSystemInfo(config);
                break;
            default:
                throw new SQLException("unsupported mode " + config.BENCHMARK_WORK_MODE);
        }

    }// main

    /**
     * 按比例选择workload执行的测试
     */
    private static void testWithDefaultPath(Config config) {
        PersistenceFactory persistenceFactory = new PersistenceFactory();
        ITestDataPersistence recorder = persistenceFactory.getPersistence();
        recorder.saveTestConfig();

        Measurement measurement = new Measurement();
        DBWrapper dbWrapper = new DBWrapper(measurement);
        // register schema if needed
        try {
            dbWrapper.init();
            if (config.IS_DELETE_DATA) {
                try {
                    dbWrapper.cleanup();
                } catch (TsdbException e) {
                    LOGGER.error("Cleanup {} failed because ", config.DB_SWITCH, e);
                }
            }
            try {
                DataSchema dataSchema = DataSchema.getInstance();
                List<DeviceSchema> schemaList = new ArrayList<>();
                for (List<DeviceSchema> schemas : dataSchema.getClientBindSchema().values()) {
                    schemaList.addAll(schemas);
                }
                dbWrapper.registerSchema(schemaList);
            } catch (TsdbException e) {
                LOGGER.error("Register {} schema failed because ", config.DB_SWITCH, e);
            }
        } catch (TsdbException e) {
            LOGGER.error("Initialize {} failed because ", config.DB_SWITCH, e);
        } finally {
            try {
                dbWrapper.close();
            } catch (TsdbException e) {
                LOGGER.error("Close {} failed because ", config.DB_SWITCH, e);
            }
        }
        // create CLIENT_NUMBER client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENT_NUMBER);
        long st = 0;
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        LOGGER.info("Generating workload buffer...");
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            SyntheticClient client = new SyntheticClient(i, downLatch, barrier);
            clients.add(client);
            st = System.nanoTime();
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    /**
     * 测试真实数据集
     *
     * @param config
     */
    private static void testWithRealDataSet(Config config) {
        // BATCH_SIZE is points number in this mode
        config.BATCH_SIZE = config.BATCH_SIZE / config.FIELDS.size();

        File dirFile = new File(config.FILE_PATH);
        if (!dirFile.exists()) {
            LOGGER.error(config.FILE_PATH + " does not exit");
            return;
        }

        LOGGER.info("use dataset: {}", config.DATA_SET);

        List<String> files = new ArrayList<>();
        getAllFiles(config.FILE_PATH, files);
        LOGGER.info("total files: {}", files.size());

        Collections.sort(files);

        List<DeviceSchema> deviceSchemaList = BasicReader.getDeviceSchemaList(files, config);

        Measurement measurement = new Measurement();
        DBWrapper dbWrapper = new DBWrapper(measurement);
        // register schema if needed
        try {
            LOGGER.info("start to init database {}", config.DB_SWITCH);
            dbWrapper.init();
            if (config.IS_DELETE_DATA) {
                try {
                    LOGGER.info("start to clean old data");
                    dbWrapper.cleanup();
                } catch (TsdbException e) {
                    LOGGER.error("Cleanup {} failed because ", config.DB_SWITCH, e);
                }
            }
            try {
                // register device schema
                LOGGER.info("start to register schema");
                dbWrapper.registerSchema(deviceSchemaList);
            } catch (TsdbException e) {
                LOGGER.error("Register {} schema failed because ", config.DB_SWITCH, e);
            }
        } catch (TsdbException e) {
            LOGGER.error("Initialize {} failed because ", config.DB_SWITCH, e);
        } finally {
            try {
                dbWrapper.close();
            } catch (TsdbException e) {
                LOGGER.error("Close {} failed because ", config.DB_SWITCH, e);
            }
        }
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENT_NUMBER);

        List<List<String>> threadFiles = new ArrayList<>();
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            threadFiles.add(new ArrayList<>());
        }

        for (int i = 0; i < files.size(); i++) {
            String filePath = files.get(i);
            int thread = i % config.CLIENT_NUMBER;
            threadFiles.get(thread).add(filePath);
        }

        // create CLIENT_NUMBER client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            Client client = new RealDatasetClient(i, downLatch, config, threadFiles.get(i), barrier);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    private static void finalMeasure(ExecutorService executorService, CountDownLatch downLatch,
                                     Measurement measurement, List<Measurement> threadsMeasurements,
                                     long st, List<Client> clients) {
        executorService.shutdown();

        try {
            // wait for all clients finish test
            downLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Exception occurred during waiting for all threads finish.", e);
            Thread.currentThread().interrupt();
        }
        long en = System.nanoTime();
        LOGGER.info("All clients finished.");
        // sum up all the measurements and calculate statistics
        measurement.setElapseTime((en - st) / NANO_TO_SECOND);
        for (Client client : clients) {
            threadsMeasurements.add(client.getMeasurement());
        }
        for (Measurement m : threadsMeasurements) {
            measurement.mergeMeasurement(m);
        }
        // must call calculateMetrics() before using the Metrics
        measurement.calculateMetrics();
        // output results
        measurement.showConfigs();
        measurement.showMeasurements();
        measurement.showMetrics();
    }

    /**
     * 测试真实数据集
     *
     * @param config configurations
     */
    private static void queryWithRealDataSet(Config config) {
        LOGGER.info("use dataset: {}", config.DATA_SET);
        //check whether the parameters are legitimate
        if (!checkParamForQueryRealDataSet(config)) {
            return;
        }

        Measurement measurement = new Measurement();
        CyclicBarrier barrier = new CyclicBarrier(config.CLIENT_NUMBER);

        // create CLIENT_NUMBER client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            Client client = new QueryRealDatasetClient(i, downLatch, barrier, config);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    private static boolean checkParamForQueryRealDataSet(Config config) {
        if (config.QUERY_SENSOR_NUM > config.FIELDS.size()) {
            LOGGER.error("QUERY_SENSOR_NUM={} can't greater than size of field, {}.",
                    config.QUERY_SENSOR_NUM, config.FIELDS);
            return false;
        }
        String[] split = config.OPERATION_PROPORTION.split(":");
        if (split.length != Operation.values().length) {
            LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
            return false;
        }
        if (!split[0].trim().equals("0")) {
            LOGGER.error("OPERATION_PROPORTION {} error, {} can't have write operation.",
                    config.OPERATION_PROPORTION, config.BENCHMARK_WORK_MODE);
            return false;
        }
        return true;
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

    /**
     * 将数据从CSV文件导入IOTDB
     */
    private static void importDataFromCSV(Config config) throws SQLException {
        MetaDateBuilder builder = new MetaDateBuilder();
        builder.createMataData(config.METADATA_FILE_PATH);
        ImportDataFromCSV importTool = new ImportDataFromCSV();
        importTool.importData(config.IMPORT_DATA_FILE_PATH);
    }

    private static void clientSystemInfo(Config config) {
        double abnormalValue = -1;
        PersistenceFactory persistenceFactory = new PersistenceFactory();
        ITestDataPersistence recorder = persistenceFactory.getPersistence();
        File dir = new File(config.DB_DATA_PATH);
        if (dir.exists() && dir.isDirectory()) {
            File file = new File(config.DB_DATA_PATH + "/log_stop_flag");
            int interval = config.INTERVAL;
            HashMap<IoUsage.IOStatistics, Float> ioStatistics;
            // 检测所需的时间在目前代码的参数下至少为2秒
            LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
            while (true) {
                ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
                ArrayList<Float> netUsageList = NetUsage.getInstance().get();
                ArrayList<Integer> openFileList = OpenFileNumber.getInstance().get();
                ioStatistics = IoUsage.getInstance().getIOStatistics();
                LOGGER.info("CPU使用率,{}", ioUsageList.get(0));
                LOGGER.info("内存使用率,{}", MemUsage.getInstance().get());
                LOGGER.info("内存使用大小GB,{}", MemUsage.getInstance().getProcessMemUsage());
                LOGGER.info("磁盘IO使用率,{},TPS,{},读速率MB/s,{},写速率MB/s,{}",
                        ioUsageList.get(1),
                        ioStatistics.get(IoUsage.IOStatistics.TPS),
                        ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                        ioStatistics.get(IoUsage.IOStatistics.MB_WRTN));
                LOGGER.info("网口接收和发送速率,{},{},KB/s", netUsageList.get(0), netUsageList.get(1));
                LOGGER.info("进程号,{},打开文件总数,{},打开benchmark目录下文件数,{},打开socket数,{}", OpenFileNumber.getInstance().getPid(),
                        openFileList.get(0), openFileList.get(1), openFileList.get(2));
                recorder.insertSystemMetrics(
                        ioUsageList.get(0),
                        MemUsage.getInstance().get(),
                        ioUsageList.get(1),
                        netUsageList.get(0),
                        netUsageList.get(1),
                        MemUsage.getInstance().getProcessMemUsage(),
                        abnormalValue,
                        abnormalValue,
                        abnormalValue,
                        abnormalValue,
                        abnormalValue,
                        ioStatistics.get(IoUsage.IOStatistics.TPS),
                        ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                        ioStatistics.get(IoUsage.IOStatistics.MB_WRTN),
                        openFileList);

                try {
                    Thread.sleep(interval * 1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (file.exists()) {
                    boolean f = file.delete();
                    if (!f) {
                        LOGGER.error("log_stop_flag 文件删除失败");
                    }
                    break;
                }
            }
        } else {
            LOGGER.error("DB_DATA_PATH not exist!");
        }

        recorder.close();
    }

    /**
     * 服务器端模式，监测系统内存等性能指标，获得插入的数据文件大小
     */
    private static void serverMode(Config config) {
        PersistenceFactory persistenceFactory = new PersistenceFactory();
        ITestDataPersistence recorder = persistenceFactory.getPersistence();
        File dir = new File(config.DB_DATA_PATH);

        boolean write2File = false;
        BufferedWriter out = null;
        char space = ' ';
        try {
            if (config.SERVER_MODE_INFO_FILE.length() > 0) {
                write2File = true;
                // if the file doesn't exits, then create the file, else append.
                out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(config.SERVER_MODE_INFO_FILE, true)));
                out.write(String.format("时间%15cCPU使用率%7c内存使用率%5c磁盘IO使用率%5ceth0接收速率%5ceth0发送速率%5cTotalFiles%5cDataAndWalFiles%5cSockets"
                                + "%5cdeltaFileNum%5cderbyFileNum%5cdigestFileNum%5cmetadataFileNum%5coverflowFileNum%5cwalsFileNum\r\n",
                        space, space, space, space, space, space, space, space));
            }

            if (dir.exists() && dir.isDirectory()) {
                File file = new File(config.DB_DATA_PATH + "/log_stop_flag");
                Map<FileSize.FileSizeKinds, Double> fileSizeStatistics;
                HashMap<IoUsage.IOStatistics, Float> ioStatistics;
                int interval = config.INTERVAL;
                // 检测所需的时间在目前代码的参数下至少为2秒
                LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
                while (true) {
                    long start = System.currentTimeMillis();
                    ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
                    LOGGER.info("IoUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
                    start = System.currentTimeMillis();
                    ArrayList<Float> netUsageList = NetUsage.getInstance().get();
                    LOGGER.info("NetUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
                    start = System.currentTimeMillis();
                    ArrayList<Integer> openFileList = new ArrayList<>();
                    for (int i = 0; i < 9; i++) {
                        openFileList.add(-1);
                    }
                    LOGGER.info("OpenFileNumber.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
                    start = System.currentTimeMillis();
                    fileSizeStatistics = FileSize.getInstance().getFileSize();
                    LOGGER.info("FileSize.getInstance().getFileSize() consume ,{}, ms", System.currentTimeMillis() - start);
                    start = System.currentTimeMillis();
                    ioStatistics = IoUsage.getInstance().getIOStatistics();
                    LOGGER.info("IoUsage.getInstance().getIOStatistics() consume ,{}, ms", System.currentTimeMillis() - start);


                    start = System.currentTimeMillis();
                    double memRate = MemUsage.getInstance().get();
                    LOGGER.info("MemUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
                    start = System.currentTimeMillis();
                    double proMem = MemUsage.getInstance().getProcessMemUsage();
                    LOGGER.info("MemUsage.getInstance().getProcessMemUsage() consume ,{}, ms", System.currentTimeMillis() - start);
                    LOGGER.info("内存使用大小GB,{}", proMem);
                    LOGGER.info("内存使用率,{}", memRate);
                    LOGGER.info("CPU使用率,{}", ioUsageList.get(0));
                    LOGGER.info("磁盘IO使用率,{},TPS,{},读速率MB/s,{},写速率MB/s,{}",
                            ioUsageList.get(1),
                            ioStatistics.get(IoUsage.IOStatistics.TPS),
                            ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                            ioStatistics.get(IoUsage.IOStatistics.MB_WRTN));
                    LOGGER.info("eth0接收和发送速率,{},{},KB/s", netUsageList.get(0), netUsageList.get(1));
                    LOGGER.info("PID={},打开文件总数{},打开data目录下文件数{},打开socket数{}", OpenFileNumber.getInstance().getPid(),
                            openFileList.get(0), openFileList.get(1), openFileList.get(2));
                    LOGGER.info("文件大小GB,data,{},system,{},sequence,{},overflow,{},wal,{}",
                            fileSizeStatistics.get(FileSize.FileSizeKinds.DATA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.STSTEM),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.SEQUENCE),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.OVERFLOW),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.WAL));
                    recorder.insertSystemMetrics(
                            ioUsageList.get(0),
                            MemUsage.getInstance().get(),
                            ioUsageList.get(1),
                            netUsageList.get(0),
                            netUsageList.get(1),
                            MemUsage.getInstance().getProcessMemUsage(),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.DATA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.STSTEM),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.SEQUENCE),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.OVERFLOW),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.WAL),
                            ioStatistics.get(IoUsage.IOStatistics.TPS),
                            ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                            ioStatistics.get(IoUsage.IOStatistics.MB_WRTN),
                            openFileList);
                    if (write2File) {
                        out.write(String.format("%d%14f%14f%15f", System.currentTimeMillis(),
                                ioUsageList.get(0), MemUsage.getInstance().get(), ioUsageList.get(1)));
                        out.write(String.format("%16f%16f%12d%8s%8d%10s%5d", netUsageList.get(0),
                                netUsageList.get(1), openFileList.get(0), space, openFileList.get(1),
                                space, openFileList.get(2)));
                        out.write(String.format("%16d%16d%16d%16d%16d%16d\n", openFileList.get(3),
                                openFileList.get(4), openFileList.get(5), space, openFileList.get(6),
                                openFileList.get(7), openFileList.get(8)));
                    }
                    try {
                        Thread.sleep(interval * 1000L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (file.exists()) {
                        boolean f = file.delete();
                        if (!f) {
                            LOGGER.error("log_stop_flag 文件删除失败");
                        }
                        break;
                    }
                }
            } else {
                LOGGER.error("DB_DATA_PATH not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            recorder.close();
            try {
                if (out != null)
                    out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
