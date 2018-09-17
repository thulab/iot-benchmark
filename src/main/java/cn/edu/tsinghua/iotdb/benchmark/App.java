package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.db.*;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Resolve;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Storage;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.benchmark.sersyslog.*;
import cn.edu.tsinghua.iotdb.benchmark.tool.ImportDataFromCSV;
import cn.edu.tsinghua.iotdb.benchmark.tool.MetaDateBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        CommandCli cli = new CommandCli();
        if (!cli.init(args)) {
            return;
        }
        Config config = ConfigDescriptor.getInstance().getConfig();
        switch (config.BENCHMARK_WORK_MODE.trim()) {
            case Constants.MODE_SERVER_MODE:
                serverMode(config);
                break;
            case Constants.MODE_INSERT_TEST_WITH_DEFAULT_PATH:
                insertTest(config);
                break;
            case Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH:
                genData(config);
                break;
            case Constants.MODE_QUERY_TEST_WITH_DEFAULT_PATH:
                queryTest(config);
                break;
            case Constants.MODE_IMPORT_DATA_FROM_CSV:
                importDataFromCSV(config);
                break;
            case Constants.MODE_EXECUTE_SQL_FROM_FILE:
                executeSQLFromFile(config);
                break;
            case Constants.MODE_CLIENT_SYSTEM_INFO:
                clientSystemInfo(config);
                break;
            default:
                throw new SQLException("unsupported mode " + config.BENCHMARK_WORK_MODE);
        }

    }// main

    /**
     * 将数据从CSV文件导入IOTDB
     *
     * @throws SQLException
     */
    private static void importDataFromCSV(Config config) throws SQLException {
        MetaDateBuilder builder = new MetaDateBuilder();
        builder.createMataData(config.METADATA_FILE_PATH);
        ImportDataFromCSV importTool = new ImportDataFromCSV();
        importTool.importData(config.IMPORT_DATA_FILE_PATH);
    }

    private static void clientSystemInfo(Config config) {
        double abnormalValue = -1;
        MySqlLog mySql = new MySqlLog();
        mySql.initMysql(System.currentTimeMillis());
        File dir = new File(config.LOG_STOP_FLAG_PATH);
        if (dir.exists() && dir.isDirectory()) {
            File file = new File(config.LOG_STOP_FLAG_PATH + "/log_stop_flag");
            int interval = config.INTERVAL;
            HashMap<IoUsage.IOStatistics,Float> ioStatistics;
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
                mySql.insertSERVER_MODE(
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
                        abnormalValue,
                        ioStatistics.get(IoUsage.IOStatistics.TPS),
                        ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                        ioStatistics.get(IoUsage.IOStatistics.MB_WRTN),
                        openFileList, "");

                try {
                    Thread.sleep(interval * 1000);
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
            LOGGER.error("LOG_STOP_FLAG_PATH not exist!");
        }

        mySql.closeMysql();
    }

    /**
     * 服务器端模式，监测系统内存等性能指标，获得插入的数据文件大小
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private static void serverMode(Config config) {
        MySqlLog mySql = new MySqlLog();
        mySql.initMysql(System.currentTimeMillis());
        File dir = new File(config.LOG_STOP_FLAG_PATH);

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
                File file = new File(config.LOG_STOP_FLAG_PATH + "/log_stop_flag");
                HashMap<FileSize.FileSizeKinds,Double> fileSizeStatistics;
                HashMap<IoUsage.IOStatistics,Float> ioStatistics;
                int interval = config.INTERVAL;
                // 检测所需的时间在目前代码的参数下至少为2秒
                LOGGER.info("----------New Test Begin with interval about {} s----------", interval + 2);
                while (true) {
                    ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
                    ArrayList<Float> netUsageList = NetUsage.getInstance().get();
                    ArrayList<Integer> openFileList = OpenFileNumber.getInstance().get();
                    fileSizeStatistics = FileSize.getInstance().getFileSize();
                    ioStatistics = IoUsage.getInstance().getIOStatistics();
                    LOGGER.info("CPU使用率,{}", ioUsageList.get(0));
                    LOGGER.info("内存使用率,{}", MemUsage.getInstance().get());
                    LOGGER.info("内存使用大小GB,{}", MemUsage.getInstance().getProcessMemUsage());
                    LOGGER.info("磁盘IO使用率,{},TPS,{},读速率MB/s,{},写速率MB/s,{}",
                            ioUsageList.get(1),
                            ioStatistics.get(IoUsage.IOStatistics.TPS),
                            ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                            ioStatistics.get(IoUsage.IOStatistics.MB_WRTN));
                    LOGGER.info("eth0接收和发送速率,{},{},KB/s", netUsageList.get(0), netUsageList.get(1));
                    LOGGER.info("PID={},打开文件总数{},打开data目录下文件数{},打开socket数{}", OpenFileNumber.getInstance().getPid(),
                            openFileList.get(0), openFileList.get(1), openFileList.get(2));
                    LOGGER.info("文件大小GB,data,{},info,{},metadata,{},overflow,{},delta,{},wal,{}",
                            fileSizeStatistics.get(FileSize.FileSizeKinds.DATA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.INFO),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.METADATA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.OVERFLOW),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.DELTA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.WAL));
                    mySql.insertSERVER_MODE(
                            ioUsageList.get(0),
                            MemUsage.getInstance().get(),
                            ioUsageList.get(1),
                            netUsageList.get(0),
                            netUsageList.get(1),
                            MemUsage.getInstance().getProcessMemUsage(),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.DATA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.INFO),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.METADATA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.OVERFLOW),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.DELTA),
                            fileSizeStatistics.get(FileSize.FileSizeKinds.WAL),
                            ioStatistics.get(IoUsage.IOStatistics.TPS),
                            ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                            ioStatistics.get(IoUsage.IOStatistics.MB_WRTN),
                            openFileList, "");
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
                        Thread.sleep(interval * 1000);
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
                LOGGER.error("LOG_STOP_FLAG_PATH not exist!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            mySql.closeMysql();
            try {
                if (out != null)
                    out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static void executeSQLFromFile(Config config) throws SQLException, ClassNotFoundException {
        MySqlLog mysql = new MySqlLog();
        mysql.initMysql(System.currentTimeMillis());
        IDBFactory idbFactory = null;
        idbFactory = getDBFactory(config);
        IDatebase datebase;
        long exeSQLFromFileStartTime;
        long exeSQLFromFileEndTime;
        float exeSQLFromFileTime = 1;
        int SQLCount = 0;
        try {
            datebase = idbFactory.buildDB(mysql.getLabID());
            datebase.init();
            exeSQLFromFileStartTime = System.nanoTime();
            datebase.exeSQLFromFileByOneBatch();
            datebase.close();
            exeSQLFromFileEndTime = System.nanoTime();
            exeSQLFromFileTime = (exeSQLFromFileEndTime - exeSQLFromFileStartTime) / 1000000000.0f;
        } catch (SQLException e) {
            LOGGER.error("Fail to init database becasue {}", e.getMessage());
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
        /*
         * LOGGER.
         * info("Execute SQL from file {} by one batch cost {} seconds. Mean rate {} SQL/s"
         * , config.SQL_FILE, exeSQLFromFileTime, 1.0f * SQLCount / exeSQLFromFileTime
         * );
         */

        // 加入新版的mysql表中
        mysql.closeMysql();
    }

    private static void genData(Config config) throws SQLException, ClassNotFoundException {
        // 一次生成一个timeseries的数据
        MySqlLog mysql = new MySqlLog();
        mysql.initMysql(System.currentTimeMillis());
        mysql.saveTestModel(config.TIMESERIES_TYPE, config.ENCODING);
        mysql.savaTestConfig();
        IDBFactory idbFactory = null;
        idbFactory = getDBFactory(config);

        IDatebase datebase;
        long createSchemaStartTime;
        long createSchemaEndTime;
        float createSchemaTime;
        try {
            datebase = idbFactory.buildDB(mysql.getLabID());
            datebase.init();
            createSchemaStartTime = System.nanoTime();
            datebase.createSchemaOfDataGen();
            datebase.close();
            createSchemaEndTime = System.nanoTime();
            createSchemaTime = (createSchemaEndTime - createSchemaStartTime) / 1000000000.0f;
        } catch (SQLException e) {
            LOGGER.error("Fail to init database becasue {}", e.getMessage());
            return;
        }

        ArrayList<Long> totalInsertErrorNums = new ArrayList<>();
        long totalErrorPoint;

        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        ArrayList<Long> totalTimes = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            executorService.submit(new ClientThread(idbFactory.buildDB(mysql.getLabID()), i, downLatch, totalTimes,
                    totalInsertErrorNums));
        }
        executorService.shutdown();
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long totalTime = 0;
        for (long c : totalTimes) {
            if (c > totalTime) {
                totalTime = c;
            }
        }
        long totalPoints = config.LOOP * config.CACHE_NUM;
        if (config.DB_SWITCH.equals(Constants.DB_IOT) && config.MUL_DEV_BATCH) {
            totalPoints = config.SENSOR_NUMBER * config.CLIENT_NUMBER * config.LOOP * config.CACHE_NUM;
        }

        totalErrorPoint = getErrorNum(config, totalInsertErrorNums, datebase);
        LOGGER.info(
                "GROUP_NUMBER = ,{}, DEVICE_NUMBER = ,{}, SENSOR_NUMBER = ,{}, CACHE_NUM = ,{}, POINT_STEP = ,{}, LOOP = ,{}, MUL_DEV_BATCH = ,{}",
                config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER, config.CACHE_NUM, config.POINT_STEP,
                config.LOOP, config.MUL_DEV_BATCH);

        LOGGER.info("Loaded ,{}, points in ,{},s with ,{}, workers (mean rate ,{}, points/s)", totalPoints,
                totalTime / 1000000000.0f, config.CLIENT_NUMBER,
                1000000000.0f * (totalPoints - totalErrorPoint) / (float) totalTime);

        LOGGER.info("Total error num is {}, create schema cost {},s", totalErrorPoint, createSchemaTime);

        // 加入新版的mysql表中
        mysql.saveResult("createSchemaTime(s)", "" + createSchemaTime);
        mysql.saveResult("totalPoints", "" + totalPoints);
        mysql.saveResult("totalTime(s)", "" + totalTime / 1000000000.0f);
        mysql.saveResult("totalErrorPoint", "" + totalErrorPoint);
        mysql.closeMysql();

    }

    /**
     * 数据库插入测试
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private static void insertTest(Config config) throws SQLException, ClassNotFoundException {
        MySqlLog mysql = new MySqlLog();
        mysql.initMysql(System.currentTimeMillis());

        mysql.saveTestModel("Double", config.ENCODING);
        mysql.savaTestConfig();

        IDBFactory idbFactory = null;
        idbFactory = getDBFactory(config);

        IDatebase datebase;
        long createSchemaStartTime;
        long createSchemaEndTime;
        float createSchemaTime;

        long insertStartTime = System.nanoTime();
        try {
            datebase = idbFactory.buildDB(mysql.getLabID());
            datebase.init();
            createSchemaStartTime = System.nanoTime();
            if(config.CREATE_SCHEMA) {
                datebase.createSchema();
            }
            datebase.close();
            createSchemaEndTime = System.nanoTime();
            createSchemaTime = (createSchemaEndTime - createSchemaStartTime) / 1000000000.0f;
        } catch (SQLException e) {
            LOGGER.error("Fail to init database becasue {}", e.getMessage());
            return;
        }

        ArrayList<Long> totalInsertErrorNums = new ArrayList<>();
        long totalErrorPoint;
        if (config.READ_FROM_FILE) {
            CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
            ArrayList<Long> totalTimes = new ArrayList<>();
            Storage storage = new Storage();
            ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER + 1);
            executorService.submit(new Resolve(config.FILE_PATH, storage));
            for (int i = 0; i < config.CLIENT_NUMBER; i++) {
                executorService.submit(new ClientThread(idbFactory.buildDB(mysql.getLabID()), i, storage, downLatch,
                        totalTimes, totalInsertErrorNums));
            }
            executorService.shutdown();
            // wait for all threads complete
            try {
                downLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            int totalItem = storage.getStoragedProductNum();
            long totalTime = 0;
            for (long c : totalTimes) {
                if (c > totalTime) {
                    totalTime = c;
                }
            }
            LOGGER.info("READ_FROM_FILE = true, TAG_PATH = ,{}, STORE_MODE = ,{}, BATCH_OP_NUM = ,{}", config.TAG_PATH,
                    config.STORE_MODE, config.BATCH_OP_NUM);
            LOGGER.info("loaded ,{}, items in ,{},s with ,{}, workers (mean rate ,{}, items/s)", totalItem,
                    totalTime / 1000000000.0f, config.CLIENT_NUMBER, (1000000000.0f * totalItem) / ((float) totalTime));

        } else {
            CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
            ArrayList<Long> totalTimes = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
            for (int i = 0; i < config.CLIENT_NUMBER; i++) {
                executorService.submit(new ClientThread(idbFactory.buildDB(mysql.getLabID()), i, downLatch, totalTimes,
                        totalInsertErrorNums));
            }
            executorService.shutdown();
            try {
                downLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long totalTime = 0;
            for (long c : totalTimes) {
                if (c > totalTime) {
                    totalTime = c;
                }
            }
            long totalPoints = config.SENSOR_NUMBER * config.DEVICE_NUMBER * config.LOOP * config.CACHE_NUM;
            if (config.DB_SWITCH.equals(Constants.DB_IOT) && config.MUL_DEV_BATCH) {
                totalPoints = config.SENSOR_NUMBER * config.CLIENT_NUMBER * config.LOOP * config.CACHE_NUM;
            }
            long insertEndTime = System.nanoTime();
            float insertElapseTime = (insertEndTime - insertStartTime) / 1000000000.0f;
            totalErrorPoint = getErrorNum(config, totalInsertErrorNums, datebase);
            LOGGER.info(
                    "Config: \n " +
                            "GROUP_NUMBER = ,{}, \n" +
                            "DEVICE_NUMBER = ,{}, \n" +
                            "SENSOR_NUMBER = ,{}, \n" +
                            "CACHE_NUM = ,{}, \n" +
                            "POINT_STEP = ,{}, \n" +
                            "LOOP = ,{}, \n" +
                            "MUL_DEV_BATCH = ,{} \n",
                    config.GROUP_NUMBER, config.DEVICE_NUMBER, config.SENSOR_NUMBER, config.CACHE_NUM,
                    config.POINT_STEP, config.LOOP, config.MUL_DEV_BATCH);

            LOGGER.info("Loaded ,{}, points in ,{},s with ,{}, workers (mean rate ,{}, points/s)", totalPoints,
                    totalTime / 1000000000.0f, config.CLIENT_NUMBER,
                    1000000000.0f * (totalPoints - totalErrorPoint) / (float) totalTime);

            LOGGER.info("Total error num is {}, create schema cost {} second. Total elapse time: {} second", totalErrorPoint, createSchemaTime, insertElapseTime);

            mysql.saveResult("createSchemaTime(s)", "" + createSchemaTime);
            mysql.saveResult("totalPoints", "" + totalPoints);
            mysql.saveResult("totalInsertionTime(s)", "" + totalTime / 1000000000.0f);
            mysql.saveResult("totalElapseTime(s)", "" + insertElapseTime);
            mysql.saveResult("totalErrorPoint", "" + totalErrorPoint);
            mysql.closeMysql();

        } // else--

    }

    private static long getErrorNum(Config config, ArrayList<Long> totalInsertErrorNums, IDatebase datebase)
            throws SQLException {
        long totalErrorPoint;
        switch (config.DB_SWITCH.trim()) {
            case Constants.DB_IOT:
                totalErrorPoint = getErrorNumIoT(totalInsertErrorNums);
                break;
            case Constants.DB_INFLUX:
                totalErrorPoint = getErrorNumInflux(config, datebase);
                break;
			case Constants.DB_OPENTS:
				totalErrorPoint = getErrorNumIoT(totalInsertErrorNums);
				break;
            case Constants.DB_CTS:
                totalErrorPoint = getErrorNumIoT(totalInsertErrorNums);
                break;
            default:
                throw new SQLException("unsupported database " + config.DB_SWITCH);
        }
        return totalErrorPoint;
    }

    private static IDBFactory getDBFactory(Config config) throws SQLException {
        switch (config.DB_SWITCH) {
            case Constants.DB_IOT:
                return new IoTDBFactory();
            case Constants.DB_INFLUX:
                return new InfluxDBFactory();
			case Constants.DB_OPENTS:
				return new OpenTSDBFactory();
            case Constants.DB_CTS:
                return new CTSDBFactory();
            default:
                throw new SQLException("unsupported database " + config.DB_SWITCH);
        }
    }

    private static long getErrorNumInflux(Config config, IDatebase database) {
        // 同一个device中不同sensor的点数是相同的，因此不对sensor遍历
        long insertedPointNum = 0;
        int groupIndex = 0;
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        for (int i = 0; i < config.DEVICE_NUMBER; i++) {
            groupIndex = i / groupSize;
            insertedPointNum += database.count("group_" + groupIndex, "d_" + i, "s_0") * config.SENSOR_NUMBER;
        }
        try {
            database.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return config.SENSOR_NUMBER * config.DEVICE_NUMBER * config.LOOP * config.CACHE_NUM - insertedPointNum;
    }

    private static long getErrorNumIoT(ArrayList<Long> totalInsertErrorNums) {
        return getSumOfList(totalInsertErrorNums);
    }

    /**
     * 数据库查询测试
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private static void queryTest(Config config) throws SQLException, ClassNotFoundException {
        IDBFactory idbFactory = null;
        idbFactory = getDBFactory(config);

        IDatebase datebase = null;
        MySqlLog mySql = new MySqlLog();
        try {
            mySql.initMysql(System.currentTimeMillis());
            datebase = idbFactory.buildDB(mySql.getLabID());
            datebase.init();
        } catch (SQLException e) {
            LOGGER.error("Fail to connect to database becasue {}", e.getMessage());
            return;
        }

        mySql.saveTestModel("Double", config.ENCODING);

        mySql.savaTestConfig();

        CountDownLatch downLatch = new CountDownLatch(config.CLIENT_NUMBER);
        ArrayList<Long> totalTimes = new ArrayList<>();
        ArrayList<Long> totalPoints = new ArrayList<>();
        ArrayList<Long> totalQueryErrorNums = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(config.CLIENT_NUMBER);
        for (int i = 0; i < config.CLIENT_NUMBER; i++) {
            executorService.submit(new QueryClientThread(idbFactory.buildDB(mySql.getLabID()), i, downLatch, totalTimes,
                    totalPoints, totalQueryErrorNums));
        }
        executorService.shutdown();
        try {
            downLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long totalTime = 0;
        for (long c : totalTimes) {
            if (c > totalTime) {
                totalTime = c;
            }
        }
        long totalResultPoint = getSumOfList(totalPoints);

        LOGGER.info(
                "{}: execute ,{}, query in ,{}, seconds, get ,{}, result points with ,{}, workers (mean rate ,{}, points/s)",
                getQueryName(config), config.CLIENT_NUMBER * config.LOOP, (totalTime / 1000.0f) / 1000000.0, totalResultPoint,
                config.CLIENT_NUMBER, (1000.0f * totalResultPoint) / ((float) totalTime / 1000000.0f));

        long totalErrorPoint = getSumOfList(totalQueryErrorNums);
        LOGGER.info("total error num is {}", totalErrorPoint);

        mySql.saveResult("queryNumber", "" + config.CLIENT_NUMBER * config.LOOP);
        mySql.saveResult("totalPoint", "" + totalResultPoint);
        mySql.saveResult("totalTime(s)", "" + (totalTime / 1000.0f) / 1000000.0);
        mySql.saveResult("resultPointPerSecond(points/s)", "" + (1000.0f * (totalResultPoint)) / (totalTime / 1000000.0));
        mySql.saveResult("totalErrorQuery", "" + totalErrorPoint);

        mySql.closeMysql();
    }

    private static String getQueryName(Config config) throws SQLException {
        switch (config.QUERY_CHOICE) {
            case 1:
                return "Exact Point Query";
            case 2:
                return "Fuzzy Point Query";
            case 3:
                return "Aggregation Function Query";
            case 4:
                return "Range Query";
            case 5:
                return "Criteria Query";
            case 6:
                return "Nearest Point Query";
            case 7:
                return "Group By Query";
            case 8:
                return "Limit SLimit Query";
            case 9:
                return "Limit Criteria Query";
            case 10:
                return "Aggregation Function Query Without Filter";
            case 11:
                return "Aggregation Function Query With Value Filter";
            default:
                throw new SQLException("unsupported query type " + config.QUERY_CHOICE);
        }
    }

    /**
     * 计算list中所有元素的和
     */
    private static long getSumOfList(ArrayList<Long> list) {
        long total = 0;
        for (long c : list) {
            total += c;
        }
        return total;
    }

}
