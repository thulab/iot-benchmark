package cn.edu.tsinghua.iotdb.benchmark.db;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.PossionDistribution;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.loadData.Point;
import cn.edu.tsinghua.iotdb.benchmark.mysql.MySqlLog;
import cn.edu.tsinghua.iotdb.jdbc.TsfileJDBCConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class IoTDB implements IDatebase {
    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
    private static final String createStatementSQL = "create timeseries %s with datatype=DOUBLE,encoding=%s";
    private static final String createStatementFromFileSQL = "create timeseries %s with datatype=%s,encoding=%s";
    private static final String setStorageLevelSQL = "set storage group to %s";
    private Connection connection;
    private Config config;
    private List<Point> points;
    private Map<String, String> mp;
    private long labID;
    private MySqlLog mySql;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Random sensorRandom;
    private Random timestampRandom;
    private final double unitTransfer = 1000000000.0;

    public IoTDB(long labID) throws ClassNotFoundException, SQLException {
        Class.forName(TsfileJDBCConfig.JDBC_DRIVER_NAME);
        config = ConfigDescriptor.getInstance().getConfig();
        points = new ArrayList<>();
        mp = new HashMap<>();
        mySql = new MySqlLog();
        this.labID = labID;
        sensorRandom = new Random(1 + config.QUERY_SEED);
        timestampRandom = new Random(2 + config.QUERY_SEED);
    }

    @Override
    public void createSchema() throws SQLException {
        if (config.READ_FROM_FILE) {
            initSchema();
            mp.put("INT32", "PLAIN");
            mp.put("INT64", "PLAIN");
            mp.put("DOUBLE", "GORILLA");
            if (config.TAG_PATH) {
                if (config.STORE_MODE == 1) {
                    for (Point p : points) {
                        setStorgeGroup(p.getPath());
                    }
                } else {
                    Set<String> uniqueMeasurementName = new HashSet<>();
                    for (Point p : points) {
                        uniqueMeasurementName.add(p.measurement);
                    }
                    for (String measure : uniqueMeasurementName) {
                        setStorgeGroup("root." + measure);
                    }
                }

                for (Point p : points) {
                    Set<String> uniqueFieldName = new HashSet<>();
                    uniqueFieldName.addAll(p.fieldName);
                    for (String sensor : uniqueFieldName) {
                        createTimeseries(p.getPath(), sensor);
                    }
                }
            } else {
                setStorgeGroup("root.device_0");
                Set<String> uniqueFieldName = new HashSet<>();
                for (Point p : points) {
                    uniqueFieldName.addAll(p.fieldName);
                }
                for (String sensor : uniqueFieldName) {
                    createTimeseries("root.device_0", sensor);
                }
            }
        } else {
            int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
            ArrayList<String> group = new ArrayList<>();
            for (int i = 0; i < config.GROUP_NUMBER; i++) {
                group.add("group_" + i);
            }
            for (String g : group) {
                setStorgeGroup(g);
            }
            int count = 0;
            int groupIndex = 0;
            int timeseriesCount = 0;
            Statement statement = connection.createStatement();
            int timeseriesTotal = config.DEVICE_NUMBER * config.SENSOR_NUMBER;
            String path;
            for (String device : config.DEVICE_CODES) {
                if (count == groupSize) {
                    groupIndex++;
                    count = 0;
                }
                path = group.get(groupIndex) + "." + device;
                for (String sensor : config.SENSOR_CODES) {
                    // createTimeseries(path, sensor);
                    timeseriesCount++;
                    createTimeseriesBatch(path, sensor, timeseriesCount, timeseriesTotal, statement);
                }
                count++;
            }
        }

    }

    @Override
    public long getLabID(){
        return this.labID;
    }

    private void initSchema() {
        // 解析到points
        BufferedReader reader = null;
        try {
            File file = new File(config.FILE_PATH);
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;

            // 读到指定的行数或者文件结束
            while ((tempString = reader.readLine()) != null) {
                Point p = new Point();

                String[] pointInfo = tempString.split(" ");
                StringTokenizer st = new StringTokenizer(pointInfo[0], ",=");

                // 解析出measurement
                if (st.hasMoreElements())
                    p.measurement = st.nextToken().replace('.', '_');

                // 解析出tag的K-V对
                while (st.hasMoreElements()) {
                    p.tagName.add(st.nextToken().replace('.', '_'));
                    p.tagValue.add(st.nextToken().replace('.', '_').replace('/', '_'));
                }

                // 解析出field的K-V对
                st = new StringTokenizer(pointInfo[1], ",=");
                while (st.hasMoreElements()) {
                    p.fieldName.add(st.nextToken());
                    p.fieldValue.add(string2num(st.nextToken()));
                }

                p.time = Long.parseLong(pointInfo[2]);
                if (points.size() == 0 || !points.get(0).getPath().equals(p.getPath())) {
                    points.add(p);
                } else {
                    break;
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    private static Number string2num(String str) {
        if (str.endsWith("i")) {
            return Long.parseLong(str.substring(0, str.length() - 1));
        } else {
            return Double.parseDouble(str);
        }
    }

    @Override
    public void insertOneBatchMulDevice(LinkedList<String> deviceCodes, int loopIndex, ThreadLocal<Long> totalTime,
                                        ThreadLocal<Long> errorCount) {
        Statement statement;
        int[] result;
        long errorNum = 0;
        try {
            statement = connection.createStatement();
            int timeStep = config.CACHE_NUM / deviceCodes.size();
            if (!config.IS_OVERFLOW) {
                for (int i = 0; i < timeStep; i++) {
                    for (String device : deviceCodes) {
                        String sql = createSQLStatmentOfMulDevice(loopIndex, i, device);
                        statement.addBatch(sql);
                    }
                }
            } else {
                //随机重排，无法准确控制overflow比例
				/*
				int shuffleSize = (int) (config.OVERFLOW_RATIO * timeStep);
				int[] shuffleSequence = new int[shuffleSize];
				for(int i = 0; i < shuffleSize; i++){
					shuffleSequence[i] = i;
				}
				Random random = new Random(loopIndex);
				for(int i = 0; i < shuffleSize; i++){
					int p = random.nextInt(shuffleSize);
					int tmp = shuffleSequence[i];
					shuffleSequence[i] = shuffleSequence[p];
					shuffleSequence[p] = tmp;
				}

				for (int i = 0; i < shuffleSize; i++) {
					for (String device : deviceCodes) {
						String sql = createSQLStatmentOfMulDevice(loopIndex, shuffleSequence[i], device);
						statement.addBatch(sql);
					}
				}
				for (int i = shuffleSize; i < config.CACHE_NUM; i++) {
					for (String device : deviceCodes) {
						String sql = createSQLStatmentOfMulDevice(loopIndex, i, device);
						statement.addBatch(sql);
					}
				}
				*/
                int shuffleSize = (int) (config.OVERFLOW_RATIO * timeStep);
                int[] shuffleSequence = new int[shuffleSize];
                for (int i = 0; i < shuffleSize; i++) {
                    shuffleSequence[i] = i;
                }

                int tmp = shuffleSequence[shuffleSize - 1];
                shuffleSequence[shuffleSize - 1] = shuffleSequence[0];
                shuffleSequence[0] = tmp;

                for (int i = 0; i < shuffleSize; i++) {
                    for (String device : deviceCodes) {
                        String sql = createSQLStatmentOfMulDevice(loopIndex, shuffleSequence[i], device);
                        statement.addBatch(sql);
                    }
                }
                for (int i = shuffleSize; i < config.CACHE_NUM; i++) {
                    for (String device : deviceCodes) {
                        String sql = createSQLStatmentOfMulDevice(loopIndex, i, device);
                        statement.addBatch(sql);
                    }
                }

            }

            // 注意config.CACHE_NUM/(config.DEVICE_NUMBER/config.CLIENT_NUMBER)=整数,即批导入大小和客户端数的乘积可以被设备数整除
            for (int i = 0; i < (config.CACHE_NUM / deviceCodes.size()); i++) {
                for (String device : deviceCodes) {
                    String sql = createSQLStatmentOfMulDevice(loopIndex, i, device);
                    statement.addBatch(sql);
                }
            }
            long startTime = System.nanoTime();
            try {
                result = statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
            }
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();
            long costTime = endTime - startTime;

            if (errorNum > 0) {
                LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);

            } else {
                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
                        (totalTime.get() + costTime) / unitTransfer,
                        (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
                totalTime.set(totalTime.get() + costTime);
            }
            errorCount.set(errorCount.get() + errorNum);
            mySql.saveInsertProcess(loopIndex, costTime / unitTransfer, totalTime.get() / unitTransfer, errorNum, config.REMARK);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void insertOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime,
                               ThreadLocal<Long> errorCount) {
        Statement statement;
        int[] result;
        long errorNum = 0;
        try {
            statement = connection.createStatement();
            if (!config.IS_OVERFLOW) {
                for (int i = 0; i < config.CACHE_NUM; i++) {
                    String sql = createSQLStatment(loopIndex, i, device);
                    statement.addBatch(sql);
                }
            } else {
                //随机重排，无法准确控制overflow比例
				/*
				int shuffleSize = (int) (config.OVERFLOW_RATIO * config.CACHE_NUM);
				int[] shuffleSequence = new int[shuffleSize];
				for(int i = 0; i < shuffleSize; i++){
					shuffleSequence[i] = i;
				}
				Random random = new Random(loopIndex);
				for(int i = 0; i < shuffleSize; i++){
					int p = random.nextInt(shuffleSize);
					int tmp = shuffleSequence[i];
					shuffleSequence[i] = shuffleSequence[p];
					shuffleSequence[p] = tmp;
				}
				for (int i = 0; i < shuffleSize; i++) {
					String sql = createSQLStatment(loopIndex, shuffleSequence[i], device);
					statement.addBatch(sql);
				}
				for (int i = shuffleSize; i < config.CACHE_NUM; i++) {
					String sql = createSQLStatment(loopIndex, i, device);
					statement.addBatch(sql);
				}
				*/
                int shuffleSize = (int) (config.OVERFLOW_RATIO * config.CACHE_NUM);
                int[] shuffleSequence = new int[shuffleSize];
                for (int i = 0; i < shuffleSize; i++) {
                    shuffleSequence[i] = i;
                }

                int tmp = shuffleSequence[shuffleSize - 1];
                shuffleSequence[shuffleSize - 1] = shuffleSequence[0];
                shuffleSequence[0] = tmp;

                for (int i = 0; i < shuffleSize; i++) {
                    String sql = createSQLStatment(loopIndex, shuffleSequence[i], device);
                    statement.addBatch(sql);
                }
                for (int i = shuffleSize; i < config.CACHE_NUM; i++) {
                    String sql = createSQLStatment(loopIndex, i, device);
                    statement.addBatch(sql);
                }


            }

            long startTime = System.nanoTime();
            try {
                result = statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
            }
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();
            long costTime = endTime - startTime;
            if (errorNum > 0) {
                LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);
            } else {
                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
                        (totalTime.get() + costTime) / unitTransfer,
                        (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
                totalTime.set(totalTime.get() + costTime);
            }
            errorCount.set(errorCount.get() + errorNum);

            mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer, totalTime.get() / unitTransfer, errorNum,
                    config.REMARK);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int insertOverflowOneBatch(String device, int loopIndex,
                                      ThreadLocal<Long> totalTime,
                                      ThreadLocal<Long> errorCount,
                                      ArrayList<Integer> before,
                                      Integer maxTimestampIndex,
                                      Random random) {
        Statement statement;
        long errorNum = 0;
        int timestampIndex = maxTimestampIndex;

        try {
            statement = connection.createStatement();
            if (loopIndex == 0) {
                String sql = createSQLStatment(device, maxTimestampIndex);
                statement.addBatch(sql);
                for (int i = 1; i < config.CACHE_NUM; i++) {
                    if (returnTrueByProb(1.0 - config.OVERFLOW_RATIO, random)) {
                        maxTimestampIndex++;
                        timestampIndex = maxTimestampIndex;
                    } else {
                        int randomIndexOfBefore = (int) (random.nextDouble() * (before.size() - 1));
                        timestampIndex = before.get(randomIndexOfBefore);
                        before.remove(randomIndexOfBefore);
                    }
                    sql = createSQLStatment(device, timestampIndex);
                    statement.addBatch(sql);
                }
            } else {
                String sql;
                for (int i = 0; i < config.CACHE_NUM; i++) {
                    if (maxTimestampIndex < (config.CACHE_NUM * config.LOOP - 1) && before.size() > 0) {
                        if (returnTrueByProb(1.0 - config.OVERFLOW_RATIO, random)) {
                            maxTimestampIndex++;
                            timestampIndex = maxTimestampIndex;
                        } else {
                            int size = before.size() - 1;
                            int randomIndexOfBefore = (int) (random.nextDouble() * size);
                            timestampIndex = before.get(randomIndexOfBefore);
                            before.remove(randomIndexOfBefore);
                        }
                    } else if (before.size() == 0) {
                        maxTimestampIndex++;
                        timestampIndex = maxTimestampIndex;
                    } else if (before.size() > 0) {
                        int size = before.size() - 1;
                        int randomIndexOfBefore = (int) (random.nextDouble() * size);
                        timestampIndex = before.get(randomIndexOfBefore);
                        before.remove(randomIndexOfBefore);
                    }

                    sql = createSQLStatment(device, timestampIndex);
                    statement.addBatch(sql);
                }
            }

            long startTime = System.nanoTime();
            try {
                statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
            }
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();
            long costTime = endTime - startTime;

            if (errorNum > 0) {
                LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);
            } else {
                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
                        (totalTime.get() + costTime) / unitTransfer,
                        (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
                totalTime.set(totalTime.get() + costTime);
            }
            errorCount.set(errorCount.get() + errorNum);

            mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer, totalTime.get() / unitTransfer, errorNum,
                    config.REMARK);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return maxTimestampIndex;
    }

    @Override
    public int insertOverflowOneBatchDist(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount, Integer maxTimestampIndex, Random random) throws SQLException {
        Statement statement;
        long errorNum = 0;
        int timestampIndex;
        PossionDistribution possionDistribution = new PossionDistribution(random);
        int nextDelta;

        try {
            statement = connection.createStatement();
            if (loopIndex == 0) {
                String sql = createSQLStatment(device, maxTimestampIndex);
                statement.addBatch(sql);
                for (int i = 1; i < config.CACHE_NUM; i++) {
                    if (returnTrueByProb(1.0 - config.OVERFLOW_RATIO, random)) {
                        maxTimestampIndex++;
                        timestampIndex = maxTimestampIndex;
                    } else {
                        nextDelta = possionDistribution.getNextPossionDelta();
                        timestampIndex = maxTimestampIndex - nextDelta;
                    }
                    sql = createSQLStatment(device, timestampIndex);
                    statement.addBatch(sql);
                }
            } else {
                String sql;
                for (int i = 0; i < config.CACHE_NUM; i++) {
                    if (returnTrueByProb(1.0 - config.OVERFLOW_RATIO, random)) {
                        maxTimestampIndex++;
                        timestampIndex = maxTimestampIndex;
                    } else {
                        nextDelta = possionDistribution.getNextPossionDelta();
                        timestampIndex = maxTimestampIndex - nextDelta;
                    }
                    sql = createSQLStatment(device, timestampIndex);
                    statement.addBatch(sql);
                }
            }

            long startTime = System.nanoTime();
            try {
                statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
            }
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();
            long costTime = endTime - startTime;

            if (errorNum > 0) {
                LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);
            } else {
                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
                        (totalTime.get() + costTime) / unitTransfer,
                        (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
                totalTime.set(totalTime.get() + costTime);
            }
            errorCount.set(errorCount.get() + errorNum);

            mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer, totalTime.get() / unitTransfer, errorNum,
                    config.REMARK);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return maxTimestampIndex;
    }

    @Override
    public void insertOneBatch(LinkedList<String> cons, int batchIndex, ThreadLocal<Long> totalTime,
                               ThreadLocal<Long> errorCount) throws SQLException {

        Statement statement;
        int[] result;
        long errorNum = 0;
        try {
            statement = connection.createStatement();
            for (String sql : cons) {
                statement.addBatch(sql);
            }

            long startTime = System.nanoTime();
            try {
                result = statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
            }
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();


            if (errorNum > 0) {
                LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);

            } else {
                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} items/s",
                        Thread.currentThread().getName(), batchIndex, (endTime - startTime) / unitTransfer,
                        ((totalTime.get() + (endTime - startTime)) / unitTransfer),
                        (cons.size() / (double) (endTime - startTime)) * unitTransfer);
                totalTime.set(totalTime.get() + (endTime - startTime));
            }
            errorCount.set(errorCount.get() + errorNum);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executeOneQuery(List<Integer> devices, int index, long startTime, QueryClientThread client,
                                ThreadLocal<Long> errorCount) {
        Statement statement = null;
        String sql = "";
        long startTimeStamp = 0, endTimeStamp = 0;
        try {
            statement = connection.createStatement();
            List<String> sensorList = new ArrayList<String>();

            switch (config.QUERY_CHOICE) {
                case 1:// 精确点查询
                    long timeStamp = (startTime - Constants.START_TIMESTAMP) / config.POINT_STEP * config.POINT_STEP
                            + Constants.START_TIMESTAMP;
                    if (config.IS_EMPTY_PRECISE_POINT_QUERY) {
                        timeStamp += config.POINT_STEP / 2;
                    }
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, timeStamp, sensorList);
                    break;
                case 2:// 模糊点查询（暂未实现）
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, sensorList);
                    break;
                case 3:// 聚合函数查询
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, config.QUERY_AGGREGATE_FUN, startTime,
                            startTime + config.QUERY_INTERVAL, sensorList);
                    break;
                case 4:// 范围查询
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, startTime,
                            startTime + config.QUERY_INTERVAL, sensorList);
                    break;
                case 5:// 条件查询
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, startTime,
                            startTime + config.QUERY_INTERVAL, config.QUERY_LOWER_LIMIT, sensorList);
                    break;
                case 6:// 最近点查询
                    sql = createQuerySQLStatment(devices, config.QUERY_SENSOR_NUM, "max_time", sensorList);
                    break;
                case 7:// groupBy查询（暂时只有一个时间段）
                    List<Long> startTimes = new ArrayList<Long>();
                    List<Long> endTimes = new ArrayList<Long>();
                    startTimes.add(startTime);
                    endTimes.add(startTime + config.QUERY_INTERVAL);
                    sql = createQuerySQLStatment(devices, config.QUERY_AGGREGATE_FUN, config.QUERY_SENSOR_NUM,
                            startTimes, endTimes, config.QUERY_LOWER_LIMIT,
                            sensorList);
                    break;
            }
            int line = 0;
            StringBuilder builder = new StringBuilder(sql);
            LOGGER.info("{} execute {} loop,提交执行的sql：{}", Thread.currentThread().getName(), index, builder.toString());

            startTimeStamp = System.nanoTime();
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            while (resultSet.next()) {
                line++;
//				int sensorNum = sensorList.size();
//				builder.append(" \ntimestamp = ").append(resultSet.getString(0)).append("; ");
//				for (int i = 1; i <= sensorNum; i++) {
//					builder.append(resultSet.getString(i)).append("; ");
//				}	
            }
            statement.close();
            endTimeStamp = System.nanoTime();

//			LOGGER.info("{}",builder.toString());
            client.setTotalPoint(client.getTotalPoint() + line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM);
            client.setTotalTime(client.getTotalTime() + endTimeStamp - startTimeStamp);

            LOGGER.info(
                    "{} execute {} loop, it costs {}s with {} result points cur_rate is {}points/s; "
                            + "TotalTime {}s with totalPoint {} rate is {}points/s",
                    Thread.currentThread().getName(), index, ((endTimeStamp - startTimeStamp) / 1000.0) / 1000000.0,
                    line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM,
                    line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM * 1000.0 / ((endTimeStamp - startTimeStamp) / 1000000.0),
                    (client.getTotalTime() / 1000.0) / 1000000.0, client.getTotalPoint(),
                    client.getTotalPoint() * 1000.0f / (client.getTotalTime() / 1000000.0));
            mySql.saveQueryProcess(index, line * config.QUERY_SENSOR_NUM * config.QUERY_DEVICE_NUM,
                    ((endTimeStamp - startTimeStamp) / 1000.0f) / 1000000.0, config.REMARK);
        } catch (SQLException e) {
            errorCount.set(errorCount.get() + 1);
            LOGGER.error("{} execute query failed! Error：{}", Thread.currentThread().getName(), e.getMessage());
            LOGGER.error("执行失败的查询语句：{}", sql);
            mySql.saveQueryProcess(index, 0, -1, "query fail!" + sql);
            e.printStackTrace();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void createTimeseries(String path, String sensor) {
        Statement statement;
        try {
            statement = connection.createStatement();
            if (config.READ_FROM_FILE) {
                String type = getTypeByField(sensor);
                statement.execute(String.format(createStatementFromFileSQL,
                        path + "." + sensor, type, mp.get(type)));
            } else if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)) {
                statement.execute(String.format(createStatementFromFileSQL,
                        path + "." + sensor, config.TIMESERIES_TYPE, config.ENCODING));
                writeSQLIntoFile(String.format(createStatementFromFileSQL,
                        path + "." + sensor, config.TIMESERIES_TYPE, config.ENCODING), config.GEN_DATA_FILE_PATH);
            } else {
                statement.execute(String.format(createStatementSQL,
                        Constants.ROOT_SERIES_NAME + "." + path + "." + sensor, config.ENCODING));
            }
            statement.close();
        } catch (SQLException e) {
            LOGGER.warn(e.getMessage());
        }

    }

    private void createTimeseriesBatch(String path, String sensor, int count, int timeseriesTotal,
                                       Statement statement) {
        try {
            statement.addBatch(String.format(createStatementSQL, Constants.ROOT_SERIES_NAME + "." + path + "." + sensor,
                    config.ENCODING));
        } catch (SQLException e) {
            LOGGER.warn("Can`t add batch when creating timeseries because: {}", e.getMessage());
        }
        if ((count % 1000) == 0) {
            long startTime = System.nanoTime();
            try {
                statement.executeBatch();
            } catch (SQLException e) {
                LOGGER.warn("Can`t execute batch when creating timeseries because: {}", e.getMessage());
            }
            try {
                statement.clearBatch();
            } catch (SQLException e) {
                LOGGER.warn("Can`t clear batch when creating timeseries because: {}", e.getMessage());
            }
            long endTime = System.nanoTime();
            LOGGER.info("batch create timeseries execute speed ,{},timeseries/s",
                    1000000000000.0f / (endTime - startTime));
            mySql.saveResult("batch" + count / 1000 + "CreateTimeseriesSpeed", "" + 1000000000000.0f / (endTime - startTime));
            if (count >= timeseriesTotal) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.warn("Can`t close statement when creating timeseries because: {}", e.getMessage());
                }
            }
            // statement.close();
        } else if (count >= timeseriesTotal) {
            try {
                statement.executeBatch();
            } catch (SQLException e) {
                LOGGER.warn("Can`t execute batch when creating timeseries because: {}", e.getMessage());
            }
            try {
                statement.clearBatch();
            } catch (SQLException e) {
                LOGGER.warn("Can`t clear batch when creating timeseries because: {}", e.getMessage());
            }
            try {
                statement.close();
            } catch (SQLException e) {
                LOGGER.warn("Can`t close statement when creating timeseries because: {}", e.getMessage());
            }
        }
    }

    private void setStorgeGroup(String device) {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            if (config.READ_FROM_FILE) {
                statement.execute(String.format(setStorageLevelSQL, device));
            } else if (config.BENCHMARK_WORK_MODE.equals(Constants.MODE_INSERT_TEST_WITH_USERDEFINED_PATH)) {
                statement.execute(String.format(setStorageLevelSQL, config.STORAGE_GROUP_NAME));
                writeSQLIntoFile(String.format(setStorageLevelSQL, config.STORAGE_GROUP_NAME), config.GEN_DATA_FILE_PATH);
            } else {
                statement.execute(String.format(setStorageLevelSQL, Constants.ROOT_SERIES_NAME + "." + device));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        try{
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
            LOGGER.warn("Can`t close statement when setting storage group because: {}", e.getMessage());
        }
    }

    @Override
    public void init() throws SQLException {
        connection = DriverManager.getConnection(String.format(Constants.URL, config.host, config.port), Constants.USER,
                Constants.PASSWD);
        mySql.initMysql(labID);
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        if (mySql != null) {
            mySql.closeMysql();
        }
    }

    private String createSQLStatment(int batch, int index, String device) {
        StringBuilder builder = new StringBuilder();
        String path = getGroupDevicePath(device);
        builder.append("insert into ").append(Constants.ROOT_SERIES_NAME).append(".").append(path).append("(timestamp");
        for (String sensor : config.SENSOR_CODES) {
            builder.append(",").append(sensor);
        }
        builder.append(") values(");
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.CACHE_NUM + index);
        builder.append(currentTime);
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            builder.append(",").append(Function.getValueByFuntionidAndParam(param, currentTime));
        }
        builder.append(")");
        LOGGER.debug("createSQLStatment:  {}", builder.toString());
        return builder.toString();
    }

    private String createSQLStatment(String device, int timestampIndex) {
        StringBuilder builder = new StringBuilder();
        String path = getGroupDevicePath(device);
        builder.append("insert into ").append(Constants.ROOT_SERIES_NAME).append(".").append(path).append("(timestamp");
        for (String sensor : config.SENSOR_CODES) {
            builder.append(",").append(sensor);
        }
        builder.append(") values(");
        long currentTime;
        if (config.IS_RANDOM_TIMESTAMP_INTERVAL) {
            currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex + (long) (config.POINT_STEP * timestampRandom.nextDouble());
        } else {
            currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * timestampIndex;
        }
        builder.append(currentTime);
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            builder.append(",").append(Function.getValueByFuntionidAndParam(param, currentTime));
        }
        builder.append(")");
        //LOGGER.debug("timestampIndex:  ,{}", timestampIndex);
        LOGGER.debug("createSQLStatment:  {}", builder.toString());
        return builder.toString();
    }

    private String createGenDataSQLStatment(int batch, int index, String device) {
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ");
        String[] spl = device.split("\\.");
        builder.append(spl[0]);
        for (int i = 1; i < spl.length - 1; i++) {
            builder.append(".");
            builder.append(spl[i]);
        }
        builder.append("(timestamp");
        builder.append(",").append(spl[spl.length - 1]);
        builder.append(") values(");
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (batch * config.CACHE_NUM + index);
        builder.append(currentTime);
        try {
            builder.append(",").append(getDataByTypeAndScope(currentTime, config));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        builder.append(")");
        LOGGER.debug("createGenDataSQLStatment:  {}", builder.toString());
        return builder.toString();
    }

    private String getDataByTypeAndScope(long currentTime, Config config) throws SQLException {
        String data = null;
        switch (config.TIMESERIES_TYPE) {
            case "BOOLEAN":
                data = getRandomBoolean(currentTime);
                break;
            case "FLOAT":
                data = getRandomFloat(currentTime, config.TIMESERIES_VALUE_SCOPE);
                break;
            case "INT32":
                data = getRandomInt(currentTime, config.TIMESERIES_VALUE_SCOPE);
                break;
            case "TEXT":
                data = getRandomText(currentTime, config.TIMESERIES_VALUE_SCOPE);
                break;
            default:
                throw new SQLException("unsupported type " + config.TIMESERIES_TYPE);
        }
        return data;
    }

    private static String getRandomText(long currentTime, String text) {
        String[] enu = text.split(",");
        int max = enu.length;
        int min = 0;
        Random random = new Random(currentTime);
        int i = (int) (random.nextFloat() * max);
        return "\"" + enu[i] + "\"";
    }

    private static String getRandomInt(long currentTime, String scope) {
        String[] spl = scope.split(",");
        int min = Integer.parseInt(spl[0]);
        int max = Integer.parseInt(spl[1]);
        Random random = new Random(currentTime);
        int i = random.nextInt(max) % (max - min + 1) + min;
        return String.valueOf(i);
    }

    private static String getRandomBoolean(long currentTime) {
        Random random = new Random(currentTime);
        boolean b = random.nextBoolean();
        return String.valueOf(b);
    }

    private static String getRandomFloat(long currentTime, String scope) {
        String[] spl = scope.split(",");
        float min = Float.parseFloat(spl[0]);
        float max = Float.parseFloat(spl[1]);
        Random random = new Random(currentTime);
        float f = random.nextFloat() * (max - min) + min;
        return String.valueOf(f);
    }

    private String createSQLStatmentOfMulDevice(int loopIndex, int i, String device) {
        StringBuilder builder = new StringBuilder();
        String path = getGroupDevicePath(device);
        builder.append("insert into ").append(Constants.ROOT_SERIES_NAME).append(".").append(path).append("(timestamp");

        for (String sensor : config.SENSOR_CODES) {
            builder.append(",").append(sensor);
        }
        builder.append(") values(");
        long currentTime = Constants.START_TIMESTAMP + config.POINT_STEP * (loopIndex * config.CACHE_NUM / (config.DEVICE_NUMBER / config.CLIENT_NUMBER) + i);
        builder.append(currentTime);
        for (String sensor : config.SENSOR_CODES) {
            FunctionParam param = config.SENSOR_FUNCTION.get(sensor);
            builder.append(",").append(Function.getValueByFuntionidAndParam(param, currentTime));
        }
        builder.append(")");
        LOGGER.debug("createSQLStatmentOfMulDevice:  {}", builder.toString());
        return builder.toString();
    }

    /**
     * 创建查询语句--(精确点查询)
     *
     * @throws SQLException
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, long time, List<String> sensorList)
            throws SQLException {

        String strTime = sdf.format(new Date(time));
        StringBuilder builder = new StringBuilder(createQuerySQLStatment(devices, num, sensorList));
        builder.append(" WHERE time = ").append(strTime);
        return builder.toString();
    }

    /**
     * 创建查询语句--(查询设备下的num个传感器数值)
     *
     * @throws SQLException
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, List<String> sensorList) throws SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        if (num > config.SENSOR_NUMBER) {
            throw new SQLException("config.SENSOR_NUMBER is " + config.SENSOR_NUMBER
                    + " shouldn't less than the number of fields in querySql");
        }
        List<String> list = new ArrayList<String>();
        for (String sensor : config.SENSOR_CODES) {
            list.add(sensor);
        }
        Collections.shuffle(list, sensorRandom);
        builder.append(list.get(0));
        sensorList.add(list.get(0));
        for (int i = 1; i < num; i++) {
            builder.append(" , ").append(list.get(i));
            sensorList.add(list.get(i));
        }
        builder.append(" FROM ").append(getFullGroupDevicePathByID(devices.get(0)));
        for (int i = 1; i < devices.size(); i++) {
            builder.append(" , ").append(getFullGroupDevicePathByID(devices.get(i)));
        }

        return builder.toString();
    }

    /**
     * 创建查询语句--(带有聚合函数的查询)
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, String method, List<String> sensorList) {
        StringBuilder builder = new StringBuilder();

        builder.append("SELECT ");

        List<String> list = new ArrayList<String>();
        for (String sensor : config.SENSOR_CODES) {
            list.add(sensor);
        }
        Collections.shuffle(list, sensorRandom);
        if (method.length() > 2) {
            builder.append(method).append("(").append(list.get(0)).append(")");
            sensorList.add(list.get(0));
            for (int i = 1; i < num; i++) {
                builder.append(" , ").append(method).append("(").append(list.get(i)).append(")");
                sensorList.add(list.get(i));
            }
        } else {
            builder.append(list.get(0));
            sensorList.add(list.get(0));
            for (int i = 1; i < num; i++) {
                builder.append(" , ").append(list.get(i));
                sensorList.add(list.get(i));
            }
        }


        builder.append(" FROM ").append(getFullGroupDevicePathByID(devices.get(0)));
        for (int i = 1; i < devices.size(); i++) {
            builder.append(" , ").append(getFullGroupDevicePathByID(devices.get(i)));
        }
        return builder.toString();
    }

    /**
     * 创建查询语句--(带有聚合函数以及时间约束的查询)
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, String method, long startTime,
                                          long endTime, List<String> sensorList) {
        StringBuilder builder = new StringBuilder(createQuerySQLStatment(devices, num, method, sensorList));
        String strstartTime = sdf.format(new Date(startTime));
        String strendTime = sdf.format(new Date(endTime));
        builder.append(" WHERE time > ");
        builder.append(strstartTime).append(" AND time < ").append(strendTime);
        return builder.toString();
    }

    /**
     * 创建查询语句--(带有时间约束条件的查询)
     *
     * @throws SQLException
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime,
                                          List<String> sensorList) throws SQLException {

        String strstartTime = sdf.format(new Date(startTime));
        String strendTime = sdf.format(new Date(endTime));
        StringBuilder builder = new StringBuilder();
        builder.append(createQuerySQLStatment(devices, num, sensorList)).append(" WHERE time > ");
        builder.append(strstartTime).append(" AND time < ").append(strendTime);
        return builder.toString();
    }

    /**
     * 创建查询语句--(带有时间约束以及条件约束的查询)
     *
     * @throws SQLException
     */
    private String createQuerySQLStatment(List<Integer> devices, int num, long startTime, long endTime, Number value,
                                          List<String> sensorList) throws SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append(createQuerySQLStatment(devices, num, startTime, endTime, sensorList));

        for (int id : devices) {
            String prefix = getFullGroupDevicePathByID(id);
            for (int i = 0; i < sensorList.size(); i++) {
                builder.append(" AND ").append(prefix).append(".").append(sensorList.get(i)).append(" > ")
                        .append(value);

            }
        }

        return builder.toString();
    }

    /**
     * 创建查询语句--(带有时间约束以及条件约束的GroupBy查询)
     *
     * @throws SQLException
     */
    private String createQuerySQLStatment(List<Integer> devices, String method, int num, List<Long> startTime, List<Long> endTime, Number value,
                                          List<String> sensorList) throws SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append(createQuerySQLStatment(devices, num, method, sensorList));
        builder.append(" WHERE ");
        for (int id : devices) {
            String prefix = getFullGroupDevicePathByID(id);
            for (int i = 0; i < sensorList.size(); i++) {
                builder.append(prefix).append(".").append(sensorList.get(i)).append(" > ")
                        .append(value).append(" AND ");
            }
        }
        builder.delete(builder.lastIndexOf("AND"), builder.length());
        builder.append(" GROUP BY(").append(config.TIME_UNIT).append("ms, ").append(Constants.START_TIMESTAMP);
        for (int i = 0; i < startTime.size(); i++) {
            String strstartTime = sdf.format(new Date(startTime.get(i)));
            String strendTime = sdf.format(new Date(endTime.get(i)));
            builder.append(",[").append(strstartTime).append(",").append(strendTime).append("]");
        }
        builder.append(")");
        return builder.toString();
    }

    /**
     * 返回数据库中设备的时间跨度
     *
     * @throws SQLException
     */
    @Override
    public long getTotalTimeInterval() throws SQLException {
        long startTime = Constants.START_TIMESTAMP, endTime = Constants.START_TIMESTAMP;
        String sql = "select MAX_TIME( s_0 ) from " + Constants.ROOT_SERIES_NAME + ".group_0.d_0";
        Statement statement = connection.createStatement();
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();
        if (resultSet.next()) {
            endTime = resultSet.getLong(1);
        }
        statement.close();

        sql = "select MIN_TIME( s_0 ) from " + Constants.ROOT_SERIES_NAME + ".group_0.d_0";
        statement = connection.createStatement();
        statement.execute(sql);
        resultSet = statement.getResultSet();
        if (resultSet.next()) {
            startTime = resultSet.getLong(1);
        }
        statement.close();
        LOGGER.info("时间间隔：{}", endTime - startTime);
        return endTime - startTime;
    }

    private String getGroupDevicePath(String device) {
        String[] spl = device.split("_");
        int deviceIndex = Integer.parseInt(spl[1]);
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupIndex = deviceIndex / groupSize;
        return "group_" + groupIndex + "." + device;
    }

    private String getFullGroupDevicePathByID(int id) {
        int groupSize = config.DEVICE_NUMBER / config.GROUP_NUMBER;
        int groupIndex = id / groupSize;
        return Constants.ROOT_SERIES_NAME + ".group_" + groupIndex + "." + config.DEVICE_CODES.get(id);
    }

    private String getTypeByField(String name) {
        if (name.endsWith("_percent") || name.startsWith("usage_")) {
            return "DOUBLE";
        }
        return "INT64";
    }

    /**
     * 强制IoTDB数据库将数据写入磁盘
     *
     * @throws SQLException
     */
    @Override
    public void flush() {
        String sql = "flush";
        try {
            Statement statement = connection.createStatement();
            statement.execute(sql);
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void getUnitPointStorageSize() throws SQLException {
        File dataDir = new File(config.LOG_STOP_FLAG_PATH + "/data");
        if (dataDir.exists() && dataDir.isDirectory()) {

            long deltaSize = getDirTotalSize(config.LOG_STOP_FLAG_PATH + "/data/delta");
            long dataSize = getDirTotalSize(config.LOG_STOP_FLAG_PATH + "/data");
            //long dataSize = getDirTotalSize(config.LOG_STOP_FLAG_PATH + "/data") ;
            long overflowSize = getDirTotalSize(config.LOG_STOP_FLAG_PATH + "/data/overflow");
            //	float pointByteSize = (deltaSize + overflowSize) *
            //			1024.0f / (config.SENSOR_NUMBER * config.DEVICE_NUMBER * config.LOOP *
            //			config.CACHE_NUM);
            //	LOGGER.info("Average size of data point ,{},Byte ,ENCODING = ,{}, dir size: delta ,{},KB overflow ,{},KB "
            //			, pointByteSize, config.ENCODING, deltaSize, overflowSize);
            LOGGER.info("ENCODING = {} , dir size: data {} KB ;delta {} KB ;overflow {} KB "
                    , config.ENCODING, dataSize, deltaSize, overflowSize);
            mySql.saveResult("DataSize", String.valueOf(dataSize));
            mySql.saveResult("DeltaSize", String.valueOf(deltaSize));
            mySql.saveResult("OverflowSize", String.valueOf(overflowSize));

        } else {
            LOGGER.info("Can not find data directory!");
        }
    }

    @Override
    public long count(String group, String device, String sensor) {

        return 0;
    }

    @Override
    public void createSchemaOfDataGen() throws SQLException {
        setStorgeGroup(config.STORAGE_GROUP_NAME);
        createTimeseries(config.STORAGE_GROUP_NAME, config.TIMESERIES_NAME);
    }

    @Override
    public void insertGenDataOneBatch(String device, int loopIndex, ThreadLocal<Long> totalTime, ThreadLocal<Long> errorCount) throws SQLException {
        Statement statement;
        int[] result;
        long errorNum = 0;
        try {
            statement = connection.createStatement();
            for (int i = 0; i < config.CACHE_NUM; i++) {
                String sql = createGenDataSQLStatment(loopIndex, i, device);
                writeSQLIntoFile(sql, config.GEN_DATA_FILE_PATH);
                statement.addBatch(sql);
            }
            long startTime = System.nanoTime();
            try {
                result = statement.executeBatch();
            } catch (BatchUpdateException e) {
                long[] arr = e.getLargeUpdateCounts();
                for (long i : arr) {
                    if (i == -3) {
                        errorNum++;
                    }
                }
            }
            statement.clearBatch();
            statement.close();
            long endTime = System.nanoTime();
            long costTime = endTime - startTime;

            if (errorNum > 0) {
                LOGGER.info("Batch insert failed, the failed number is {}! ", errorNum);

            } else {
                LOGGER.info("{} execute {} loop, it costs {}s, totalTime {}s, throughput {} points/s",
                        Thread.currentThread().getName(), loopIndex, costTime / unitTransfer,
                        (totalTime.get() + costTime) / unitTransfer,
                        (config.CACHE_NUM * config.SENSOR_NUMBER / (double) costTime) * unitTransfer);
                totalTime.set(totalTime.get() + costTime);
            }
            errorCount.set(errorCount.get() + errorNum);

            mySql.saveInsertProcess(loopIndex, (endTime - startTime) / unitTransfer, totalTime.get() / unitTransfer, errorNum,
                    config.REMARK);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void exeSQLFromFileByOneBatch() throws SQLException {
        Statement statement;
        int count = 0;
        long startTime;
        long endTime;
        long costTime;
        long totalCostTime = 0;
        int errorNum = 0;
        File file = new File(config.SQL_FILE);
        if (file.exists()) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                try {
                    statement = connection.createStatement();
                    String sql = null;
                    int line = 0;
                    while ((sql = br.readLine()) != null) {
                        line++;
                        if (!sql.startsWith("#") && !sql.equals("")) {
                            count++;
                            int lines = 0;

                            startTime = System.nanoTime();
                            try {
                                boolean hasResult = statement.execute(sql);
                                if (hasResult) {
                                    ResultSet resultSet = statement.getResultSet();
                                    while (resultSet.next()) {
                                        lines++;
//				int sensorNum = sensorList.size();
//				builder.append(" \ntimestamp = ").append(resultSet.getString(0)).append("; ");
//				for (int i = 1; i <= sensorNum; i++) {
//					builder.append(resultSet.getString(i)).append("; ");
//				}
                                    }
                                }
                            } catch (SQLException e) {
                                errorNum++;
                                LOGGER.error("Line {} : Execute ' {} ' failed ! Because {}",
                                        line,
                                        sql,
                                        e
                                );
                            }
                            statement.close();
                            endTime = System.nanoTime();

                            costTime = endTime - startTime;
                            totalCostTime += costTime;
                            LOGGER.info("Execute one SQL from file, resultSet size is {}, it costs {} ms, current total time {} ms, the SQL : {}",
                                    lines,
                                    costTime / 1000000.0,
                                    totalCostTime / 1000000.0,
                                    sql
                            );
                        }
                    }


                    LOGGER.info("Execute {} SQL from file finished, it costs {} seconds, mean rate {} SQL/s . {} SQL execute failed.",
                            count,
                            totalCostTime / unitTransfer,
                            unitTransfer * (count - errorNum) / totalCostTime,
                            errorNum
                    );


                    //mySql.saveInsertProcess(loopIndex, (endTime - startTime) / 1000.0, totalTime.get() / 1000.0, errorNum,config.REMARK);

                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }


        } else {
            LOGGER.error("Execute SQL from file mode: SQL file not found.");
        }
    }


    private void writeSQLIntoFile(String sql, String gen_data_file_path) {
        BufferedWriter out = null;
        File dir = new File(gen_data_file_path);
        if (dir.exists() && dir.isDirectory()) {
            try {
                out = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(gen_data_file_path + Constants.SAMPLE_DATA_FILE_NAME, true)));
                out.write(sql);
                out.newLine();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 拿到某个路径的大小
     *
     * @param dir
     * @return
     */
    private static long getDirTotalSize(String dir) {
        long totalsize = 0;

        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            // 获得文件夹大小，单位 Byte
            String command = "du " + dir;
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            String lastLine = null;
            while (true) {
                lastLine = line;
                if ((line = in.readLine()) == null) {
                    System.out.println(lastLine);
                    break;
                }
            }
            String[] temp = lastLine.split("\\s+");
            totalsize = Long.parseLong(temp[0]);

            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }

        return totalsize;
    }

    /**
     * 使用QUERY_SEED参数作为随机数种子
     *
     * @param p 返回true的概率
     * @return 布尔值
     */
    private boolean returnTrueByProb(double p, Random random) {
        if (random.nextDouble() < p) {
            return true;
        } else {
            return false;
        }
    }

}
