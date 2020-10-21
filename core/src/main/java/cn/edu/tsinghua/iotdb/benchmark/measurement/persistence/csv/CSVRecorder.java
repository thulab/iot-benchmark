package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.csv;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.utils.CSVFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;

public class CSVRecorder implements ITestDataPersistence {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVRecorder.class);
    private String localName;
    private final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
    private static final long EXP_TIME = System.currentTimeMillis();
    private final Config config = ConfigDescriptor.getInstance().getConfig();
    private final String projectID = String.format("%s_%s_%s_%s",config.getBENCHMARK_WORK_MODE(), config.getDB_SWITCH(), config.getREMARK(), sdf.format(new java.util.Date(EXP_TIME)));
    String serverInfoCSV;
    String confCSV;
    String finalResultCSV;
    String projectCSV;
    private static final String FOUR = "NULL, %s, %s, %s\n";

    public CSVRecorder() {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            localName = localhost.getHostName();
        } catch (UnknownHostException e) {
            localName = "localName";
            LOGGER.error("获取本机主机名称失败;UnknownHostException：{}", e.getMessage(), e);
        }
        localName = localName.replace("-", "_");
        localName = localName.replace(".", "_");
        Date date = new Date(EXP_TIME);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
        String day = dateFormat.format(date);
        String confDir = System.getProperty(Constants.BENCHMARK_CONF);
        String dataDir = confDir.substring(0, confDir.length() - 23) + "/data/csv";
        confCSV = dataDir + "/CONF.csv";
        finalResultCSV = dataDir + "/FINAL_RESULT.csv";
        projectCSV = dataDir + "/" + projectID + ".csv";
        File file = new File(dataDir);
        if(file.exists() && file.isDirectory()) {
            if (!file.mkdir()) {
                LOGGER.error("can't create dir");
            }
        }
        serverInfoCSV = dataDir + "/SERVER_MODE_" + localName + "_" + day + ".csv";
        initCSVFile();
    }

    public void initCSVFile() {
        if(!CSVFileUtil.isCSVFileExist(serverInfoCSV)) {
            String firstLine = "id,cpu_usage,mem_usage,diskIo_usage,net_recv_rate,net_send_rate" +
                    ",pro_mem_size,dataFileSize,systemFizeSize,sequenceFileSize,unsequenceFileSize" +
                    ",walFileSize,tps,MB_read,MB_wrtn\n";
            File file = new File(serverInfoCSV);
            try {
                if (!file.createNewFile()) {
                    LOGGER.error("can't create file");
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
            CSVFileUtil.appendMethod(serverInfoCSV, firstLine);
        }
        if(!CSVFileUtil.isCSVFileExist(confCSV)) {
            String firstLine = "id,projectID,configuration_item,configuration_value\n";
            File file = new File(confCSV);
            try {
                if (!file.createNewFile()) {
                    LOGGER.error("can't create file");
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
            CSVFileUtil.appendMethod(confCSV, firstLine);
        }
        if(!CSVFileUtil.isCSVFileExist(finalResultCSV)) {
            String firstLine = "id,projectID,operation,result_key,result_value\n";
            File file = new File(finalResultCSV);
            try {
                if (!file.createNewFile()) {
                    LOGGER.error("can't create file");
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
            CSVFileUtil.appendMethod(finalResultCSV, firstLine);
        }
        if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH) && !CSVFileUtil.isCSVFileExist(
                projectCSV)) {
            String firstLine = "id,recordTime,clientName,operation,okPoint,failPoint,latency,rate,remark";
            File file = new File(projectCSV);
            try {
                if (!file.createNewFile()) {
                    LOGGER.error("can't create file");
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }
            CSVFileUtil.appendMethod(projectCSV, firstLine);
        }

    }

    @Override
    public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
        String system = System.currentTimeMillis() + "," +
                systemMetricsMap.get(SystemMetrics.CPU_USAGE) + "," +
                systemMetricsMap.get(SystemMetrics.MEM_USAGE) + "," +
                systemMetricsMap.get(SystemMetrics.DISK_IO_USAGE) + "," +
                systemMetricsMap.get(SystemMetrics.NETWORK_R_RATE) + "," +
                systemMetricsMap.get(SystemMetrics.NETWORK_S_RATE) + "," +
                systemMetricsMap.get(SystemMetrics.PROCESS_MEM_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.DATA_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.SYSTEM_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.SEQUENCE_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.UN_SEQUENCE_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.WAL_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.DISK_TPS) + "," +
                systemMetricsMap.get(SystemMetrics.DISK_READ_SPEED_MB) + "," +
                systemMetricsMap.get(SystemMetrics.DISK_WRITE_SPEED_MB) + "\n";
        CSVFileUtil.appendMethod(serverInfoCSV, system);
    }

    @Override
    public void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark) {
        double rate = 0;
        if (latency > 0) {
            rate = okPoint * 1000 / latency; //unit: points/second
        }
        String time = df.format(new java.util.Date(System.currentTimeMillis()));
        String line = String.format("NULL,%s,%s,%s,%d,%d,%f,%f,%s\n",
                time, Thread.currentThread().getName(), operation, okPoint, failPoint, latency, rate,
                remark);
        CSVFileUtil.appendMethod(projectCSV,line);
    }

    @Override
    public void saveResult(String operation, String k, String v) {
        String line = String.format("NULL,%s,%s,%s,%s", projectID, operation, k, v);
        CSVFileUtil.appendMethod(finalResultCSV, line);
    }

    @Override
    public void saveTestConfig() {
        StringBuilder str = new StringBuilder();
        if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
            str.append(String.format(FOUR, projectID,
                    "MODE", "DEFAULT_TEST_MODE"));
        }
        switch (config.getDB_SWITCH().trim()) {
            case Constants.DB_IOT:
            case Constants.DB_TIMESCALE:
                str.append(String.format(FOUR, projectID, "ServerIP", config.getHOST()));
                break;
            case Constants.DB_INFLUX:
            case Constants.DB_OPENTS:
            case Constants.DB_KAIROS:
            case Constants.DB_CTS:
                String host = config.getDB_URL()
                        .substring(config.getDB_URL().lastIndexOf('/') + 1, config.getDB_URL().lastIndexOf(':'));
                str.append(String.format(FOUR, projectID, "ServerIP", host));
                break;
            default:
                LOGGER.error("unsupported database " + config.getDB_SWITCH());
        }
        str.append(String.format(FOUR, projectID, "CLIENT", localName));
        str.append(String.format(FOUR, projectID, "DB_SWITCH", config.getDB_SWITCH()));
        str.append(String.format(FOUR, projectID, "VERSION", config.getVERSION()));
        str.append(String.format(FOUR, projectID, "getCLIENT_NUMBER()", config.getCLIENT_NUMBER()));
        str.append(String.format(FOUR, projectID, "LOOP", config.getLOOP()));
        if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
            str.append(String.format(FOUR, projectID, "查询数据集存储组数", config.getGROUP_NUMBER()));
            str.append(String.format(FOUR, projectID, "查询数据集设备数", config.getDEVICE_NUMBER()));
            str.append(String.format(FOUR, projectID, "查询数据集传感器数", config.getSENSOR_NUMBER()));
            if (config.getDB_SWITCH().equals(Constants.DB_IOT)) {
                str.append(String.format(FOUR, projectID, "IOTDB编码方式", config.getENCODING()));
            }
            str.append(String.format(FOUR, projectID, "QUERY_DEVICE_NUM", config.getQUERY_DEVICE_NUM()));
            str.append(String.format(FOUR, projectID, "QUERY_SENSOR_NUM", config.getQUERY_SENSOR_NUM()));
            str.append(String.format(FOUR, projectID, "IS_OVERFLOW", config.isIS_OVERFLOW()));
            if (config.isIS_OVERFLOW()) {
                str.append(String.format(FOUR, projectID, "OVERFLOW_RATIO",  config.getOVERFLOW_RATIO()));
            }
            str.append(String.format(FOUR, projectID, "MUL_DEV_BATCH", config.isMUL_DEV_BATCH()));
            str.append(String.format(FOUR, projectID, "DEVICE_NUMBER", config.getDEVICE_NUMBER()));
            str.append(String.format(FOUR, projectID, "GROUP_NUMBER", config.getGROUP_NUMBER()));
            str.append(String.format(FOUR, projectID, "DEVICE_NUMBER", config.getDEVICE_NUMBER()));
            str.append(String.format(FOUR, projectID, "SENSOR_NUMBER", config.getSENSOR_NUMBER()));
            str.append(String.format(FOUR, projectID, "BATCH_SIZE", config.getBATCH_SIZE()));
            str.append(String.format(FOUR, projectID, "POINT_STEP", config.getPOINT_STEP()));
            if (config.getDB_SWITCH().equals(Constants.DB_IOT)) {
                str.append(String.format(FOUR, projectID, "ENCODING", config.getENCODING()));
            }
        }
        CSVFileUtil.appendMethod(confCSV, str.toString());
    }

    @Override
    public void close() {

    }
}
