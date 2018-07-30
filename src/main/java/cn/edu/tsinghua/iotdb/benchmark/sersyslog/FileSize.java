package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;

public class FileSize {
    private static Logger log = LoggerFactory.getLogger(FileSize.class);
    private static Config config;
    private final String LINUX_FILE_SIZE_CMD = "du -sm %s";
    private final double MB2GB = 1024.0;
    private final double ABNORMALVALUE = -1;
    public enum FileSizeKinds {
        DATA(config.LOG_STOP_FLAG_PATH + "/data/data"),
        INFO(config.LOG_STOP_FLAG_PATH + "/data/system/info"),
        METADATA(config.LOG_STOP_FLAG_PATH + "/data/system/schema"),
        OVERFLOW(config.LOG_STOP_FLAG_PATH + "/data/data/overflow"),
        DELTA(config.LOG_STOP_FLAG_PATH + "/data/data/settled"),
        WAL(config.LOG_STOP_FLAG_PATH + "/data/wal");

        public String path;

        FileSizeKinds(String path){
            this.path = path;
        }
    };

    private static class FileSizeHolder {
        private static final FileSize INSTANCE = new FileSize();
    }

    private FileSize(){
        config = ConfigDescriptor.getInstance().getConfig();

        switch (config.DB_SWITCH){
            case Constants.DB_IOT:

                break;
            case Constants.DB_INFLUX:
                FileSizeKinds.DATA.path = config.LOG_STOP_FLAG_PATH + "/data/" + config.INFLUX_DB_NAME;
                FileSizeKinds.DELTA.path = config.LOG_STOP_FLAG_PATH + "/data/" + config.INFLUX_DB_NAME + "/autogen";
                break;
            case Constants.BENCHMARK_IOTDB:
                break;
            default:
                log.error("unsupported db name :" + config.DB_SWITCH);
        }
    }

    public static final FileSize getInstance(){
        return FileSizeHolder.INSTANCE;
    }

    public HashMap<FileSizeKinds, Double> getFileSize() {
        HashMap<FileSizeKinds, Double> fileSize = new HashMap<>();
        BufferedReader in ;
        Process pro = null;
        Runtime runtime = Runtime.getRuntime();
        for(FileSizeKinds kinds : FileSizeKinds.values()){
            String command = String.format(LINUX_FILE_SIZE_CMD, kinds.path);
            double fileSizeGB = ABNORMALVALUE;
            try {
                pro = runtime.exec(command);
                in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                String line ;
                while((line=in.readLine()) != null) {
                    String size = line.split("\\s+")[0];
                    fileSizeGB = Long.parseLong(size) / MB2GB;
                }
                in.close();
            } catch (IOException e) {
                log.info("Execute command failed: " + command);
            }
            fileSize.put(kinds,fileSizeGB);
        }
        if (pro != null) {
            pro.destroy();
        }
        return fileSize;
    }

}
