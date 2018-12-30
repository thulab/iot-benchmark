package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

import java.io.*;
import java.util.EnumMap;
import java.util.Map;

public class FileSize {
    private static Logger log = LoggerFactory.getLogger(FileSize.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private static final String LINUX_FILE_SIZE_CMD = "du -sm %s";
    private static final double MB2GB = 1024.0;
    private static final double ABNORMAL_VALUE = -1;
    public enum FileSizeKinds {
        DATA("iotdb/data/data"),
        INFO("iotdb/data/system/info"),
        METADATA("iotdb/data/system/schema"),
        OVERFLOW("iotdb/data/data/overflow"),
        DELTA("iotdb/data/data/settled"),
        WAL("iotdb/data/wal");

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        private String path;

        FileSizeKinds(String path){
            this.path = path;
        }
    }

    private static class FileSizeHolder {
        private static final FileSize INSTANCE = new FileSize();
    }

    private FileSize(){
        switch (config.DB_SWITCH){
            case Constants.DB_IOT:
                break;
            case Constants.DB_INFLUX:
                FileSizeKinds.DATA.setPath("/data/" + config.DB_NAME);
                FileSizeKinds.DELTA.setPath("/data/" + config.DB_NAME + "/autogen");
                FileSizeKinds.WAL.setPath("/wal/" + config.DB_NAME);
                FileSizeKinds.METADATA.setPath("/meta");
                FileSizeKinds.OVERFLOW.setPath("/overflow");
                FileSizeKinds.INFO.setPath("/info");
                break;
            case Constants.DB_CTS:
            case Constants.DB_KAIROS:
            case Constants.DB_OPENTS:
            case Constants.DB_TIMESCALE:
            case Constants.BENCHMARK_IOTDB:
                break;
            default:
                log.error("unsupported db name: {}", config.DB_SWITCH);
        }
    }

    public static FileSize getInstance(){
        return FileSizeHolder.INSTANCE;
    }

    public Map<FileSizeKinds, Double> getFileSize() {
        Map<FileSize.FileSizeKinds, String> fileSizePathMap = OpenFileNumber.getFileSizePath();
        System.out.println(fileSizePathMap);
        EnumMap<FileSizeKinds, Double> fileSize = new EnumMap<> (FileSizeKinds.class);
        BufferedReader in ;
        Process pro = null;
        Runtime runtime = Runtime.getRuntime();
        for(FileSizeKinds kinds : FileSizeKinds.values()){
            String command = String.format(LINUX_FILE_SIZE_CMD, fileSizePathMap.get(kinds));
            double fileSizeGB = ABNORMAL_VALUE;
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
                log.info("Execute command failed: {}", command);
            }
            fileSize.put(kinds, fileSizeGB);
        }
        if (pro != null) {
            pro.destroy();
        }
        return fileSize;
    }

}
