package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSize {
    private static Logger log = LoggerFactory.getLogger(FileSize.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private static final String LINUX_FILE_SIZE_CMD = "du -sm %s";
    private static final double MB2GB = 1024.0;
    private static final double ABNORMALVALUE = -1;
    public enum FileSizeKinds {
        DATA(config.IOTDB_DATA_DIR),
        STSTEM(config.IOTDB_SYSTEM_DIR),
        WAL(config.IOTDB_WAL_DIR),
        SEQUENCE(config.SEQUENCE_DIR),
        OVERFLOW(config.UNSEQUENCE_DIR);

        List<String> path;

        FileSizeKinds(List<String> path){
            this.path = path;
        }
    }

    private static class FileSizeHolder {
        private static final FileSize INSTANCE = new FileSize();
    }

    private FileSize(){
        switch (config.DB_SWITCH){
            case Constants.DB_IOT:
            case Constants.BENCHMARK_IOTDB:
                break;
            default:
                log.error("unsupported db name: {}", config.DB_SWITCH);
        }
    }

    public static final FileSize getInstance(){
        return FileSizeHolder.INSTANCE;
    }

    public Map<FileSizeKinds, Double> getFileSize() {
        Map<FileSizeKinds, Double> fileSize = new EnumMap<>(FileSizeKinds.class);
        BufferedReader in;
        Process pro = null;
        Runtime runtime = Runtime.getRuntime();
        for (FileSizeKinds kinds : FileSizeKinds.values()) {
            double fileSizeGB = ABNORMALVALUE;
            for (String path_ : kinds.path) {
                String command = String.format(LINUX_FILE_SIZE_CMD, path_);
                try {
                    pro = runtime.exec(command);
                    in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                    String line;
                    while ((line = in.readLine()) != null) {
                        String size = line.split("\\s+")[0];
                        if (fileSizeGB == ABNORMALVALUE) {
                            fileSizeGB = 0;
                        }
                        fileSizeGB += Long.parseLong(size) / MB2GB;
                    }
                    in.close();
                } catch (IOException e) {
                    log.info("Execute command failed: {}", command);
                }
            }
            fileSize.put(kinds, fileSizeGB);
        }
        if (pro != null) {
            pro.destroy();
        }
        return fileSize;
    }

}
