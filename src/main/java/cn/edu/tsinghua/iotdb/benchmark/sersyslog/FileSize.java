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
    private String cmds[] = {"/bin/bash", "-c", ""};
    private final double MB2GB = 1024.0;
    private final double ABNORMALVALUE = -2;
    public enum FileSizeKinds {
        DATA(config.LOG_STOP_FLAG_PATH + "/data"),
        DIGEST(config.LOG_STOP_FLAG_PATH + "/data/digest"),
        METADATA(config.LOG_STOP_FLAG_PATH + "/data/metadata"),
        OVERFLOW(config.LOG_STOP_FLAG_PATH + "/data/overflow"),
        DELTA(config.LOG_STOP_FLAG_PATH + "/data/delta");

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
        BufferedReader in = null;
        Process pro = null;
        Runtime runtime = Runtime.getRuntime();
        double fileSizeGB = ABNORMALVALUE;;
        for(FileSizeKinds kinds : FileSizeKinds.values()){

            try {
                String command = String.format(LINUX_FILE_SIZE_CMD, kinds.path);
                pro = runtime.exec(command);
                in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                String line = null;
                while((line=in.readLine()) != null) {
                    System.out.println("line="+line);
                    String size = line.split("\\s+")[0];
                    fileSizeGB = Long.parseLong(size) / MB2GB;
                }

            } catch (IOException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
            }



            /*
            String command = String.format(LINUX_FILE_SIZE_CMD, kinds.path);
            cmds[2] = command;
            try {
                pro = runtime.exec(cmds);
            } catch (IOException e) {
                log.info("Execute command failed :" + command);
                e.printStackTrace();
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            try {
                line = br.readLine();
                br.close();
                System.out.println("line==="+line);
            } catch (IOException e) {
                log.info("Read command input stream failed :" + command);
                e.printStackTrace();
            }
            if(line != null && line.equals("")) {
                System.out.println("line="+line);
                String size = line.split("\\s+")[0];
                fileSizeGB = Long.parseLong(size) / MB2GB;
            } else{
                fileSizeGB = ABNORMALVALUE;
            }
            */
            fileSize.put(kinds,fileSizeGB);

        }
        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        pro.destroy();

        return fileSize;
    }

}
