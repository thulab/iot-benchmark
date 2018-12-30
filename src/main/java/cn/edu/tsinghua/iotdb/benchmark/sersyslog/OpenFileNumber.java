package cn.edu.tsinghua.iotdb.benchmark.sersyslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Map;

public class OpenFileNumber {

    private static Logger log = LoggerFactory.getLogger(OpenFileNumber.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private static String[] cmds = {"/bin/bash", "-c", ""};
    private static final String SEARCH_PID = "ps -aux | grep -i %s | grep -v grep";
    private static final String SEARCH_OPEN_DATA_FILE_BY_PID = "lsof -p %d";
    private static int pid = getPID(config.DB_SWITCH);
    private static Map<FileSize.FileSizeKinds, String> fileSizePathMap = new EnumMap<> (FileSize.FileSizeKinds.class);
    private static int fileSizePathCount = 0;

    private static class OpenFileNumberHolder {
        private static final OpenFileNumber INSTANCE = new OpenFileNumber();
    }

    private OpenFileNumber() {
        //initialize fileSizePathMap
        for (FileSize.FileSizeKinds openFileNumStatistics : FileSize.FileSizeKinds.values()) {
            fileSizePathMap.put(openFileNumStatistics, "none");
        }
    }

    public static final OpenFileNumber getInstance() {
        return OpenFileNumberHolder.INSTANCE;
    }

    /**
     * 获得当前指定的数据库服务器的PID
     * @param dbName TSDB name
     * @return int pid
     */
    public static int getPID(String dbName) {
        int pid = -1;
        Process pro1;
        Runtime r = Runtime.getRuntime();
        String filter = "";
        switch (dbName) {
            case Constants.DB_IOT:
                filter = "IOTDB_HOME";
                break;
            case Constants.DB_INFLUX:
                filter = "/usr/bin/influxd";
                break;
            case Constants.DB_KAIROS:
                filter = "kairosdb";
                break;
            case Constants.DB_TIMESCALE:
                filter = "postgresql";
                break;
            case Constants.BENCHMARK_IOTDB:
                filter = "../conf/config.properties";
                break;
        }
        try {
            String command = String.format(SEARCH_PID, filter);
            cmds[2] = command;
            pro1 = r.exec(cmds);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                String[] temp = line.split("\\s+");
                if (temp.length > 1 && isNumeric(temp[1])) {
                    pid = Integer.parseInt(temp[1]);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
        } catch (IOException e) {
            log.error("统计打开文件数时getPid()发生InstantiationException. {}", e.getMessage());
        }
        return pid;
    }

    /**
     * 返回打开的文件数目的列表，
     * 其中:
     * list[0]表示该进程一共打开的文件数目，
     * 1ist[1]表示该进程打开的数据文件和写前日志的数目
     * 1ist[2]表示该进程打开的socket的数目
     * <p>
     * list[3]表示该进程打开delta文件的数目
     * 1ist[4]表示该进程打开derby文件的数目
     * 1ist[5]表示该进程打开digest文件的数目
     * 1ist[6]表示该进程打开metadata文件的数目
     * 1ist[7]表示该进程打开overflow文件的数目
     * 1ist[8]表示该进程打开wals文件的数目
     */
    private static ArrayList<Integer> getOpenFile(int pid) throws SQLException {
        ArrayList<Integer> list = new ArrayList<>();
        int dataFileNum = 0;
        int totalFileNum = 0;
        int socketNum = 0;

        int deltaNum = 0;
        int derbyNum = 0;
        int digestNum = 0;
        int metadataNum = 0;
        int overflowNum = 0;
        int walsNum = 0;

        String filter = "";
        switch (config.DB_SWITCH) {
            case Constants.DB_IOT:
                filter = "/data/";
                break;
            case Constants.DB_INFLUX:
                filter = ".influxdb";
                break;
            case Constants.DB_KAIROS:
                filter = "kairosdb";
                break;
            case Constants.DB_TIMESCALE:
                filter = "postgresql";
                break;
            case Constants.BENCHMARK_IOTDB:
                filter = "iotdb-benchmark";
                break;
            default:
                throw new SQLException("unsupported db name :" + config.DB_SWITCH);
        }
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = String.format(SEARCH_OPEN_DATA_FILE_BY_PID, pid);
            cmds[2] = command;
            pro = r.exec(cmds);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;

            while ((line = in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (line.contains("" + pid) && temp.length > 8) {
                    totalFileNum++;
                    if (temp[8].contains(filter)) {
                        dataFileNum++;
                        if (temp[8].contains("settled")) {
                            deltaNum++;
                        } else if (temp[8].contains("info")) {
                            derbyNum++;
                        } else if (temp[8].contains("schema")) {
                            digestNum++;
                        } else if (temp[8].contains("metadata")) {
                            metadataNum++;
                        } else if (temp[8].contains("overflow")) {
                            overflowNum++;
                        } else if (temp[8].contains("wal")) {
                            walsNum++;
                        }
                    }
                    if (temp[7].contains("TCP") || temp[7].contains("UDP")) {
                        socketNum++;
                    }
                }
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            log.error("统计打开文件数时getOpenFile()发生InstantiationException. {}", e.getMessage());
        }
        list.add(totalFileNum);
        list.add(dataFileNum);
        list.add(socketNum);
        list.add(deltaNum);
        list.add(derbyNum);
        list.add(digestNum);
        list.add(metadataNum);
        list.add(overflowNum);
        list.add(walsNum);
        return list;
    }

    static Map<FileSize.FileSizeKinds, String> getFileSizePath() {
        if(fileSizePathCount < FileSize.FileSizeKinds.values().length) {
            Process pro;
            Runtime r = Runtime.getRuntime();
            try {
                String command = String.format(SEARCH_OPEN_DATA_FILE_BY_PID, pid);
                cmds[2] = command;
                pro = r.exec(cmds);
                BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                String line;

                while ((line = in.readLine()) != null) {
                    String[] temp = line.split("\\s+");
                    boolean flag = false;
                    if (line.contains("" + pid) && temp.length > 8) {
                        for (FileSize.FileSizeKinds openFileNumStatistics : FileSize.FileSizeKinds.values()) {
                            if (fileSizePathMap.get(openFileNumStatistics).equals("none")) {
                                String path = openFileNumStatistics.getPath();
                                if (temp[8].contains(path)) {
                                    String rootPath = temp[8].substring(0, temp[8].indexOf(path));
                                    for(FileSize.FileSizeKinds statistics : FileSize.FileSizeKinds.values()){
                                        fileSizePathMap.put(statistics, rootPath + statistics.getPath());
                                        log.info(statistics.toString());
                                        log.info(rootPath);
                                        log.info(rootPath + statistics.getPath());
                                        fileSizePathCount++;
                                        log.info(fileSizePathCount + "");
                                    }
                                    flag = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (flag){
                        break;
                    }
                }
                in.close();
                pro.destroy();
            } catch (IOException e) {
                log.error("Cannot get file size path of IoTDB process because of {}", e.getMessage());
            }
        }
        System.out.println(fileSizePathMap);
        return fileSizePathMap;
    }

    /**
     * 返回打开的文件数目的列表，其中：
     * list[0]表示该进程一共打开的文件数目
     * 1ist[1]表示该进程打开的数据文件和写前日志的数目
     * 1ist[2]表示该进程打开的socket的数目
     * <p>
     * list[3]表示该进程打开delta文件的数目，
     * 1ist[4]表示该进程打开derby文件的数目
     * 1ist[5]表示该进程打开digest文件的数目
     * 1ist[6]表示该进程打开metadata文件的数目
     * 1ist[7]表示该进程打开overflow文件的数目
     * 1ist[8]表示该进程打开wals文件的数目
     */
    public ArrayList<Integer> get() {
        //System.out.println("port :"+port + " ; pid :"+pid);

        ArrayList<Integer> list = null;
        //如果port和pid不合理，再次尝试获取
        if (!(pid > 0)) {
            pid = getPID(config.DB_SWITCH);
        }
        //如果pid合理，则加入打开文件总数和数据文件数目以及socket数目
        if (pid > 0) {
            try {
                list = getOpenFile(pid);
            } catch (SQLException e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        } else {
            //pid 不合理，赋不合法的值
            if (list == null)
                list = new ArrayList<Integer>();
            for (int i = 0; i < 9; i++) {
                list.add(-1);
            }
        }
        return list;
    }

    //检验一个字符串是否是整数
    public static boolean isNumeric(String str) {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public int getPid() {
        return getPID(config.DB_SWITCH);
    }


}
