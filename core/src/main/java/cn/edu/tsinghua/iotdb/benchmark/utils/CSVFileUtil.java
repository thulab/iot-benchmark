package cn.edu.tsinghua.iotdb.benchmark.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CSVFileUtil {

    public static void appendMethod(String fileName, String content) {
        try {
            //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
            FileWriter writer = new FileWriter(fileName, true);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isCSVFileExist(String filename) {
        File csvFile = new File(filename);
        return csvFile.exists() && !csvFile.isDirectory();
    }
}
