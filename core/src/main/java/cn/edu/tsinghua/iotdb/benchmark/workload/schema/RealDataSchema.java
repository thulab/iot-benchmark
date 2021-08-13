package cn.edu.tsinghua.iotdb.benchmark.workload.schema;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.utils.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author stormbroken
 * Create by 2021/08/12
 * @Version 1.0
 **/

public class RealDataSchema extends BaseDataSchema{

    private static final Logger LOGGER = LoggerFactory.getLogger(RealDataSchema.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    /**
     * Create Data Schema for each device
     */
    @Override
    protected void createDataSchema() {
        File dirFile = new File(config.getFILE_PATH());
        if (!dirFile.exists()) {
            LOGGER.error("{} does not exit", config.getFILE_PATH());
            return;
        }

        LOGGER.info("use dataset: {}", config.getDATA_SET());

        List<String> files = new ArrayList<>();
        getAllFiles(config.getFILE_PATH(), files);
        LOGGER.info("total files: {}", files.size());
        Collections.sort(files);

        List<DeviceSchema> deviceSchemaList = BasicReader.getDeviceSchemaList(files, config);

        // Split into Thread And store Type
        for(int i = 0; i < deviceSchemaList.size(); i++){
            int threadId = i % config.getCLIENT_NUMBER();
            DeviceSchema deviceSchema = deviceSchemaList.get(i);
            if(!CLIENT_BIND_SCHEMA.containsKey(threadId)){
                CLIENT_BIND_SCHEMA.put(threadId, new ArrayList<>());
            }
            CLIENT_BIND_SCHEMA.get(threadId).add(deviceSchema);
            for(String sensor: deviceSchema.getSensors()){
                // TODO fix to multi type
                addSensorType(deviceSchema.getDevice(), sensor, Type.DOUBLE);
            }
        }

        // Split Files into Thread
        List<List<String>> threadFiles = new ArrayList<>();
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            threadFiles.add(new ArrayList<>());
        }

        for (int i = 0; i < files.size(); i++) {
            String filePath = files.get(i);
            int thread = i % config.getCLIENT_NUMBER();
            threadFiles.get(thread).add(filePath);
        }
        MetaUtil.setThreadFiles(threadFiles);
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
}
