package cn.edu.tsinghua.iotdb.benchmark.source;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import com.opencsv.CSVReaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class CopyDataReader extends DataReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyDataReader.class);
    private static final MetaDataSchema metaDataSchema = MetaDataSchema.getInstance();
    private Iterator<String[]> iterator = null;
    private long loopTimes = config.getLOOP();
    private Batch copyBatch = null;
    private long deltaTimeStamp = 0;

    public CopyDataReader(List<String> files) {
        // 在初始化的时候全部把数据全部读出来存储到workloads上，不用每次都去读取一遍数据
        super(files);
        currentFileName = files.get(0);
        String separator = File.separator;
        if (separator.equals("\\")) {
            separator = "\\\\";
        }
        String[] url = currentFileName.split(separator);
        String deviceName = url[url.length - 2];
        DeviceSchema deviceSchema = null;
        List<Sensor> sensors = null;
        List<Record> records = new ArrayList<>();
        long begin = 0;
        long curtimestamp = 0;
        try {
            com.opencsv.CSVReader csvReader =
                    new CSVReaderBuilder(
                            new BufferedReader(
                                    new InputStreamReader(
                                            new FileInputStream(new File(currentFileName)),
                                            StandardCharsets.UTF_8)))
                            .build();
            iterator = csvReader.iterator();
            boolean firstLine = true;
            boolean secondLine = true;
            // TODO 自定义是否BATCH_SIZE
            while (iterator.hasNext() && records.size() < config.getBATCH_SIZE_PER_WRITE()) {
                if (firstLine) {
                    String[] items = iterator.next();
                    // TODO Optimize
                    DeviceSchema originMetaSchema = metaDataSchema.getDeviceSchemaByName(deviceName);
                    Map<String, Sensor> stringSensorMap = new HashMap<>();
                    for (Sensor sensor : originMetaSchema.getSensors()) {
                        stringSensorMap.put(sensor.getName(), sensor);
                    }
                    sensors = new ArrayList<>();
                    for (int i = 1; i < items.length; i++) {
                        sensors.add(stringSensorMap.get(items[i]));
                    }
                    deviceSchema =
                            new DeviceSchema(MetaUtil.getGroupIdFromDeviceName(deviceName), deviceName, sensors);
                    firstLine = false;
                    continue;
                }
                String[] values = iterator.next();
                if (values[0].equals("Sensor")) {
                    LOGGER.warn("There is some thing wrong when read file.");
                    System.exit(1);
                }
                curtimestamp = Long.parseLong(values[0]);
                if(secondLine){
                    begin = curtimestamp;
                    secondLine = false;
                }
                List<Object> recordValues = new ArrayList<>();
                for (int i = 1; i < values.length; i++) {
                    switch (sensors.get(i - 1).getSensorType()) {
                        case BOOLEAN:
                            recordValues.add(Boolean.parseBoolean(values[i]));
                            break;
                        case INT32:
                            recordValues.add(Integer.parseInt(values[i]));
                            break;
                        case INT64:
                            recordValues.add(Long.parseLong(values[i]));
                            break;
                        case FLOAT:
                            recordValues.add(Float.parseFloat(values[i]));
                            break;
                        case DOUBLE:
                            recordValues.add(Double.parseDouble(values[i]));
                            break;
                        case TEXT:
                            recordValues.add(values[i]);
                            break;
                        default:
                            LOGGER.error("Error Type");
                    }
                }
                Record record = new Record(curtimestamp, recordValues);
                records.add(record);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            LOGGER.error("Failed to read file:" + exception.getMessage());
        }
        copyBatch = new Batch(deviceSchema, records);
        deltaTimeStamp = curtimestamp - begin;
    }

    @Override
    public boolean hasNextBatch() {
        return loopTimes != 0;
    }

    @Override
    public Batch nextBatch() {
        loopTimes -= 1;
        // TODO add white noise to differ the Batch
        // We need to change the timestamp
        for(Record record : copyBatch.getRecords()){
            record.setTimestamp(record.getTimestamp() + deltaTimeStamp);
        }
        return copyBatch;
    }
}
