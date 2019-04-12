package cn.edu.tsinghua.iotdb.benchmark.workload.reader;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TDriveReader extends BasicReader {

  private static Logger logger = LoggerFactory.getLogger(TDriveReader.class);
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

  private DeviceSchema deviceSchema;
  private List<String> sensors = new ArrayList<>();

  public TDriveReader(Config config, List<String> files) {
    super(config, files);
    sensors.add("longitude");
    sensors.add("latitude");
  }

  @Override
  public void init() {
    currentDeviceId = new File(currentFile).getName();
    deviceSchema = new DeviceSchema(group, currentDeviceId, sensors);
  }

  @Override
  public Batch nextBatch() {
    List<Record> records = new ArrayList<>();
    for (String line : cachedLines) {
      Record record = convertToRecord(line);
      if (record != null) {
        records.add(record);
      }
    }
    return new Batch(deviceSchema, records);
  }


  private Record convertToRecord(String line) {

    try {
      List<String> fields = new ArrayList<>();

      String[] items = line.split(",");

      fields.add(items[2]);
      fields.add(items[3]);

      Date date = dateFormat.parse(items[1]);
      long time = date.getTime();

      return new Record(time, fields);
    } catch (Exception ignore) {
      ignore.printStackTrace();
    }
    return null;
  }

}
