package cn.edu.tsinghua.iotdb.benchmark.workload.reader;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ReddReader extends BasicReader {

  private DeviceSchema deviceSchema;

  public ReddReader(Config config, List<String> files) {
    super(config, files);
  }

  @Override
  public void init() {
    currentDeviceId = new File(currentFile).getName();
    deviceSchema = new DeviceSchema(calGroupIdStr(currentDeviceId, config.GROUP_NUMBER),
        currentDeviceId, config.FIELDS);
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
      String[] items = line.split(" ");
      long time = Long.parseLong(items[0]) * 1000;
      fields.add(items[1]);
      return new Record(time, fields);
    } catch (Exception ignore) {
      ignore.printStackTrace();
    }
    return null;
  }
}
