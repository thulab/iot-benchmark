package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Batch {

  private DeviceSchema deviceSchema;
  private Map<Long, List<String>> records;

  public Batch() {
    records = new LinkedHashMap<>();
  }

  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }

  public void setDeviceSchema(DeviceSchema deviceSchema) {
    this.deviceSchema = deviceSchema;
  }

  public Map<Long, List<String>> getRecords() {
    return records;
  }

  public void add(long timestamp, List<String> values) {
    records.put(timestamp, values);
  }

  /**
   * use the row protocol which means data are organized in List[timestamp, List[value]]
   *
   * @return data point number in this batch
   */
  public int pointNum() {
    int pointNum = 0;
    for (List<String> list : records.values()) {
      pointNum += list.size();
    }
    return pointNum;
  }

}
