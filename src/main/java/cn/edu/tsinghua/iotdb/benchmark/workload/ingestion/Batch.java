package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Batch {

  private DeviceSchema deviceSchema;
  private List<DataPoint> dataPointsList;
  private Map<Long, List<String>> records;

  public Batch() {
    dataPointsList = new ArrayList<>();
    records = new HashMap<>();
  }

  public Batch(DeviceSchema deviceSchema, List<DataPoint> list, Map<Long, List<String>> records) {
    this.deviceSchema = deviceSchema;
    this.dataPointsList = list;
    this.records = records;
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

  public void setRecords(Map<Long, List<String>> records) {
    this.records = records;
  }

  public List<DataPoint> getDataPointsList() {
    return dataPointsList;
  }

  public void setDataPointsList(
      List<DataPoint> dataPointsList) {
    this.dataPointsList = dataPointsList;
  }

  public void add(DataPoint dataPoint) {
    dataPointsList.add(dataPoint);
  }

  public void add(long timestamp, List<String> values) {
    records.put(timestamp, values);
  }

  /**
   * use the row protocol which means data are organized in List[timestamp, List[value]]
   * @return data point number in this batch
   */
  public int pointNum() {
    int pointNum = 0;
    for (List<String> list : records.values()) {
      pointNum += list.size();
    }
    return pointNum;
  }

  @Override
  public String toString() {
    String result = "";
    long count = 0;
    for (DataPoint dataPoint : dataPointsList) {
      result = result.concat(dataPoint.toString());
      result = result.concat("\n");
      count++;
    }
    result = result.concat("data point count: " + count);
    return result;
  }
}
