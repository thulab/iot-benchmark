package cn.edu.tsinghua.iot.benchmark.iotdb140;

import java.util.List;

public class IoTDBRestPayload {
  public String device;
  public boolean is_aligned;
  public List<List<Object>> values;
  public List<String> data_types;
  public List<String> measurements;
  public List<Long> timestamps;
}
