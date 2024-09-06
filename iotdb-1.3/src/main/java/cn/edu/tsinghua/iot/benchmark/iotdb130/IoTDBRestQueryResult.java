package cn.edu.tsinghua.iot.benchmark.iotdb130;

import java.util.List;

public class IoTDBRestQueryResult {
  public List<String> expressions;
  public List<String> column_names;
  public List<Long> timestamps;
  public List<List<Object>> values;
}
