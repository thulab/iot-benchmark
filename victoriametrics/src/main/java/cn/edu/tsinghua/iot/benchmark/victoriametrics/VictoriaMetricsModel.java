package cn.edu.tsinghua.iot.benchmark.victoriametrics;

import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class VictoriaMetricsModel implements Serializable {
  private static final long serialVersionUID = 1L;
  private String metric;
  private long timestamp;
  private Object value;
  private SensorType sensorType;
  private Map<String, String> tags = new HashMap<String, String>();

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public SensorType getType() {
    return sensorType;
  }

  public void setType(SensorType sensorType) {
    this.sensorType = sensorType;
  }

  @Override
  public String toString() {
    StringBuffer result = new StringBuffer(metric);
    result.append("{");
    if (tags != null) {
      boolean first = true;
      for (Map.Entry<String, String> pair : tags.entrySet()) {
        if (!first) {
          result.append(",");
        } else {
          first = false;
        }
        result.append(pair.getKey());
        result.append("=\"");
        result.append(pair.getValue());
        result.append("\"");
      }
    }
    result.append("} ");
    if (sensorType == SensorType.BOOLEAN) {
      result.append((boolean) value ? 1 : 0);
    } else {
      result.append(value);
    }
    result.append(" ");
    result.append(timestamp);
    return result.toString();
  }
}
