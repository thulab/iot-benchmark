package cn.edu.tsinghua.iotdb.benchmark.influxdb2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class InfluxDBModel implements Serializable {
  private static final long serialVersionUID = 1L;
  private String metric;
  private long timestamp;

  private Map<String, String> tags = new HashMap<String, String>();
  private Map<String, Object> fields = new HashMap<>();

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

  public Map<String, String> getTags() {
    return tags;
  }

  public Map<String, Object> getFields() {
    return fields;
  }

  public void addTag(String tag, String value){
    tags.put(tag, value);
  }

  public void addField(String sensor, Object value){
    fields.put(sensor, value);
  }


  @Override
  public String toString() {
    StringBuffer result = new StringBuffer(metric);
    if (tags != null) {
      for (Map.Entry<String, String> pair: tags.entrySet()) {
        result.append(",");
        result.append(pair.getKey());
        result.append("=");
        result.append(pair.getValue());
      }
    }
    result.append(" ");
    if(fields != null){
      boolean first = true;
      for(Map.Entry<String, Object> pair: fields.entrySet()){
        if(first){
          first = false;
        }else{
          result.append(",");
        }
        result.append(pair.getKey());
        result.append("=");
        result.append(pair.getValue());
      }
    }
    result.append(" ");
    result.append(timestamp);
    return result.toString();
  }
}
