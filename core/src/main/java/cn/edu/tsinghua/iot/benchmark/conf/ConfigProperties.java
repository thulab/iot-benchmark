package cn.edu.tsinghua.iot.benchmark.conf;

import java.util.LinkedHashMap;
import java.util.Map;

public class ConfigProperties {
  private final Map<String, Map<String, Object>> properties = new LinkedHashMap<>();
  private final Map<String, Object> allProperties = new LinkedHashMap<>();

  public void addProperty(String groupName, String name, Object value) {
    if (!properties.containsKey(groupName)) {
      properties.put(groupName, new LinkedHashMap<>());
    }
    properties.get(groupName).put(name, value);
    allProperties.put(name, value);
  }

  public Map<String, Object> getAllProperties() {
    return allProperties;
  }

  @Override
  public String toString() {
    StringBuffer configPropertiesStr = new StringBuffer();
    for (Map.Entry<String, Map<String, Object>> group : properties.entrySet()) {
      configPropertiesStr
          .append("########### ")
          .append(group.getKey())
          .append(" ###########")
          .append(System.lineSeparator());
      for (Map.Entry<String, Object> property : group.getValue().entrySet()) {
        configPropertiesStr
            .append(property.getKey())
            .append("=")
            .append(property.getValue())
            .append(System.lineSeparator());
      }
    }
    return configPropertiesStr.toString();
  }
}
