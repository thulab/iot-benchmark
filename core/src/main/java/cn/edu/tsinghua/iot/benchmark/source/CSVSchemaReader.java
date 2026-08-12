package cn.edu.tsinghua.iot.benchmark.source;

import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import cn.edu.tsinghua.iot.benchmark.entity.enums.SensorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CSVSchemaReader extends SchemaReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVSchemaReader.class);

  /**
   * get device schema based on file name and data set sensorType
   *
   * @return device schema list to register
   */
  @Override
  public Map<String, List<Sensor>> getDeviceSchemaList() {
    Path path = Paths.get(config.getFILE_PATH(), Constants.SCHEMA_PATH);
    if (!Files.exists(path) || !Files.isRegularFile(path)) {
      LOGGER.error("Failed to find schema file in " + path.getFileName().toString());
      System.exit(0);
    }
    Map<String, List<Sensor>> result = new LinkedHashMap<>();
    try {
      List<String> schemaLines = Files.readAllLines(path);
      for (String schemaLine : schemaLines) {
        if (schemaLine.trim().length() != 0) {
          String[] line = schemaLine.split(" ");
          String deviceName = line[0];
          if (!result.containsKey(deviceName)) {
            result.put(deviceName, new ArrayList<>());
          }
          Sensor sensor = new Sensor(line[1], SensorType.getType(Integer.parseInt(line[2])));
          result.get(deviceName).add(sensor);
        }
      }
    } catch (IOException exception) {
      LOGGER.error("Failed to init register");
    }
    return result;
  }
}
