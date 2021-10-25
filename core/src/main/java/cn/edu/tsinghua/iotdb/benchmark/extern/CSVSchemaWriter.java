package cn.edu.tsinghua.iotdb.benchmark.extern;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import cn.edu.tsinghua.iotdb.benchmark.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class CSVSchemaWriter extends SchemaWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CSVSchemaWriter.class);

  /**
   * Write Schema to line
   *
   * @param deviceSchemaList
   * @return
   */
  @Override
  public boolean writeSchema(List<DeviceSchema> deviceSchemaList) {
    try {
      // process target
      Path path = Paths.get(config.getFILE_PATH());
      Files.deleteIfExists(path);
      Files.createDirectories(path);

      LOGGER.info("Finish record schema.");

      Path schemaPath = Paths.get(FileUtils.union(config.getFILE_PATH(), Constants.SCHEMA_PATH));
      Files.createFile(schemaPath);
      for (DeviceSchema deviceSchema : deviceSchemaList) {
        for (Sensor sensor : deviceSchema.getSensors()) {
          SensorType sensorType = sensor.getSensorType();
          String line = deviceSchema.getDevice() + " " + sensor + " " + sensorType.ordinal() + "\n";
          Files.write(schemaPath, line.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        }
      }

      Path infoPath = Paths.get(FileUtils.union(config.getFILE_PATH(), Constants.INFO_PATH));
      Files.createFile(infoPath);
      Files.write(
          infoPath, config.toInfoText().getBytes(StandardCharsets.UTF_8), StandardOpenOption.WRITE);
      return true;
    } catch (IOException ioException) {
      ioException.printStackTrace();
      LOGGER.error(
          "Failed to generate Schema. Please check whether "
              + config.getFILE_PATH()
              + " is empty.");
      return false;
    }
  }
}
