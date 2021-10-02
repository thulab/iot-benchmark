package cn.edu.tsinghua.iotdb.benchmark.extern;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
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

public class CSVWriter extends BasicWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(CSVWriter.class);

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
        for (String sensor : deviceSchema.getSensors()) {
          SensorType sensorType = metaDataSchema.getSensorType(deviceSchema.getDevice(), sensor);
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

  @Override
  public boolean writeBatch(Batch batch, long insertLoopIndex) {
    String device = batch.getDeviceSchema().getDevice();
    try {
      Path dirFile = Paths.get(FileUtils.union(config.getFILE_PATH(), device));
      if (!Files.exists(dirFile)) {
        Files.createDirectories(dirFile);
      }
      Path dataFile =
          Paths.get(
              FileUtils.union(
                  config.getFILE_PATH(),
                  device,
                  "BigBatch_" + (insertLoopIndex / config.getBIG_BATCH_SIZE()) + ".csv"));
      if (!Files.exists(dataFile)) {
        Files.createFile(dataFile);
      }
      List<String> sensors = batch.getDeviceSchema().getSensors();
      String sensorLine = String.join(",", sensors);
      sensorLine = "Sensor," + sensorLine + "\n";
      Files.write(dataFile, sensorLine.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
      for (Record record : batch.getRecords()) {
        StringBuffer line = new StringBuffer(String.valueOf(record.getTimestamp()));
        for (String sensor : sensors) {
          Object value = null;
          if (batch.getColIndex() != -1) {
            value = record.getRecordDataValue().get(0);
          } else {
            int index = Integer.valueOf(sensor.split("_")[1]);
            value = record.getRecordDataValue().get(index);
          }
          if (value instanceof String) {
            value = "\"" + value + "\"";
          }
          line.append(",").append(value);
        }
        line.append("\n");
        Files.write(
            dataFile, line.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
      }
    } catch (IOException ioException) {
      LOGGER.error("Write batch Error!" + batch);
      return false;
    }
    return true;
  }
}
