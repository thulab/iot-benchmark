package cn.edu.tsinghua.iotdb.benchmark.extern;

import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.Record;
import cn.edu.tsinghua.iotdb.benchmark.entity.Sensor;
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

public class CSVDataWriter extends DataWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVDataWriter.class);

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
      List<Sensor> sensors = batch.getDeviceSchema().getSensors();
      StringBuffer sensorLine = new StringBuffer("Sensor");
      for (Sensor sensor : sensors) {
        sensorLine.append(",").append(sensor.getName());
      }
      sensorLine.append("\n");
      Files.write(
          dataFile,
          sensorLine.toString().getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.APPEND);
      for (Record record : batch.getRecords()) {
        StringBuffer line = new StringBuffer(String.valueOf(record.getTimestamp()));
        for (int i = 0; i < sensors.size(); i++) {
          Object value = null;
          value = record.getRecordDataValue().get(i);
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
