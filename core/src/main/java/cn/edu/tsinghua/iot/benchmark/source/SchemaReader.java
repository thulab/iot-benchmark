package cn.edu.tsinghua.iot.benchmark.source;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.conf.Constants;
import cn.edu.tsinghua.iot.benchmark.entity.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SchemaReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaReader.class);
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();

  /**
   * get device schema based on file name and data set sensorType
   *
   * @return device schema list to register
   */
  public abstract Map<String, List<Sensor>> getDeviceSchemaList();

  /** Check whether the on-disk dataset's info.txt matches the current config. */
  public boolean checkDataSet() {
    Path path = Paths.get(config.getFILE_PATH(), Constants.INFO_PATH);
    if (!Files.exists(path) || !Files.isRegularFile(path)) {
      return false;
    }
    try {
      List<String> configs = Files.readAllLines(path);
      List<String> nowConfigs = new ArrayList<>(Arrays.asList(config.toInfoText().split("\n")));
      Map<String, String> differs = new HashMap<>();
      for (int i = 0; i < nowConfigs.size(); i++) {
        // A shorter info.txt (e.g. from an older benchmark version with fewer config lines) must be
        // reported as a difference, not crash with IndexOutOfBounds.
        String configValue = i < configs.size() ? configs.get(i) : "";
        String nowConfigValue = nowConfigs.get(i);
        if (!nowConfigValue.equals(configValue)) {
          differs.put(configValue, nowConfigValue);
        }
      }
      for (Map.Entry<String, String> differ : differs.entrySet()) {
        LOGGER.error(
            "The config in dataSet is "
                + differ.getKey()
                + " but now config is "
                + differ.getValue());
      }
      if (differs.size() != 0) {
        return false;
      }
    } catch (IOException exception) {
      LOGGER.error("Failed to check config");
    }
    return true;
  }
}
