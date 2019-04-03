package cn.edu.tsinghua.iotdb.benchmark.workload.reader;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasicReader {

  private static Logger logger = LoggerFactory.getLogger(BasicReader.class);
  protected Config config;
  private List<String> files;
  private BufferedReader reader;
  protected List<String> cachedLines;
  protected static String group = "default";
  private boolean hasInit = false;

  private int currentFileIndex = 0;
  protected String currentFile;
  protected String currentDeviceId;

  public BasicReader(Config config, List<String> files) {
    this.config = config;
    this.files = files;
    cachedLines = new ArrayList<>();
  }

  public boolean hasNextBatch() {

    if (files == null || files.isEmpty()) {
      return false;
    }

    if (!hasInit) {
      try {
        reader = new BufferedReader(new FileReader(files.get(currentFileIndex)));
        currentFile = files.get(currentFileIndex);
        logger.info("start to read {}-th file {}", currentFileIndex, currentFile);
        init();
        hasInit = true;
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("meet exception when init file: {}", currentFile);
      }
    }

    cachedLines.clear();

    try {
      String line;
      while (true) {

        if (reader == null) {
          return false;
        }

        line = reader.readLine();

        // current file end
        if (line == null) {

          // current file has been resolved, read next file
          if (cachedLines.isEmpty()) {
            if (currentFileIndex < files.size() - 1) {
              currentFile = files.get(currentFileIndex++);
              logger.info("start to read {}-th file {}", currentFileIndex, currentFile);
              reader.close();
              reader = new BufferedReader(new FileReader(currentFile));
              init();
              continue;
            } else {
              // no more file to read
              reader.close();
              reader = null;
              break;
            }
          } else {
            // resolve current file
            return true;
          }
        } else if (line.isEmpty()) {
          continue;
        }

        // read a line, cache it
        cachedLines.add(line);
        if (cachedLines.size() == config.BATCH_SIZE) {
          break;
        }
      }
    } catch (Exception ignore) {
      logger.error("read file {} failed", currentFile);
      ignore.printStackTrace();
      return false;
    }

    return !cachedLines.isEmpty();
  }


  /**
   * convert the cachedLines to Record list
   */
  abstract public Batch nextBatch();


  /**
   * initialize when start reading a file maybe skip the first lines maybe init the
   * tagValue(deviceId) from file name
   */
  public abstract void init() throws Exception;


  /**
   * get device schema from file name and data set type
   * @param files absolute file paths to read
   * @return device schema list to register
   */
  public static List<DeviceSchema> getDeviceSchemaList(List<String> files, Config config) {
    List<DeviceSchema> deviceSchemaList = new ArrayList<>();
    for(String currentFile: files) {
      String[] items = currentFile.split("/");
      String deviceId = items[items.length - 1];
      deviceSchemaList.add(new DeviceSchema(group, deviceId, config.FIELDS));
    }
    return deviceSchemaList;
  }

}
