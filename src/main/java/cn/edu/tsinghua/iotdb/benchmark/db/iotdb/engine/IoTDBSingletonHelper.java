package cn.edu.tsinghua.iotdb.benchmark.db.iotdb.engine;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.io.File;
import java.io.IOException;
import org.apache.iotdb.db.api.ITSEngine;
import org.apache.iotdb.db.api.IoTDBEngineException;
import org.apache.iotdb.db.api.IoTDBOptions;
import org.apache.iotdb.db.api.impl.IoTDBEngine;


public class IoTDBSingletonHelper {

  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static IoTDBSingletonHelper helper = null;
  private static boolean isClosed;
  private ITSEngine engine = null;

  private IoTDBSingletonHelper(ITSEngine engine) {
    this.engine = engine;
  }

  public ITSEngine getEngine() {
    return engine;
  }

  public synchronized static IoTDBSingletonHelper getInstance() {
    // 如果help等于null
    // 构造一个新的database
    // 可以负责多线程并发open
    if (helper == null) {
      isClosed = false;
      File file = new File(config.GEN_DATA_FILE_PATH);
      IoTDBOptions options = new IoTDBOptions();
      ITSEngine engine = new IoTDBEngine(file, options);
      try {
        engine.openOrCreate();
      } catch (IoTDBEngineException e) {
        e.printStackTrace();
      }
      helper = new IoTDBSingletonHelper(engine);
    }
    return helper;
  }

  public synchronized static void closeInstance() {
    // 如果db没有被关闭
    // 关闭db
    // 赋值为null
    // 可以负责多线程并发close
    if (isClosed == false) {
      try {
        helper.getEngine().close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      isClosed = true;
      helper = null;
    }
  }

}
