package cn.edu.tsinghua.iotdb.benchmark.tsdb.kvdb;


import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.leveldb.LevelTimeSeriesDBFactory;
import edu.tsinghua.k1.rocksdb.RocksDBTimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;
import org.iq80.leveldb.Options;

public class KVDBHoler {

  private static TimeSeriesKVDB instance = null;
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static ReentrantLock lock = new ReentrantLock();

  public static TimeSeriesKVDB getInstance() throws IOException {
    if (instance != null) {
      return instance;
    } else {
      lock.lock();
      try {
        instance = createKVDB();
      } finally {
        lock.unlock();
      }
    }
    return instance;
  }

  private static TimeSeriesKVDB createKVDB() throws IOException {
    File file = new File(config.GEN_DATA_FILE_PATH);
    ITimeSeriesDB iTimeSeriesDB = null;
    switch (config.DB_SWITCH) {
      case Constants.DB_LEVELDB:
        Options optionsLevel = new Options();
        optionsLevel.createIfMissing(true);
        iTimeSeriesDB = LevelTimeSeriesDBFactory.getInstance().openOrCreate(file, optionsLevel);
        break;
      default:
        org.rocksdb.Options optionsRocks = new org.rocksdb.Options();
        optionsRocks.setCreateIfMissing(true);
        iTimeSeriesDB = RocksDBTimeSeriesDBFactory.getInstance().openOrCreate(file, optionsRocks);
        break;
    }
    return new TimeSeriesKVDB(iTimeSeriesDB);
  }
}
