package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceFactory.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();

    public ITestDataPersistence getPersistence() {

        switch (config.TEST_DATA_PERSISTENCE) {
            case Constants.TDP_NONE:
                return new IoTDB();
            case Constants.TDP_IoTDB:
                return new InfluxDB();
            case Constants.TDP_MySQL:
                return new KairosDB();
            default:
                LOGGER.error("unsupported test data persistence way: {}", config.TEST_DATA_PERSISTENCE);
                return new IoTDB();
        }
    }
}
