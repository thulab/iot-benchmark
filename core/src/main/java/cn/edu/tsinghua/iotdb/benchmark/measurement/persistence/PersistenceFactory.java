package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.iotdb.IotdbRecorder;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.mysql.MySqlRecorder;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.none.NoneRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersistenceFactory.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    public ITestDataPersistence getPersistence() {
        switch (config.getTEST_DATA_PERSISTENCE()) {
            case Constants.TDP_NONE:
                return new NoneRecorder();
            case Constants.TDP_IOTDB:
                return new IotdbRecorder();
            case Constants.TDP_MYSQL:
                return new MySqlRecorder();
            default:
                LOGGER.error("unsupported test data persistence way: {}, use NoneRecorder", config.getTEST_DATA_PERSISTENCE());
                return new NoneRecorder();
        }
    }
}
