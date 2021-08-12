package cn.edu.tsinghua.iotdb.benchmark.mode;

import cn.edu.tsinghua.iotdb.benchmark.client.Client;
import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.client.QueryRealDatasetClient;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class QueryWithRealDataSetMode extends BaseMode{
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryWithRealDataSetMode.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    /**
     * Start benchmark
     */
    @Override
    public void run() {
        LOGGER.info("use dataset: {}", config.getDATA_SET());
        // check whether the parameters are legitimate
        if (!checkParamForQueryRealDataSet(config)) {
            return;
        }

        Measurement measurement = new Measurement();
        CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());

        // create getCLIENT_NUMBER() client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            Client client = new QueryRealDatasetClient(i, downLatch, barrier, config);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    /**
     * Check validation of real data set
     *
     * @param config
     * @return
     */
    private static boolean checkParamForQueryRealDataSet(Config config) {
        if (config.getQUERY_SENSOR_NUM() > config.getFIELDS().size()) {
            LOGGER.error(
                    "QUERY_SENSOR_NUM={} can't greater than size of field, {}.",
                    config.getQUERY_SENSOR_NUM(),
                    config.getFIELDS());
            return false;
        }
        String[] split = config.getOPERATION_PROPORTION().split(":");
        if (split.length != Operation.values().length) {
            LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
            return false;
        }
        if (!split[0].trim().equals("0")) {
            LOGGER.error(
                    "OPERATION_PROPORTION {} error, {} can't have write operation.",
                    config.getOPERATION_PROPORTION(),
                    config.getBENCHMARK_WORK_MODE());
            return false;
        }
        return true;
    }
}
