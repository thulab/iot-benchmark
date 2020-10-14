package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationController {

  private static final Logger LOGGER = null;
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private Random random;

  OperationController(int seed) {
    random = new Random(seed);
  }

  /**
   * @return Operation the next operation for client to execute
   */
  Operation getNextOperationType() {
    List<Double> proportion = resolveOperationProportion();
    double[] p = new double[Operation.values().length + 1];
    p[0] = 0.0;
    // split [0,1] to n regions, each region corresponds to a operation type whose probability
    // is the region range size.
    for (int i = 1; i <= Operation.values().length; i++) {
      p[i] = p[i - 1] + proportion.get(i - 1);
    }
    double rand = random.nextDouble();
    int i;
    for (i = 1; i <= Operation.values().length; i++) {
      if (rand >= p[i - 1] && rand <= p[i]) {
        break;
      }
    }
    switch (i) {
      case 1:
        return Operation.INGESTION;
      case 2:
        return Operation.PRECISE_QUERY;
      case 3:
        return Operation.RANGE_QUERY;
      case 4:
        return Operation.VALUE_RANGE_QUERY;
      case 5:
        return Operation.AGG_RANGE_QUERY;
      case 6:
        return Operation.AGG_VALUE_QUERY;
      case 7:
        return Operation.AGG_RANGE_VALUE_QUERY;
      case 8:
        return Operation.GROUP_BY_QUERY;
      case 9:
        return Operation.LATEST_POINT_QUERY;
      default:
        LOGGER.error("Unsupported operation {}, use default operation: INGESTION.", i);
        return Operation.INGESTION;
    }
  }

  List<Double> resolveOperationProportion() {
    List<Double> proportion = new ArrayList<>();
    String[] split = config.getOPERATION_PROPORTION().split(":");
    if (split.length != Operation.values().length) {
      LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
    }
    double[] proportions = new double[Operation.values().length];
    double sum = 0;
    for (int i = 0; i < split.length; i++) {
      proportions[i] = Double.parseDouble(split[i]);
      sum += proportions[i];
    }
    for (int i = 0; i < split.length; i++) {
      if (sum != 0) {
        proportion.add(proportions[i] / sum);
      } else {
        proportion.add(0.0);
        LOGGER.error("The sum of operation proportions is zero!");
      }
    }
    return proportion;
  }

}
