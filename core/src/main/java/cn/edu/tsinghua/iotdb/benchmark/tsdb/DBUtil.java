
package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(DBUtil.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static double[] probabilities = null;

  public static String getDataType(int sensorIndex) {
    if (probabilities == null) {
      resolveDataTypeProportion();
    }
    double sensorPosition = sensorIndex * 1.0 / config.getSENSOR_NUMBER();
    int i;
    for (i = 1; i <= 6; i++) {
      if (sensorPosition >= probabilities[i - 1] && sensorPosition < probabilities[i]) {
        break;
      }
    }
    switch (i) {
      case 1:
        return "BOOLEAN";
      case 2:
        return "INT32";
      case 3:
        return "INT64";
      case 4:
        return "FLOAT";
      case 5:
        return "DOUBLE";
      case 6:
        return "TEXT";
      default:
        LOGGER.error("Unsupported data type {}, use default data type: TEXT.", i);
        return "TEXT";
    }
  }

  private synchronized  static void resolveDataTypeProportion() {
    if(probabilities != null) {
      //someone has executed this method.
      return;
    }
    //the following implementation is not graceful, but it is okey as it only is run once.
    List<Double> proportion = new ArrayList<>();
    String[] split = config.getINSERT_DATATYPE_PROPORTION().split(":");
    if (split.length != 6) {
      LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
    }
    double[] proportions = new double[6];
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
        LOGGER.error("The sum of INSERT_DATATYPE_PROPORTION is zero!");
      }
    }

    probabilities = new double[6 + 1];
    probabilities[0] = 0.0;
    // split [0,1] to n regions, each region corresponds to a data type whose proportion
    // is the region range size.
    for (int i = 1; i <= 6; i++) {
      probabilities[i] = probabilities[i - 1] + proportion.get(i - 1);
    }
  }

  // parse different sensor index as different data type value
  public static Object parseNumber(int index, String value) throws TsdbException {
    switch (DBUtil.getDataType(index)) {
      case "FLOAT":
        return convertToFloat(value);
      case "DOUBLE":
        return convertToDouble(value);
      case "INT32":
        return convertToInt(value);
      case "INT64":
        return convertToLong(value);
      case "BOOLEAN":
        return convertToBoolean(value);
      case "TEXT":
        return convertToText(value);
      default:
        throw new TsdbException("unsuport datatype " + DBUtil.getDataType(index));
    }
  }

  public static List<String> recordTransform(List<String> valueList){
    List<String> ret = new ArrayList<>();
    try {
      for (int i = 0; i < valueList.size(); i++) {
        Object value = DBUtil.parseNumber(i, valueList.get(i));
        ret.add(value.toString());
      }
    } catch (Exception e) {
      LOGGER.error("transform record value failed");
    }
    return ret;
  }

  public static List<String> recordTransform(List<String> valueList,int colIndex){
    List<String> ret = new ArrayList<>();
    try {
      Object value = DBUtil.parseNumber(colIndex, valueList.get(0));
      ret.add(value.toString());
    } catch (Exception e) {
      LOGGER.error("transform record value failed");
    }
    return ret;
  }


  //currently, the workload engine just generate double, so we need a uniform way to transform it to other data type.
  public static boolean convertToBoolean(String doubleString) {
    return Double.parseDouble(doubleString) > 500;
  }
  public static int convertToInt(String doubleString) {
    return (int) Double.parseDouble(doubleString);
  }
  public static long convertToLong(String doubleString) {
    return (long) Double.parseDouble(doubleString);
  }
  public static float convertToFloat(String doubleString) {
    return (float) Double.parseDouble(doubleString);
  }
  public static double convertToDouble(String doubleString) {
    return  Double.parseDouble(doubleString);
  }

  public static String convertToText(String doubleString) {
    return  doubleString;
  }

}

