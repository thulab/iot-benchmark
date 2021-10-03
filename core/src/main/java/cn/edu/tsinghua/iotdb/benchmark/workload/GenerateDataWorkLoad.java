package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.distribution.ProbTool;
import cn.edu.tsinghua.iotdb.benchmark.entity.Batch;
import cn.edu.tsinghua.iotdb.benchmark.entity.enums.SensorType;
import cn.edu.tsinghua.iotdb.benchmark.exception.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.function.Function;
import cn.edu.tsinghua.iotdb.benchmark.function.FunctionParam;
import cn.edu.tsinghua.iotdb.benchmark.schema.MetaUtil;
import cn.edu.tsinghua.iotdb.benchmark.schema.schemaImpl.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public abstract class GenerateDataWorkLoad extends DataWorkLoad {

  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateDataWorkLoad.class);
  protected static final ProbTool probTool = new ProbTool();
  protected static final Random poissonRandom = new Random(config.getDATA_SEED());
  protected static final Random dataRandom = new Random(config.getDATA_SEED());
  protected static final Random timestampRandom = new Random(config.getDATA_SEED());
  protected static final String CHAR_TABLE =
      "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  protected static final long timeStampConst = getTimestampConst(config.getTIMESTAMP_PRECISION());
  /**
   * workloadValues[SENSOR_NUMBER][WORKLOAD_BUFFER_SIZE]。 For those regular data, a piece of data of
   * each sensor is stored for rapid generation according to the law this must after timeStampConst
   */
  protected static final Object[][] workloadValues = initWorkloadValues();

  protected AtomicLong insertLoop = new AtomicLong(0);
  protected List<DeviceSchema> deviceSchemas = new ArrayList<>();
  protected int deviceSchemaSize = 0;

  public static GenerateDataWorkLoad getInstance(int clientId) {
    List<DeviceSchema> deviceSchemas = metaDataSchema.getDeviceSchemaByClientId(clientId);
    if (config.isIS_CLIENT_BIND()) {
      return new SyntheticDataWorkLoad(deviceSchemas);
    } else {
      return SingletonWorkDataWorkLoad.getInstance();
    }
  }

  @Override
  public Batch getOneBatch() throws WorkloadException {
    if (!config.isIS_OUT_OF_ORDER()) {
      return getOrderedBatch();
    } else {
      switch (config.getOUT_OF_ORDER_MODE()) {
        case 0:
          return getDistOutOfOrderBatch();
        case 1:
          return getLocalOutOfOrderBatch();
        default:
          throw new WorkloadException(
              "Unsupported out of order mode: " + config.getOUT_OF_ORDER_MODE());
      }
    }
  }

  @Override
  public long getBatchNumber() {
    return config.getDEVICE_NUMBER() * config.getLOOP();
  }

  protected abstract Batch getOrderedBatch();

  protected abstract Batch getDistOutOfOrderBatch();

  protected abstract Batch getLocalOutOfOrderBatch();

  /**
   * Add one row into batch, row contains data from all sensors
   *
   * @param batch
   * @param stepOffset
   */
  protected void addOneRowIntoBatch(Batch batch, long stepOffset) {
    List<Object> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    for (int i = 0; i < config.getSENSOR_NUMBER(); i++) {
      values.add(
          workloadValues[i][(int) (Math.abs(stepOffset) % config.getWORKLOAD_BUFFER_SIZE())]);
    }
    batch.add(currentTimestamp, values);
  }

  /**
   * Add one row into batch, row contains data from one sensor which index is colIndex
   *
   * @param batch
   * @param stepOffset
   * @param colIndex
   */
  protected void addOneRowIntoBatch(Batch batch, long stepOffset, int colIndex) {
    List<Object> values = new ArrayList<>();
    long currentTimestamp = getCurrentTimestamp(stepOffset);
    values.add(
        workloadValues[colIndex][(int) (Math.abs(stepOffset) % config.getWORKLOAD_BUFFER_SIZE())]);
    batch.add(currentTimestamp, values);
  }

  /**
   * Get timestamp according to stepOffset
   *
   * @param stepOffset
   * @return
   */
  protected static long getCurrentTimestamp(long stepOffset) {
    // offset of data ahead
    long offset = config.getPOINT_STEP() * stepOffset;
    // timestamp for next data
    long timestamp = 0;
    // change timestamp frequency
    if (config.isIS_REGULAR_FREQUENCY()) {
      // data is in regular frequency, then do nothing
      timestamp += config.getPOINT_STEP();
    } else {
      // data is not in regular frequency, then use random
      timestamp += config.getPOINT_STEP() * timestampRandom.nextDouble();
    }
    return (Constants.START_TIMESTAMP + offset + timestamp) * timeStampConst;
  }

  /**
   * Init workload values
   *
   * @return
   */
  private static Object[][] initWorkloadValues() {
    Object[][] workloadValues = null;
    if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
      // if the first number in OPERATION_PROPORTION not equals to 0, then write data
      workloadValues = new Object[config.getSENSOR_NUMBER()][config.getWORKLOAD_BUFFER_SIZE()];
      for (int j = 0; j < config.getSENSOR_NUMBER(); j++) {
        String sensor = config.getSENSOR_CODES().get(j);
        SensorType sensorType =
            metaDataSchema.getSensorType(
                MetaUtil.getDeviceName(config.getFIRST_DEVICE_INDEX()), sensor);
        for (int i = 0; i < config.getWORKLOAD_BUFFER_SIZE(); i++) {
          // This time stamp is only used to generate periodic data. So the timestamp is also
          // periodic
          long currentTimestamp = getCurrentTimestamp(i);
          Object value;
          if (sensorType == SensorType.TEXT) {
            // TEXT case: pick STRING_LENGTH chars to be a String for insertion.
            StringBuffer builder = new StringBuffer(config.getSTRING_LENGTH());
            for (int k = 0; k < config.getSTRING_LENGTH(); k++) {
              builder.append(CHAR_TABLE.charAt(dataRandom.nextInt(CHAR_TABLE.length())));
            }
            value = builder.toString();
          } else {
            // not TEXT case
            FunctionParam param = config.getSENSOR_FUNCTION().get(sensor);
            Number number = Function.getValueByFunctionIdAndParam(param, currentTimestamp);
            switch (sensorType) {
              case BOOLEAN:
                value = number.floatValue() > ((param.getMax() + param.getMin()) / 2);
                break;
              case INT32:
                value = number.intValue();
                break;
              case INT64:
                value = number.longValue();
                break;
              case FLOAT:
                value = (float) (Math.round(number.floatValue()));
                break;
              case DOUBLE:
                value = (double) Math.round(number.doubleValue());
                break;
              default:
                value = null;
                break;
            }
          }
          workloadValues[j][i] = value;
        }
      }
    } else {
      LOGGER.info("According to OPERATION_PROPORTION, there is no need to write");
    }
    return workloadValues;
  }

  private static long getTimestampConst(String timePrecision) {
    if (timePrecision.equals("ms")) {
      return 1L;
    } else if (timePrecision.equals("us")) {
      return 1000L;
    } else {
      return 1000000L;
    }
  }
}
