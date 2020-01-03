package cn.edu.tsinghua.iotdb.benchmark.tsdb.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSession extends IoTDB {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSession.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private Session session;

  public IoTDBSession() {
    super();
    session = new Session(config.HOST, config.PORT, Constants.USER, Constants.PASSWD);
    try {
      session.open();
    } catch (IoTDBSessionException e) {
      LOGGER.error("Failed to add session", e);
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    Schema schema = new Schema();
    for (String sensor : batch.getDeviceSchema().getSensors()) {
      schema.registerMeasurement(
          new MeasurementSchema(sensor, Enum.valueOf(TSDataType.class, config.DATA_TYPE),
              Enum.valueOf(TSEncoding.class, config.ENCODING)));
    }
    RowBatch rowBatch = schema.createRowBatch(
        Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." + batch
            .getDeviceSchema().getDevice(), batch.getRecords().size());
    long[] timestamps = rowBatch.timestamps;
    Object[] values = rowBatch.values;

    for (int recordIndex = 0; recordIndex < batch.getRecords().size(); recordIndex++) {
      rowBatch.batchSize++;
      Record record = batch.getRecords().get(recordIndex);
      long currentTime = record.getTimestamp();
      timestamps[recordIndex] = currentTime;
      for (int recordValueIndex = 0; recordValueIndex < record.getRecordDataValue().size(); recordValueIndex++) {
        if(!config.DATA_TYPE.equals("TEXT")) {
          double[] sensors = (double[]) values[recordValueIndex];
          sensors[recordIndex] = Double.parseDouble(record.getRecordDataValue().get(
              recordValueIndex));
        } else {
          Binary[] sensors = (Binary[]) values[recordValueIndex];
          sensors[recordIndex] = Binary.valueOf(record.getRecordDataValue().get(recordValueIndex));
        }
      }
    }
    try {
      session.insertBatch(rowBatch);
      rowBatch.reset();
      return new Status(true);
    } catch (IoTDBSessionException e) {
      return new Status(false, 0, e, e.toString());
    }
  }

}
