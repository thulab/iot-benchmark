package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import cn.edu.tsinghua.iotdb.benchmark.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Batch {

  private DeviceSchema deviceSchema;
  private DeviceSchema deviceSchema_sensor;
  private List<Record> records;
  private int colIndex = -1;
  private String colType ;

  public Batch() {
    records = new LinkedList<>();
  }

  public Batch(DeviceSchema deviceSchema, List<Record> records) {
    this.deviceSchema = deviceSchema;
    this.records = records;
  }
  
  public Batch(DeviceSchema deviceSchema_sensor, List<Record> records,String colType) {
	this.deviceSchema = null;
	this.deviceSchema_sensor = deviceSchema_sensor;
	this.records = records;
	this.colType = colType;
  }

  public DeviceSchema getDeviceSchema() {
    return deviceSchema;
  }
  
  public DeviceSchema getDeviceSchema_sensor() {
	return deviceSchema_sensor;
  }

  public void setDeviceSchema(DeviceSchema deviceSchema) {
    this.deviceSchema = deviceSchema;
  }
  
  public void setDeviceSchema_sensor(DeviceSchema deviceSchema_sensor) {
	this.deviceSchema_sensor = deviceSchema_sensor;
  }
  
  public void setColIndex(int colIndex) {
	this.colIndex = colIndex;
  } 
  
  public void setColType(String colType) {
	this.colType = colType;
  }  

  public int getColIndex(){
	return colIndex;
  }
  
  public String getColType(){
	return colType;
  }
  
  public List<Record> getRecords() {
    return records;
  }

  public void add(long timestamp, List<Object> values) {
    records.add(new Record(timestamp, values));
  }

  /**
   * use the row protocol which means data are organized in List[timestamp, List[value]]
   *
   * @return data point number in this batch
   */
  public int pointNum() {
    int pointNum = 0;
    for (Record record : records) {
      pointNum += record.size();
    }
    return pointNum;
  }

  /**
   * serialize to output stream
   * @param outputStream output stream
   */
  public void serialize(ByteArrayOutputStream outputStream) throws IOException {
    deviceSchema.serialize(outputStream);
    ReadWriteIOUtils.write(records.size(), outputStream);
    for(Record record : records){
      record.serialize(outputStream);
    }
  }

  /**
   * deserialize from input stream
   *
   * @param inputStream input stream
   */
  public static Batch deserialize(ByteArrayInputStream inputStream) throws IOException {
    DeviceSchema deviceSchema = DeviceSchema.deserialize(inputStream);
    int size = ReadWriteIOUtils.readInt(inputStream);
    List<Record> records = new LinkedList<>();
    for (int i = 0; i < size; i++) {
      records.add(Record.deserialize(inputStream));
    }

    return new Batch(deviceSchema, records);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Batch)) {
      return false;
    }

    Batch batch = (Batch) o;

    return new EqualsBuilder()
        .append(deviceSchema, batch.deviceSchema)
        .append(records, batch.records)
        .isEquals();
  }

  @Override
  public String toString() {
	if(deviceSchema != null)
		return "Batch{" +
        "deviceSchema=" + deviceSchema +
        ", records=" + records +
        '}';
	else
		return "Batch{" +
        "deviceSchema_sensor=" + deviceSchema_sensor +
        ", records=" + records +
        '}';

  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(deviceSchema)
        .append(records)
        .toHashCode();
  }
}

