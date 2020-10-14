package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import cn.edu.tsinghua.iotdb.benchmark.utils.ReadWriteIOUtils;

public class Record {

  private long timestamp;
  private List<String> recordDataValue;

  public Record(long timestamp, List<String> recordDataValue) {
    this.timestamp = timestamp;
    this.recordDataValue = recordDataValue;
  }

  /**
   * deserialize from input stream
   *
   * @param inputStream input stream
   */
  public static Record deserialize(ByteArrayInputStream inputStream) throws IOException {
    long timestamp = ReadWriteIOUtils.readLong(inputStream);
    return new Record(timestamp, ReadWriteIOUtils.readStringList(inputStream));
  }

  @Override
  public String toString() {
    return "Record{" +
        "timestamp=" + timestamp +
        ", recordDataValue=" + recordDataValue +
        '}';
  }

  public int size() {
    return recordDataValue.size();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public List<String> getRecordDataValue() {
    return recordDataValue;
  }

  /**
   * serialize to output stream
   *
   * @param outputStream output stream
   */
  public void serialize(ByteArrayOutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(timestamp, outputStream);
    ReadWriteIOUtils.write(recordDataValue.size(), outputStream);
    for (String value : recordDataValue) {
      ReadWriteIOUtils.write(value, outputStream);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Record)) {
      return false;
    }

    Record record = (Record) o;

    return new EqualsBuilder()
        .append(timestamp, record.timestamp)
        .append(recordDataValue, record.recordDataValue)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(timestamp)
        .append(recordDataValue)
        .toHashCode();
  }
}
