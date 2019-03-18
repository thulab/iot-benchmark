package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import java.util.ArrayList;
import java.util.List;

public class Batch {

  private List<DataPoint> dataPointsList;

  public Batch() {
    dataPointsList = new ArrayList<>();
  }

  Batch(List<DataPoint> list) {
    this.dataPointsList = list;
  }

  public List<DataPoint> getDataPointsList() {
    return dataPointsList;
  }

  public void setDataPointsList(
      List<DataPoint> dataPointsList) {
    this.dataPointsList = dataPointsList;
  }

  public void add(DataPoint dataPoint) {
    dataPointsList.add(dataPoint);
  }

  @Override
  public String toString() {
    String result = "";
    long count = 0;
    for (DataPoint dataPoint : dataPointsList) {
      result = result.concat(dataPoint.toString());
      result = result.concat("\n");
      count++;
    }
    result = result.concat("data point count: " + count);
    return result;
  }
}
