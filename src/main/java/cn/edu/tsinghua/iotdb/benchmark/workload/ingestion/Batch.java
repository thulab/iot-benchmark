package cn.edu.tsinghua.iotdb.benchmark.workload.ingestion;

import java.util.ArrayList;
import java.util.List;

public class Batch {

  private List<TimeValuePair> timeValuePairsList;

  Batch(){
    timeValuePairsList = new ArrayList<>();
  }

  Batch(List<TimeValuePair> list){
    this.timeValuePairsList = list;
  }

  public List<TimeValuePair> getTimeValuePairsList() {
    return timeValuePairsList;
  }

  public void setTimeValuePairsList(
      List<TimeValuePair> timeValuePairsList) {
    this.timeValuePairsList = timeValuePairsList;
  }

  public void add(TimeValuePair pair){
    timeValuePairsList.add(pair);
  }
}
