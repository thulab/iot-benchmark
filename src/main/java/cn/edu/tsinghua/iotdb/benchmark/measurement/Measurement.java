package cn.edu.tsinghua.iotdb.benchmark.measurement;


import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class Measurement {

  private Map<Operation, List<Double>> operationLatencies;
  private double createSchemaTime;
  private double elapseTime;

  public Measurement() {
    operationLatencies = new EnumMap<>(Operation.class);
  }

  public Map<Operation, List<Double>> getOperationLatencies() {
    return operationLatencies;
  }

  public void setOperationLatencies(
      Map<Operation, List<Double>> operationLatencies) {
    this.operationLatencies = operationLatencies;
  }

  public double getCreateSchemaTime() {
    return createSchemaTime;
  }

  public void setCreateSchemaTime(double createSchemaTime) {
    this.createSchemaTime = createSchemaTime;
  }

  public double getElapseTime() {
    return elapseTime;
  }

  public void setElapseTime(double elapseTime) {
    this.elapseTime = elapseTime;
  }

  public void mergeOperationLatency(Measurement m){
    for(Operation operation: Operation.values()){
      operationLatencies.get(operation).addAll(m.getOperationLatencies().get(operation));
    }
  }

  public void addOperationLatency(Operation op, double latency){
    operationLatencies.get(op).add(latency);
  }

  public void updateMetrics(){

  }

  public enum Metric {
    SUM_LATENCY,
    AVG_LATENCY,
    MEDIAN_LATENCY,
    MID_AVG_LATENCY,
    MIN_LATENCY,
    P10_LATENCY,
    P25_LATENCY,
    P75_LATENCY,
    P90_LATENCY,
    MAX_LATENCY,
    OK_NUM,
    FAIL_NUM;

    public double getValue() {
      return value;
    }

    public void setValue(double value) {
      this.value = value;
    }

    public Operation getType() {
      return type;
    }

    public void setType(Operation type) {
      this.type = type;
    }

    double value;

    Operation type;

  }

  public enum Status {
    OK,
    FAIL,
    NOT_SUPPORT
  }
}
