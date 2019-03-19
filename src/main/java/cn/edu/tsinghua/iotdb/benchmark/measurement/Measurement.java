package cn.edu.tsinghua.iotdb.benchmark.measurement;


import cn.edu.tsinghua.iotdb.benchmark.client.OperationController.Operation;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Measurement {

  private Map<Operation, List<Double>> operationLatencies;
  private double createSchemaTime;
  private double elapseTime;
  private static final double[] MID_AVG_RANGE = {0.1, 0.9};

  public Measurement() {
    operationLatencies = new EnumMap<>(Operation.class);
    for (Operation operation : Operation.values()) {
      operationLatencies.put(operation, new ArrayList<>());
    }
  }

  private Map<Operation, List<Double>> getOperationLatencies() {
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

  public void mergeOperationLatency(Measurement m) {
    for (Operation operation : Operation.values()) {
      operationLatencies.get(operation).addAll(m.getOperationLatencies().get(operation));
    }
    updateMetrics();
  }

  public void addOperationLatency(Operation op, double latency) {
    operationLatencies.get(op).add(latency);
  }

  public void updateMetrics() {
    for (Operation operation : Operation.values()) {
      List<Double> latencyList = operationLatencies.get(operation);
      int totalOps = latencyList.size();
      double sumLatency = 0;
      for (double latency : latencyList) {
        sumLatency += latency;
      }
      double avgLatency = sumLatency / totalOps;
      Metric.SUM_LATENCY.getTypeValueMap().put(operation, sumLatency);
      Metric.AVG_LATENCY.getTypeValueMap().put(operation, avgLatency);
      latencyList.sort(new DoubleComparator());
      Metric.MIN_LATENCY.getTypeValueMap().put(operation, latencyList.get(0));
      Metric.MAX_LATENCY.getTypeValueMap().put(operation, latencyList.get(totalOps - 1));
      Metric.P10_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.10)));
      Metric.P25_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.25)));
      Metric.MEDIAN_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.50)));
      Metric.P75_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.75)));
      Metric.P90_LATENCY.getTypeValueMap().put(operation, latencyList.get((int) (totalOps * 0.90)));
      double midAvgLatency = 0;
      double midSum = 0;
      for (int i = (int) (totalOps * MID_AVG_RANGE[0]); i < (int) (totalOps * MID_AVG_RANGE[1]); i++) {
        midSum += latencyList.get(i);
      }
      midAvgLatency = midSum / ((int) (totalOps * MID_AVG_RANGE[1]) - (totalOps * MID_AVG_RANGE[0]));
      Metric.MID_AVG_LATENCY.getTypeValueMap().put(operation, midAvgLatency);
    }
  }

  class DoubleComparator implements Comparator<Double> {

    @Override
    public int compare(Double a, Double b) {
      if (a < b) {
        return -1;
      } else if (Objects.equals(a, b)) {
        return 0;
      } else {
        return 1;
      }
    }
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

    public Map<Operation, Double> getTypeValueMap() {
      return typeValueMap;
    }

    public void setTypeValueMap(
        Map<Operation, Double> typeValueMap) {
      this.typeValueMap = typeValueMap;
    }

    Map<Operation, Double> typeValueMap;

    Metric() {
      typeValueMap = new EnumMap<Operation, Double>(Operation.class);
    }

  }

  public enum Status {
    OK,
    FAIL,
    NOT_SUPPORT
  }
}
