package cn.edu.tsinghua.iotdb.benchmark.measurement;

public class Status {

  private boolean isOk;
  private long costTime;
  private Exception exception;
  // errorMessage is our self-defined message used to logged into MySQL,
  // it can be the error SQL or anything.
  private String errorMessage;

  public Status(boolean isOk, long costTime, Exception exception, String errorMessage) {
    this.isOk = isOk;
    this.costTime = costTime;
    this.exception = exception;
    this.errorMessage = errorMessage;
  }

  public long getCostTime() {
    return costTime;
  }

  public Exception getException() {
    return exception;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public boolean isOk() {
    return isOk;
  }


}
