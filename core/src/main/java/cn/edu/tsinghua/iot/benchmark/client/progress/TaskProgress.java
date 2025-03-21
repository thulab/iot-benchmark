package cn.edu.tsinghua.iot.benchmark.client.progress;

import java.util.concurrent.atomic.AtomicReference;

public class TaskProgress {
  /** Thread name */
  private final AtomicReference<String> threadNameAtomic;
  /** Total number of loop */
  private volatile long totalLoop;
  /** Loop Index, using for loop and log */
  private volatile long loopIndex;

  public TaskProgress() {
    this.threadNameAtomic = new AtomicReference<>();
    this.totalLoop = 1L;
    this.loopIndex = 0L;
  }

  public void setThreadName(String threadName) {
    threadNameAtomic.set(threadName);
  }

  public String getThreadName() {
    return threadNameAtomic.get();
  }

  public void setTotalLoop(Long value) {
    totalLoop = value;
  }

  public long getLoopIndex() {
    return loopIndex;
  }

  public void resetLoopIndex() {
    loopIndex = 0L;
  }

  public void incrementLoopIndex() {
    loopIndex++;
  }

  public double getPercent() {
    if (totalLoop == 0) {
      return 0.00D;
    }
    return (double) loopIndex * 100.0D / totalLoop;
  }
}
