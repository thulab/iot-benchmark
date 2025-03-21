package cn.edu.tsinghua.iot.benchmark.client.progress;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TaskProgress {
  /** Thread name */
  private final AtomicReference<String> threadNameAtomic;
  /** Total number of loop */
  private AtomicLong totalLoop;
  /** Loop Index, using for loop and log */
  private AtomicLong loopIndex;

  public TaskProgress() {
    this.threadNameAtomic = new AtomicReference<>();
    this.totalLoop = new AtomicLong(1);
    this.loopIndex = new AtomicLong(0);
  }

  public void setThreadName(String threadName) {
    threadNameAtomic.set(threadName);
  }

  public String getThreadName() {
    return threadNameAtomic.get();
  }

  public void setTotalLoop(Long value) {
    totalLoop.set(value);
  }

  public AtomicLong getLoopIndex() {
    return loopIndex;
  }

  public void resetLoopIndex() {
    loopIndex.set(0);
  }

  public void incrementLoopIndex() {
    loopIndex.getAndIncrement();
  }

  public double getPercent() {
    if (totalLoop.get() == 0) {
      return 0.00D;
    }
    return (double) loopIndex.get() * 100.0D / totalLoop.get();
  }
}
