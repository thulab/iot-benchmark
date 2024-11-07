package cn.edu.tsinghua.iot.benchmark.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
  private final String poolName;
  private final AtomicInteger count = new AtomicInteger(1);
  private final ThreadFactory threadFactory;
  private Boolean daemon = false;

  public NamedThreadFactory(String name) {
    this.poolName = name;
    this.threadFactory = Executors.defaultThreadFactory();
  }

  public NamedThreadFactory(String name, Boolean daemon) {
    this.poolName = name;
    this.daemon = daemon;
    this.threadFactory = Executors.defaultThreadFactory();
  }

  private String getThreadName() {
    return poolName + "-thread-" + String.valueOf(count.getAndIncrement());
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread = threadFactory.newThread(r);
    thread.setName(getThreadName());
    if (daemon) {
      thread.setDaemon(true);
    }
    return thread;
  }
}
