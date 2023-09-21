package cn.edu.tsinghua.iot.benchmark.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger count = new AtomicInteger(1);
    private final ThreadFactory threadFactory;

    public NamedThreadFactory(String name) {
        this.poolName = name;
        this.threadFactory = Executors.defaultThreadFactory();
    }

    private String getThreadName() {
        return poolName + "-thread-" + String.valueOf(count.getAndIncrement());
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = threadFactory.newThread(r);
        thread.setName(getThreadName());
        return thread;
    }
}
