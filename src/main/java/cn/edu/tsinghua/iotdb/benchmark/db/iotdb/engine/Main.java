package cn.edu.tsinghua.iotdb.benchmark.db.iotdb.engine;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by liukun on 19/3/11.
 */
public class Main {

  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static void main(String[] args) {
    lock.readLock().lock();
    lock.writeLock().lock();
    System.out.println(lock);
    lock.readLock().unlock();
  }

}
