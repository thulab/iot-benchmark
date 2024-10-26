/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.mode;

import cn.edu.tsinghua.iot.benchmark.conf.Config;
import cn.edu.tsinghua.iot.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iot.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iot.benchmark.measurement.persistence.TestDataPersistence;
import cn.edu.tsinghua.iot.benchmark.mode.syslog.IoUsage;
import cn.edu.tsinghua.iot.benchmark.mode.syslog.MemUsage;
import cn.edu.tsinghua.iot.benchmark.mode.syslog.NetUsage;
import cn.edu.tsinghua.iot.benchmark.mode.syslog.OpenFileStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Benchmark mode: serverMode Server-side mode, monitor performance indicators such as system
 * memory, and obtain the inserted data file size
 */
public class ServerMode extends BaseMode {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerMode.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  protected boolean preCheck() {
    return true;
  }

  /** Start benchmark */
  @Override
  public void run() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    TestDataPersistence recorder = persistenceFactory.getPersistence();

    float abnormalValue = -1;

    HashMap<IoUsage.IOStatistics, Float> ioStatistics;
    int interval = config.getMONITOR_INTERVAL();
    boolean headerPrinted = false;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    // The minimum of interval is 2s
    while (true) {
      long start = System.currentTimeMillis();
      ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
      LOGGER.debug(
          "IoUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
      start = System.currentTimeMillis();
      ArrayList<Float> netUsageList = NetUsage.getInstance().get();
      LOGGER.debug(
          "NetUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
      start = System.currentTimeMillis();
      ioStatistics = IoUsage.getInstance().getIOStatistics();
      LOGGER.debug(
          "IoUsage.getInstance().getIOStatistics() consume ,{}, ms",
          System.currentTimeMillis() - start);
      start = System.currentTimeMillis();
      double memRate = MemUsage.getInstance().get();
      LOGGER.debug(
          "MemUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
      start = System.currentTimeMillis();
      double proMem = MemUsage.getInstance().getProcessMemUsage();
      LOGGER.debug(
          "MemUsage.getInstance().getProcessMemUsage() consume ,{}, ms",
          System.currentTimeMillis() - start);

      if (!headerPrinted) {
        LOGGER.info(
            ",time,date,PID,memory usage(GB),memory ratio(%),cpu ratio(%),disk io usage(%),disk TPS,read rate(MB/s),write rate(MB/s),net receive rate(KB/s),net send rate(KB/s)");
        headerPrinted = true;
      }
      String time = sdf.format(new Date(start));
      LOGGER.info(
          ",{},{},{},{},{},{},{},{},{},{},{},{}",
          start,
          time,
          OpenFileStatistics.getInstance().getPid(),
          proMem,
          memRate,
          ioUsageList.get(0),
          ioUsageList.get(1),
          ioStatistics.get(IoUsage.IOStatistics.TPS),
          ioStatistics.get(IoUsage.IOStatistics.MB_READ),
          ioStatistics.get(IoUsage.IOStatistics.MB_WRTN),
          netUsageList.get(0),
          netUsageList.get(1));

      Map<SystemMetrics, Float> systemMetricsMap = new EnumMap<>(SystemMetrics.class);
      systemMetricsMap.put(SystemMetrics.CPU_USAGE, ioUsageList.get(0));
      systemMetricsMap.put(SystemMetrics.MEM_USAGE, MemUsage.getInstance().get());
      systemMetricsMap.put(SystemMetrics.DISK_IO_USAGE, ioUsageList.get(1));
      systemMetricsMap.put(SystemMetrics.NETWORK_R_RATE, netUsageList.get(0));
      systemMetricsMap.put(SystemMetrics.NETWORK_S_RATE, netUsageList.get(1));
      systemMetricsMap.put(
          SystemMetrics.PROCESS_MEM_SIZE, MemUsage.getInstance().getProcessMemUsage());
      systemMetricsMap.put(SystemMetrics.DISK_TPS, ioStatistics.get(IoUsage.IOStatistics.TPS));
      systemMetricsMap.put(
          SystemMetrics.DISK_READ_SPEED_MB, ioStatistics.get(IoUsage.IOStatistics.MB_READ));
      systemMetricsMap.put(
          SystemMetrics.DISK_WRITE_SPEED_MB, ioStatistics.get(IoUsage.IOStatistics.MB_WRTN));
      recorder.insertSystemMetrics(systemMetricsMap);

      try {
        Thread.sleep(interval * 1000L);
      } catch (Exception e) {
        LOGGER.error("sleep failed", e);
      }
    }
  }

  @Override
  protected void postCheck() {}
}
