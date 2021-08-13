package cn.edu.tsinghua.iotdb.benchmark.mode;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.syslog.*;
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

  /** Start benchmark */
  @Override
  public void run() {
    PersistenceFactory persistenceFactory = new PersistenceFactory();
    ITestDataPersistence recorder = persistenceFactory.getPersistence();
    recorder.saveTestConfig();

    float abnormalValue = -1;

    Map<FileSize.FileSizeKinds, Float> fileSizeStatistics =
        new EnumMap<>(FileSize.FileSizeKinds.class);

    for (FileSize.FileSizeKinds kinds : FileSize.FileSizeKinds.values()) {
      fileSizeStatistics.put(kinds, abnormalValue);
    }

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
      fileSizeStatistics = FileSize.getInstance().getFileSize();
      LOGGER.debug(
          "FileSize.getInstance().getFileSize() consume ,{}, ms",
          System.currentTimeMillis() - start);
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
            ",测量时间,PID,内存使用大小GB,内存使用率,CPU使用率,磁盘IO使用率,磁盘TPS,读速率MB/s,写速率MB/s,网卡接收速率KB/s,网卡发送速率KB/s,data文件大小GB,system文件大小GB,sequence文件大小GB,unsequence文件大小GB,wal文件大小GB");
        headerPrinted = true;
      }
      String time = sdf.format(new Date(start));
      LOGGER.info(
          ",{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
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
          netUsageList.get(1),
          fileSizeStatistics.get(FileSize.FileSizeKinds.DATA),
          fileSizeStatistics.get(FileSize.FileSizeKinds.SYSTEM),
          fileSizeStatistics.get(FileSize.FileSizeKinds.SEQUENCE),
          fileSizeStatistics.get(FileSize.FileSizeKinds.UN_SEQUENCE),
          fileSizeStatistics.get(FileSize.FileSizeKinds.WAL));

      Map<SystemMetrics, Float> systemMetricsMap = new EnumMap<>(SystemMetrics.class);
      systemMetricsMap.put(SystemMetrics.CPU_USAGE, ioUsageList.get(0));
      systemMetricsMap.put(SystemMetrics.MEM_USAGE, MemUsage.getInstance().get());
      systemMetricsMap.put(SystemMetrics.DISK_IO_USAGE, ioUsageList.get(1));
      systemMetricsMap.put(SystemMetrics.NETWORK_R_RATE, netUsageList.get(0));
      systemMetricsMap.put(SystemMetrics.NETWORK_S_RATE, netUsageList.get(1));
      systemMetricsMap.put(
          SystemMetrics.PROCESS_MEM_SIZE, MemUsage.getInstance().getProcessMemUsage());
      systemMetricsMap.put(
          SystemMetrics.DATA_FILE_SIZE, fileSizeStatistics.get(FileSize.FileSizeKinds.DATA));
      systemMetricsMap.put(
          SystemMetrics.SYSTEM_FILE_SIZE, fileSizeStatistics.get(FileSize.FileSizeKinds.SYSTEM));
      systemMetricsMap.put(
          SystemMetrics.SEQUENCE_FILE_SIZE,
          fileSizeStatistics.get(FileSize.FileSizeKinds.SEQUENCE));
      systemMetricsMap.put(
          SystemMetrics.UN_SEQUENCE_FILE_SIZE,
          fileSizeStatistics.get(FileSize.FileSizeKinds.UN_SEQUENCE));
      systemMetricsMap.put(
          SystemMetrics.WAL_FILE_SIZE, fileSizeStatistics.get(FileSize.FileSizeKinds.WAL));
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
}
