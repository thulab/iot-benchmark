package cn.edu.tsinghua.iot.benchmark.constant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ThreadName {
  // -------------------------- ClientService --------------------------
  DATA_CLIENT_THREAD("DataClientService"),
  SCHEMA_CLIENT_THREAD("SchemaClientService"),
  DATA_CLIENT_EXECUTE_JOB("DataClientExecuteJob"),

  // -------------------------- showService --------------------------
  SHOW_WORK_PROCESS("ShowWorkProgress"),
  SHOW_RESULT_PERIODICALLY("ShowResultPeriodically"),
  RESULT_PERSISTENCE("ResultPersistence"),
  CSV_RECORDER("CSVRecorder"),

  UNKNOWN("UNKNOWN");

  private final String name;
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadName.class);

  ThreadName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
