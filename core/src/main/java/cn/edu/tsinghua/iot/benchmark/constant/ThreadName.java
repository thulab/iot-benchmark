package cn.edu.tsinghua.iot.benchmark.constant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum ThreadName {
  // -------------------------- ClientService --------------------------
  DATA_CLIENT_THREAD("DataClient_Service"),
  SCHEMA_CLIENT_THREAD("SchemaClient_Service"),
  DATA_CLIENT_EXECUTE_JOB("DataClientExecuteJob"),

  // -------------------------- showService --------------------------
  SHOW_WORK_PROCESS("Show_WorkProgress"),
  SHOW_RESULT_PERIODICALLY("Show_Result_Periodically"),
  RESULT_PERSISTENCE("Result_Persistence"),
  CSV_RECORDER("CSV_Recorder"),

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
