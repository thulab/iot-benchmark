/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iot.benchmark.conf;

import cn.edu.tsinghua.iot.benchmark.mode.enums.BenchmarkMode;
import cn.edu.tsinghua.iot.benchmark.tsdb.DBConfig;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBSwitch;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBType;
import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBVersion;
import cn.edu.tsinghua.iot.benchmark.workload.enums.OutOfOrderMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Properties;

public class ConfigDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigDescriptor.class);

  private final Config config;

  private static class ConfigDescriptorHolder {
    private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
  }

  private ConfigDescriptor() {
    config = new Config();
    // load properties and call init methods
    loadProps();
    // check properties
    if (!checkConfig()) {
      System.exit(1);
    }
    config.initInnerFunction();
    config.initSensorCodes();
    config.initSensorFunction();
  }

  public static ConfigDescriptor getInstance() {
    return ConfigDescriptorHolder.INSTANCE;
  }

  public Config getConfig() {
    return config;
  }

  /** load properties from config.properties */
  private void loadProps() {
    String url =
        System.getProperty(Constants.BENCHMARK_CONF, "configuration/conf/config.properties");
    if (url != null) {
      InputStream inputStream;
      try {
        inputStream = new FileInputStream(new File(url));
      } catch (FileNotFoundException e) {
        LOGGER.warn("Fail to find config file {}", url);
        return;
      }
      Properties properties = new Properties();
      try {
        properties.load(inputStream);
        config.setIS_DELETE_DATA(
            Boolean.parseBoolean(
                properties.getProperty("IS_DELETE_DATA", config.isIS_DELETE_DATA() + "")));
        config.setINIT_WAIT_TIME(
            Long.parseLong(
                properties.getProperty("INIT_WAIT_TIME", config.getINIT_WAIT_TIME() + "")));
        config.setNET_DEVICE(properties.getProperty("NET_DEVICE", config.getNET_DEVICE()));
        config.setLOOP(Long.parseLong(properties.getProperty("LOOP", config.getLOOP() + "")));
        config.setBENCHMARK_WORK_MODE(
            BenchmarkMode.getBenchmarkMode(properties.getProperty("BENCHMARK_WORK_MODE", "")));
        config.setTEST_MAX_TIME(
            Long.parseLong(
                properties.getProperty("TEST_MAX_TIME", config.getTEST_MAX_TIME() + "")));
        config.setUSE_MEASUREMENT(
            Boolean.parseBoolean(
                properties.getProperty("USE_MEASUREMENT", config.isUSE_MEASUREMENT() + "")));
        config.setRESULT_PRECISION(
            Double.parseDouble(
                properties.getProperty("RESULT_PRECISION", config.getRESULT_PRECISION() + "")));

        config.setDB_SWITCH(DBSwitch.getDBType(properties.getProperty("DB_SWITCH", "")));
        String hosts = properties.getProperty("HOST", config.getDbConfig().getHOST() + "");
        config.setHOST(Arrays.asList(hosts.split(",")));
        config.setIsAllNodesVisible(properties.getProperty("IS_ALL_NODES_VISIBLE ", "false"));
        String ports = properties.getProperty("PORT", config.getDbConfig().getPORT() + "");
        config.setPORT(Arrays.asList(ports.split(",")));
        config.setUSERNAME(properties.getProperty("USERNAME", config.getDbConfig().getUSERNAME()));
        config.setPASSWORD(properties.getProperty("PASSWORD", config.getDbConfig().getPASSWORD()));
        config.setDB_NAME(properties.getProperty("DB_NAME", config.getDbConfig().getDB_NAME()));
        config.setTOKEN(properties.getProperty("TOKEN", config.getDbConfig().getTOKEN()));

        config.setIS_DOUBLE_WRITE(
            Boolean.parseBoolean(
                properties.getProperty("IS_DOUBLE_WRITE", config.isIS_DOUBLE_WRITE() + "")));
        if (config.isIS_DOUBLE_WRITE()) {
          config.setANOTHER_DB_SWITCH(
              DBSwitch.getDBType(properties.getProperty("ANOTHER_DB_SWITCH", "")));
          String anotherHosts =
              properties.getProperty("ANOTHER_HOST", config.getANOTHER_DBConfig().getHOST() + "");
          config.setANOTHER_HOST(Arrays.asList(anotherHosts.split(",")));
          String anotherPorts =
              properties.getProperty("ANOTHER_PORT", config.getANOTHER_DBConfig().getPORT() + "");
          config.setANOTHER_PORT(Arrays.asList(anotherPorts.split(",")));
          config.setANOTHER_USERNAME(
              properties.getProperty(
                  "ANOTHER_USERNAME", config.getANOTHER_DBConfig().getUSERNAME()));
          config.setANOTHER_PASSWORD(
              properties.getProperty(
                  "ANOTHER_PASSWORD", config.getANOTHER_DBConfig().getPASSWORD()));
          config.setANOTHER_DB_NAME(
              properties.getProperty("ANOTHER_DB_NAME", config.getANOTHER_DBConfig().getDB_NAME()));
          config.setANOTHER_TOKEN(
              properties.getProperty("ANOTHER_TOKEN", config.getANOTHER_DBConfig().getTOKEN()));
          config.setIS_COMPARISON(
              Boolean.parseBoolean(
                  properties.getProperty("IS_COMPARISON", config.isIS_COMPARISON() + "")));
          config.setIS_POINT_COMPARISON(
              Boolean.parseBoolean(
                  properties.getProperty(
                      "IS_POINT_COMPARISON", config.isIS_POINT_COMPARISON() + "")));
          if (config.isIS_POINT_COMPARISON()) {
            config.setVERIFICATION_STEP_SIZE(
                Integer.parseInt(
                    properties.getProperty(
                        "VERIFICATION_STEP_SIZE", config.getVERIFICATION_STEP_SIZE() + "")));
          }
        }

        String dataDir = properties.getProperty("IOTDB_DATA_DIR", "/home/liurui/data/data");
        config.setIOTDB_DATA_DIR(Arrays.asList(dataDir.split(",")));
        String walDir = properties.getProperty("IOTDB_WAL_DIR", "/home/liurui/data/wal");
        config.setIOTDB_WAL_DIR(Arrays.asList(walDir.split(",")));
        String systemDir = properties.getProperty("IOTDB_SYSTEM_DIR", "/home/liurui/data/system");
        config.setIOTDB_SYSTEM_DIR(Arrays.asList(systemDir.split(",")));
        for (String data : config.getIOTDB_DATA_DIR()) {
          config.getSEQUENCE_DIR().add(data + "/sequence");
          config.getUNSEQUENCE_DIR().add(data + "/unsequence");
        }

        config.setKAFKA_LOCATION(
            properties.getProperty("KAFKA_LOCATION", config.getKAFKA_LOCATION() + ""));
        config.setZOOKEEPER_LOCATION(
            properties.getProperty("ZOOKEEPER_LOCATION", config.getZOOKEEPER_LOCATION() + ""));
        config.setTOPIC_NAME(properties.getProperty("TOPIC_NAME", "NULL"));

        config.setPOINT_STEP(
            Long.parseLong(properties.getProperty("POINT_STEP", config.getPOINT_STEP() + "")));
        config.setTIMESTAMP_PRECISION(
            properties.getProperty("TIMESTAMP_PRECISION", config.getTIMESTAMP_PRECISION() + ""));
        // check whether is able to run in this precision
        switch (config.getTIMESTAMP_PRECISION()) {
          case "ms":
            break;
          case "us":
          case "ns":
            if (config.getDbConfig().getDB_SWITCH().getType() != DBType.IoTDB
                && config.getDbConfig().getDB_SWITCH().getType() != DBType.InfluxDB) {
              throw new RuntimeException(
                  "The database "
                      + config.getDbConfig().getDB_SWITCH()
                      + " can't use microsecond precision");
            }
            break;
          default:
            throw new RuntimeException(
                "not support timestamp precision: " + config.getTIMESTAMP_PRECISION());
        }

        config.setSTRING_LENGTH(
            Integer.parseInt(
                properties.getProperty("STRING_LENGTH", config.getSTRING_LENGTH() + "")));
        config.setDOUBLE_LENGTH(
            Integer.parseInt(
                properties.getProperty("DOUBLE_LENGTH", config.getDOUBLE_LENGTH() + "")));
        config.setINSERT_DATATYPE_PROPORTION(
            properties.getProperty(
                "INSERT_DATATYPE_PROPORTION", config.getINSERT_DATATYPE_PROPORTION()));

        config.setCOMPRESSOR(properties.getProperty("COMPRESSOR", config.getCOMPRESSOR()));
        config.setENCODING_BOOLEAN(
            properties.getProperty("ENCODING_BOOLEAN", config.getENCODING_BOOLEAN()));
        config.setENCODING_INT32(
            properties.getProperty("ENCODING_INT32", config.getENCODING_INT32()));
        config.setENCODING_INT64(
            properties.getProperty("ENCODING_INT64", config.getENCODING_INT64()));
        config.setENCODING_FLOAT(
            properties.getProperty("ENCODING_FLOAT", config.getENCODING_FLOAT()));
        config.setENCODING_DOUBLE(
            properties.getProperty("ENCODING_DOUBLE", config.getENCODING_DOUBLE()));
        config.setENCODING_TEXT(properties.getProperty("ENCODING_TEXT", config.getENCODING_TEXT()));

        config.setFILE_PATH(properties.getProperty("FILE_PATH", config.getFILE_PATH()));
        config.setBIG_BATCH_SIZE(
            Integer.parseInt(
                properties.getProperty("BIG_BATCH_SIZE", config.getBIG_BATCH_SIZE() + "")));

        config.setDEVICE_NUMBER(
            Integer.parseInt(
                properties.getProperty("DEVICE_NUMBER", config.getDEVICE_NUMBER() + "")));
        config.setREAL_INSERT_RATE(
            Double.parseDouble(
                properties.getProperty("REAL_INSERT_RATE", config.getREAL_INSERT_RATE() + "")));
        if (config.getREAL_INSERT_RATE() <= 0 || config.getREAL_INSERT_RATE() > 1) {
          config.setREAL_INSERT_RATE(1);
          LOGGER.error(
              "Invalid parameter REAL_INSERT_RATE: {}, whose value range should be (0, "
                  + "1], using default value 1.0",
              config.getREAL_INSERT_RATE());
        }
        config.setSENSOR_NUMBER(
            Integer.parseInt(
                properties.getProperty("SENSOR_NUMBER", config.getSENSOR_NUMBER() + "")));
        config.setIS_SENSOR_TS_ALIGNMENT(
            Boolean.parseBoolean(
                properties.getProperty(
                    "IS_SENSOR_TS_ALIGNMENT", config.isIS_SENSOR_TS_ALIGNMENT() + "")));
        if (!config.isIS_SENSOR_TS_ALIGNMENT()) {
          config.setTS_ALIGNMENT_RATIO(
              Double.parseDouble(
                  properties.getProperty(
                      "TS_ALIGNMENT_RATIO", config.getTS_ALIGNMENT_RATIO() + "")));
        }
        config.setIS_CLIENT_BIND(
            Boolean.parseBoolean(
                properties.getProperty("IS_CLIENT_BIND", config.isIS_CLIENT_BIND() + "")));
        config.setCLIENT_NUMBER(
            Integer.parseInt(
                properties.getProperty("CLIENT_NUMBER", config.getCLIENT_NUMBER() + "")));
        config.setGROUP_NAME_PREFIX(
            properties.getProperty("GROUP_NAME_PREFIX", config.getGROUP_NAME_PREFIX()));
        config.setDEVICE_NAME_PREFIX(
            properties.getProperty("DEVICE_NAME_PREFIX", config.getDEVICE_NAME_PREFIX()));
        config.setSENSOR_NAME_PREFIX(
            properties.getProperty("SENSOR_NAME_PREFIX", config.getSENSOR_NAME_PREFIX()));
        config.setDEVICE_TAGS(properties.getProperty("DEVICE_TAGS", ""));

        config.setBENCHMARK_CLUSTER(
            Boolean.parseBoolean(
                properties.getProperty("BENCHMARK_CLUSTER", config.isBENCHMARK_CLUSTER() + "")));
        if (config.isBENCHMARK_CLUSTER()) {
          config.setBENCHMARK_INDEX(
              Integer.parseInt(
                  properties.getProperty("BENCHMARK_INDEX", config.getBENCHMARK_INDEX() + "")));
          config.setFIRST_DEVICE_INDEX(config.getBENCHMARK_INDEX() * config.getDEVICE_NUMBER());
        } else {
          config.setFIRST_DEVICE_INDEX(0);
        }
        config.setIS_ALL_NODES_VISIBLE(
            Boolean.parseBoolean(
                properties.getProperty(
                    "IS_ALL_NODES_VISIBLE", String.valueOf(config.isIS_ALL_NODES_VISIBLE()))));

        config.setLINE_RATIO(
            Double.parseDouble(properties.getProperty("LINE_RATIO", config.getLINE_RATIO() + "")));
        config.setSIN_RATIO(
            Double.parseDouble(properties.getProperty("SIN_RATIO", config.getSIN_RATIO() + "")));
        config.setSQUARE_RATIO(
            Double.parseDouble(
                properties.getProperty("SQUARE_RATIO", config.getSQUARE_RATIO() + "")));
        config.setRANDOM_RATIO(
            Double.parseDouble(
                properties.getProperty("RANDOM_RATIO", config.getRANDOM_RATIO() + "")));
        config.setCONSTANT_RATIO(
            Double.parseDouble(
                properties.getProperty("CONSTANT_RATIO", config.getCONSTANT_RATIO() + "")));
        config.setDATA_SEED(
            Long.parseLong(properties.getProperty("DATA_SEED", config.getDATA_SEED() + "")));

        config.setENABLE_THRIFT_COMPRESSION(
            Boolean.parseBoolean(
                properties.getProperty(
                    "ENABLE_THRIFT_COMPRESSION", config.isENABLE_THRIFT_COMPRESSION() + "")));
        config.setSG_STRATEGY(properties.getProperty("SG_STRATEGY", "hash"));
        config.setGROUP_NUMBER(
            Integer.parseInt(
                properties.getProperty("GROUP_NUMBER", config.getGROUP_NUMBER() + "")));
        config.setIOTDB_SESSION_POOL_SIZE(
            Integer.parseInt(
                properties.getProperty(
                    "IOTDB_SESSION_POOL_SIZE", config.getIOTDB_SESSION_POOL_SIZE() + "")));
        config.setTEMPLATE(
            Boolean.parseBoolean(
                properties.getProperty("TEMPLATE", String.valueOf(config.isTEMPLATE()))));
        config.setVECTOR(
            Boolean.parseBoolean(
                properties.getProperty("VECTOR", String.valueOf(config.isVECTOR()))));
        config.setIOTDB_USE_DEBUG(
            Boolean.parseBoolean(
                properties.getProperty(
                    "IOTDB_USE_DEBUG", String.valueOf(config.isIOTDB_USE_DEBUG()))));
        config.setIOTDB_USE_DEBUG_RATIO(
            Double.parseDouble(
                properties.getProperty(
                    "IOTDB_USE_DEBUG_RATIO", String.valueOf(config.getIOTDB_USE_DEBUG_RATIO()))));
        config.setHTTP_CLIENT_POOL_SIZE(
            Integer.parseInt(
                properties.getProperty(
                    "HTTP_CLIENT_POOL_SIZE", String.valueOf(config.getHTTP_CLIENT_POOL_SIZE()))));

        config.setCOMPRESSION(properties.getProperty("COMPRESSION", "NONE"));
        config.setTIMESCALEDB_REPLICATION_FACTOR(
            Integer.parseInt(
                properties.getProperty(
                    "TIMESCALEDB_REPLICATION_FACTOR",
                    config.getTIMESCALEDB_REPLICATION_FACTOR() + "")));
        config.setTDENGINE_WAL_LEVEL(
            Integer.parseInt(
                properties.getProperty("TDENGINE_WAL_LEVEL", config.getTDENGINE_WAL_LEVEL() + "")));
        config.setTDENGINE_REPLICA(
            Integer.parseInt(
                properties.getProperty("TDENGINE_REPLICA", config.getTDENGINE_REPLICA() + "")));

        config.setOP_MIN_INTERVAL(
            Long.parseLong(
                properties.getProperty("OP_MIN_INTERVAL", config.getOP_MIN_INTERVAL() + "")));
        if (config.getOP_MIN_INTERVAL() == -1L) {
          config.setOP_MIN_INTERVAL(config.getPOINT_STEP());
        }
        config.setOP_MIN_INTERVAL_RANDOM(
            Boolean.parseBoolean(
                properties.getProperty(
                    "OP_MIN_INTERVAL_RANDOM", config.isOP_MIN_INTERVAL_RANDOM() + "")));
        config.setWRITE_OPERATION_TIMEOUT_MS(
            Integer.parseInt(
                properties.getProperty(
                    "WRITE_OPERATION_TIMEOUT_MS", config.getWRITE_OPERATION_TIMEOUT_MS() + "")));
        config.setREAD_OPERATION_TIMEOUT_MS(
            Integer.parseInt(
                properties.getProperty(
                    "READ_OPERATION_TIMEOUT_MS", config.getREAD_OPERATION_TIMEOUT_MS() + "")));
        config.setBATCH_SIZE_PER_WRITE(
            Integer.parseInt(
                properties.getProperty(
                    "BATCH_SIZE_PER_WRITE", config.getBATCH_SIZE_PER_WRITE() + "")));

        config.setCREATE_SCHEMA(
            Boolean.parseBoolean(
                properties.getProperty("CREATE_SCHEMA", config.isCREATE_SCHEMA() + "")));
        config.setSTART_TIME(properties.getProperty("START_TIME", config.getSTART_TIME()));
        config.setIS_COPY_MODE(
            Boolean.parseBoolean(
                properties.getProperty("IS_COPY_MODE", config.isIS_COPY_MODE() + "")));
        config.setIS_ADD_ANOMALY(
            Boolean.parseBoolean(
                properties.getProperty("IS_ADD_ANOMALY", config.isIS_ADD_ANOMALY() + "")));
        config.setANOMALY_RATE(
            Double.parseDouble(
                properties.getProperty("ANOMALY_RATE", config.getANOMALY_RATE() + "")));
        config.setANOMALY_TIMES(
            Integer.parseInt(
                properties.getProperty("ANOMALY_TIMES", config.getANOMALY_TIMES() + "")));
        config.setIS_OUT_OF_ORDER(
            Boolean.parseBoolean(
                properties.getProperty("IS_OUT_OF_ORDER", config.isIS_OUT_OF_ORDER() + "")));
        config.setOUT_OF_ORDER_MODE(
            OutOfOrderMode.getOutOfOrderMode(
                properties.getProperty(
                    "OUT_OF_ORDER_MODE", config.getOUT_OF_ORDER_MODE().toString())));
        config.setOUT_OF_ORDER_RATIO(
            Double.parseDouble(
                properties.getProperty("OUT_OF_ORDER_RATIO", config.getOUT_OF_ORDER_RATIO() + "")));
        config.setIS_REGULAR_FREQUENCY(
            Boolean.parseBoolean(
                properties.getProperty(
                    "IS_REGULAR_FREQUENCY", config.isIS_REGULAR_FREQUENCY() + "")));

        config.setLAMBDA(
            Double.parseDouble(properties.getProperty("LAMBDA", config.getLAMBDA() + "")));
        config.setMAX_K(Integer.parseInt(properties.getProperty("MAX_K", config.getMAX_K() + "")));

        config.setIS_RECENT_QUERY(
            Boolean.parseBoolean(
                properties.getProperty("IS_RECENT_QUERY", config.isIS_RECENT_QUERY() + "")));
        config.setSTEP_SIZE(
            Long.parseLong(properties.getProperty("STEP_SIZE", config.getSTEP_SIZE() + "")));
        config.setOPERATION_PROPORTION(
            properties.getProperty("OPERATION_PROPORTION", config.getOPERATION_PROPORTION()));
        config.setQUERY_SENSOR_NUM(
            Integer.parseInt(
                properties.getProperty("QUERY_SENSOR_NUM", config.getQUERY_SENSOR_NUM() + "")));
        config.setQUERY_DEVICE_NUM(
            Integer.parseInt(
                properties.getProperty("QUERY_DEVICE_NUM", config.getQUERY_DEVICE_NUM() + "")));
        config.setQUERY_AGGREGATE_FUN(
            properties.getProperty("QUERY_AGGREGATE_FUN", config.getQUERY_AGGREGATE_FUN()));

        config.setQUERY_INTERVAL(
            Long.parseLong(
                properties.getProperty("QUERY_INTERVAL", config.getQUERY_INTERVAL() + "")));
        config.setQUERY_LOWER_VALUE(
            Double.parseDouble(
                properties.getProperty("QUERY_LOWER_VALUE", config.getQUERY_LOWER_VALUE() + "")));
        config.setGROUP_BY_TIME_UNIT(
            Long.parseLong(
                properties.getProperty("GROUP_BY_TIME_UNIT", config.getGROUP_BY_TIME_UNIT() + "")));
        config.setQUERY_SEED(
            Long.parseLong(properties.getProperty("QUERY_SEED", config.getQUERY_SEED() + "")));

        config.setWORKLOAD_BUFFER_SIZE(
            Integer.parseInt(
                properties.getProperty(
                    "WORKLOAD_BUFFER_SIZE", config.getWORKLOAD_BUFFER_SIZE() + "")));
        config.setTEST_DATA_PERSISTENCE(properties.getProperty("TEST_DATA_PERSISTENCE", "None"));
        config.setRECORD_SPLIT(
            Boolean.parseBoolean(
                properties.getProperty("RECORD_SPLIT", config.isRECORD_SPLIT() + "")));
        config.setRECORD_SPLIT_MAX_LINE(
            Long.parseLong(
                properties.getProperty(
                    "RECORD_SPLIT_MAX_LINE", config.getRECORD_SPLIT_MAX_LINE() + "")));

        config.setMONITOR_INTERVAL(
            Integer.parseInt(
                properties.getProperty("MONITOR_INTERVAL", config.getMONITOR_INTERVAL() + "")));

        config.setIS_QUIET_MODE(
            Boolean.parseBoolean(
                properties.getProperty("IS_QUIET_MODE", config.isIS_QUIET_MODE() + "")));
        config.setLOG_PRINT_INTERVAL(
            Integer.parseInt(
                properties.getProperty("LOG_PRINT_INTERVAL", config.getLOG_PRINT_INTERVAL() + "")));

        config.setTEST_DATA_STORE_IP(
            properties.getProperty("TEST_DATA_STORE_IP", config.getTEST_DATA_STORE_IP()));
        config.setTEST_DATA_STORE_PORT(
            properties.getProperty("TEST_DATA_STORE_PORT", config.getTEST_DATA_STORE_PORT()));
        config.setTEST_DATA_STORE_DB(
            properties.getProperty("TEST_DATA_STORE_DB", config.getTEST_DATA_STORE_DB()));
        config.setTEST_DATA_STORE_USER(
            properties.getProperty("TEST_DATA_STORE_USER", config.getTEST_DATA_STORE_USER()));
        config.setTEST_DATA_STORE_PW(
            properties.getProperty("TEST_DATA_STORE_PW", config.getTEST_DATA_STORE_PW()));
        config.setTEST_DATA_WRITE_TIME_OUT(
            Integer.parseInt(
                properties.getProperty(
                    "TEST_DATA_WRITE_TIME_OUT", config.getTEST_DATA_WRITE_TIME_OUT() + "")));
        config.setTEST_DATA_MAX_CONNECTION(
            Integer.parseInt(
                properties.getProperty(
                    "TEST_DATA_MAX_CONNECTION", config.getTEST_DATA_MAX_CONNECTION() + "")));
        if (config.getTEST_DATA_PERSISTENCE().equals("CSV")) {
          config.setTEST_DATA_MAX_CONNECTION(1);
        }

        config.setREMARK(properties.getProperty("REMARK", "-"));
        config.setMYSQL_REAL_INSERT_RATE(
            Double.parseDouble(
                properties.getProperty(
                    "MYSQL_REAL_INSERT_RATE", config.getMYSQL_REAL_INSERT_RATE() + "")));

        config.setMYSQL_REAL_INSERT_RATE(
            Double.parseDouble(
                properties.getProperty(
                    "MYSQL_REAL_INSERT_RATE", config.getMYSQL_REAL_INSERT_RATE() + "")));

        config.setCSV_OUTPUT(
            Boolean.parseBoolean(properties.getProperty("CSV_OUTPUT", config.isCSV_OUTPUT() + "")));
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
        inputStream.close();
      } catch (IOException e) {
        LOGGER.error("Fail to close config file input stream", e);
      }
    } else {
      LOGGER.warn("{} No config file path, use default config", Constants.CONSOLE_PREFIX);
    }
  }

  /** Check validation of config */
  private boolean checkConfig() {
    boolean result = true;
    // Checking config according to mode
    switch (config.getBENCHMARK_WORK_MODE()) {
      case TEST_WITH_DEFAULT_PATH:
        if (config.isIS_CLIENT_BIND() && config.getDEVICE_NUMBER() < config.getCLIENT_NUMBER()) {
          LOGGER.error(
              "In client bind way, the number of client should be greater than the number of device");
          result = false;
        }
        if (!config.hasWrite()) {
          // no write
          checkQuery();
        } else {
          if (!config.isIS_DELETE_DATA()) {
            LOGGER.info("Benchmark not delete data before writing.");
          }
          if (!config.isCREATE_SCHEMA()) {
            LOGGER.info("Benchmark not create schema before writing.");
          }
        }
        if (config.isIS_DOUBLE_WRITE()) {
          DBConfig dbConfig = config.getDbConfig();
          DBConfig anotherConfig = config.getANOTHER_DBConfig();
          if (dbConfig.getDB_SWITCH() == DBSwitch.DB_INFLUX
              || anotherConfig.getDB_SWITCH() == DBSwitch.DB_INFLUX) {
            LOGGER.error("Double write not support influxdb v1.x");
            result = false;
          }
          if (config.isIS_COMPARISON() && config.isIS_POINT_COMPARISON()) {
            LOGGER.error(
                "Benchmark not support IS_COMPARISON and IS_POINT_COMPARISON, please only choose one");
            result = false;
            checkQuery();
          } else {
            if (config.isIS_COMPARISON()) {
              if (!config.hasQuery()) {
                LOGGER.warn(
                    "There is no query when doing comparison, so auto set IS_COMPARISON = false");
                config.setIS_COMPARISON(false);
              }
            }
            if (config.isIS_POINT_COMPARISON()) {
              if (config.getDEVICE_NUMBER() < config.getCLIENT_NUMBER()) {
                LOGGER.warn("There are too many client ( > device number)");
              }
            }
          }
          if (config.isIS_COMPARISON() || config.isIS_POINT_COMPARISON()) {
            result &= checkDatabaseVerification(dbConfig);
            result &= checkDatabaseVerification(anotherConfig);
            checkQuery();
          }
        } else {
          if (config.isIS_COMPARISON() || config.isIS_POINT_COMPARISON()) {
            LOGGER.warn(
                "There are ONLY 1 database, not support IS_COMPARISON and IS_POINT_COMPARISON");
            config.setIS_COMPARISON(false);
            config.setIS_POINT_COMPARISON(false);
          }
        }
        break;
      case VERIFICATION_QUERY:
        result &= checkDatabaseVerification(config.getDbConfig());
        if (config.isIS_DOUBLE_WRITE()) {
          result &= checkDatabaseVerification(config.getANOTHER_DBConfig());
        }
      default:
        break;
    }
    // check config
    String[] op = config.getOPERATION_PROPORTION().split(":");
    int minOps = 0;
    for (int i = 0; i < op.length; i++) {
      if (Double.valueOf(op[i]) > 1e-7) {
        minOps++;
      }
    }
    if (minOps > config.getLOOP()) {
      LOGGER.error("Loop is too small that can't meet the need of OPERATION_PROPORTION");
      result = false;
    }
    if (config.getCLIENT_NUMBER() == 0) {
      LOGGER.error("Client number can't be zero");
      result = false;
    }
    return result;
  }

  private void checkQuery() {
    if (config.isIS_DELETE_DATA()) {
      LOGGER.warn("Benchmark is doing query, no need to delete data.");
      config.setIS_DELETE_DATA(false);
    }
    if (config.isCREATE_SCHEMA()) {
      LOGGER.warn("Benchmark is doing query, no need to create schema.");
      config.setCREATE_SCHEMA(false);
    }
    if (config.isIS_RECENT_QUERY()) {
      LOGGER.warn("Benchmark is only doing query, can not do recent query.");
      config.setIS_RECENT_QUERY(false);
    }
  }

  /**
   * Check whether database support verification
   *
   * @param dbConfig
   * @return
   */
  private boolean checkDatabaseVerification(DBConfig dbConfig) {
    boolean result = false;
    if (dbConfig.getDB_SWITCH().getType() == DBType.IoTDB) {
      // support iotdb 0.12 & 0.13
      if (dbConfig.getDB_SWITCH().getVersion() == DBVersion.IOTDB_100
          || dbConfig.getDB_SWITCH().getVersion() == DBVersion.IOTDB_013
          || dbConfig.getDB_SWITCH().getVersion() == DBVersion.IOTDB_012) {
        result = true;
      }
    } else if (dbConfig.getDB_SWITCH().getType() == DBType.InfluxDB) {
      // support influxdb 1.x
      if (dbConfig.getDB_SWITCH().getVersion() == null) {
        result = true;
      }
    } else if (dbConfig.getDB_SWITCH() == DBSwitch.DB_TIMESCALE) {
      // support timescaledb
      result = true;
    }
    if (!result) {
      LOGGER.error(
          "Verification only support between iotdb v0.12, v0.13, timescaledb and influxdb 1.x");
    }
    return result;
  }
}
