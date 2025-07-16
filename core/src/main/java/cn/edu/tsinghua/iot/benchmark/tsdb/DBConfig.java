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

package cn.edu.tsinghua.iot.benchmark.tsdb;

import cn.edu.tsinghua.iot.benchmark.tsdb.enums.DBSwitch;

import java.util.Collections;
import java.util.List;

/** Hold all configuration of database */
public class DBConfig {
  /**
   * The database to use, format: {name of database}{-version}{-insert mode} name of database, for
   * more, in README.md
   */
  private DBSwitch DB_SWITCH = DBSwitch.DB_IOT_200_SESSION_BY_TABLET;

  /** The host of database server for IoTDB */
  private List<String> HOST = Collections.singletonList("127.0.0.1");

  /** The port of database server */
  private List<String> PORT = Collections.singletonList("6667");

  /** The user name of database to use */
  private String USERNAME = "root";

  /** The password of user */
  private String PASSWORD = "root";

  /** The name of database to use, eg.IoTDB root.{DB_NAME} */
  private String DB_NAME = "test";

  /** In some database, it will need token to access, such as InfluxDB 2.0 */
  private String TOKEN = "token";

  public DBConfig() {}

  public DBSwitch getDB_SWITCH() {
    return DB_SWITCH;
  }

  public void setDB_SWITCH(DBSwitch DB_SWITCH) {
    this.DB_SWITCH = DB_SWITCH;
  }

  public List<String> getHOST() {
    return HOST;
  }

  public String getHOSTString() {
    String hosts = HOST.toString();
    if (hosts.startsWith("[")) {
      hosts = hosts.substring(1);
    }
    if (hosts.endsWith("]")) {
      hosts = hosts.substring(0, hosts.length() - 1);
    }
    return hosts;
  }

  public void setHOST(List<String> HOST) {
    this.HOST = HOST;
  }

  public List<String> getPORT() {
    return PORT;
  }

  public String getPORTString() {
    String ports = PORT.toString();
    if (ports.startsWith("[")) {
      ports = ports.substring(1);
    }
    if (ports.endsWith("]")) {
      ports = ports.substring(0, ports.length() - 1);
    }
    return ports;
  }

  public void setPORT(List<String> PORT) {
    this.PORT = PORT;
  }

  public String getUSERNAME() {
    return USERNAME;
  }

  public void setUSERNAME(String USERNAME) {
    this.USERNAME = USERNAME;
  }

  public String getPASSWORD() {
    return PASSWORD;
  }

  public void setPASSWORD(String PASSWORD) {
    this.PASSWORD = PASSWORD;
  }

  public String getDB_NAME() {
    return DB_NAME;
  }

  public void setDB_NAME(String DB_NAME) {
    this.DB_NAME = DB_NAME;
  }

  public String getTOKEN() {
    return TOKEN;
  }

  public void setTOKEN(String TOKEN) {
    this.TOKEN = TOKEN;
  }

  public String getMainConfig() {
    return "\n" + "  DB_SWITCH=" + DB_SWITCH + "\n" + "  HOST=" + HOST;
  }

  @Override
  public String toString() {
    return "\n"
        + "  DB_SWITCH="
        + DB_SWITCH
        + "\n"
        + "  HOST="
        + HOST
        + "\n"
        + "  PORT="
        + PORT
        + "\n"
        + "  USERNAME="
        + USERNAME
        + "\n"
        + "  PASSWORD="
        + PASSWORD
        + "\n"
        + "  DB_NAME="
        + DB_NAME
        + "\n"
        + "  TOKEN="
        + TOKEN;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof DBConfig)) {
      return false;
    }
    DBConfig c = (DBConfig) obj;
    return this.DB_SWITCH.equals(c.DB_SWITCH)
        && this.HOST.equals(c.HOST)
        && this.PORT.equals(c.PORT)
        && this.USERNAME.equals(c.USERNAME)
        && this.PASSWORD.equals(c.PASSWORD)
        && this.TOKEN.equals(c.TOKEN)
        && this.DB_NAME.equals(c.DB_NAME);
  }
}
