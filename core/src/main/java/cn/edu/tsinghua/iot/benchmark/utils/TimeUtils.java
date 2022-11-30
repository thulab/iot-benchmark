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

package cn.edu.tsinghua.iot.benchmark.utils;

import org.joda.time.DateTime;

public class TimeUtils {

  public static long convertDateStrToTimestamp(String dateStr) {
    DateTime dateTime = new DateTime(dateStr);
    return dateTime.getMillis();
  }

  public static long getTimestampConst(String timePrecision) {
    if (timePrecision.equals("ms")) {
      return 1L;
    } else if (timePrecision.equals("us")) {
      return 1000L;
    } else {
      return 1000000L;
    }
  }

  public static double convertToSeconds(long time, String timePrecision) {
    return time * 1.0 / (getTimestampConst(timePrecision) * 1000);
  }
}
