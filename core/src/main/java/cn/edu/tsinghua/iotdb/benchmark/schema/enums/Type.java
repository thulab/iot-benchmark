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

package cn.edu.tsinghua.iotdb.benchmark.schema.enums;

public enum Type {
  BOOLEAN(1, "BOOLEAN"),
  INT32(2, "INT32"),
  INT64(3, "INT64"),
  FLOAT(4, "FLOAT"),
  DOUBLE(5, "DOUBLE"),
  TEXT(6, "TEXT");

  public int index;
  public String name;

  Type(int index, String name) {
    this.index = index;
    this.name = name;
  }

  public static Type[] getValueTypes() {
    Type type[] = new Type[4];
    for (int i = 1; i < 5; i++) {
      type[i - 1] = Type.values()[i];
    }
    return type;
  }

  public static Type getType(int index) {
    for (Type type : Type.values()) {
      if (type.index == index) {
        return type;
      }
    }
    // default type
    return Type.TEXT;
  }

  @Override
  public String toString() {
    return name;
  }
}
