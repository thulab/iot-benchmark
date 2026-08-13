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
package cn.edu.tsinghua.iot.benchmark.allinone;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * All-in-one 发行包体积优化（构建期后处理）。
 *
 * <p>各数据库模块的发行目录会各自带上 core 的全部依赖（fastutil 16MB 等在每个模块目录 重复一份，18 份总计约 490MB）。本工具把 lib/&lt;db&gt; 中与
 * lib/core **内容完全一致**的 jar 删除（按文件名 + 逐字节比较判定，同名不同版本的 jar 保留在模块目录）。zip 由 maven-antrun-plugin
 * 在去重后重建（见 all-in-one/pom.xml）。
 *
 * <p>运行时 classpath 顺序是「专属目录在前、lib/core 在后」（见 bin/startup.sh），所以删除 模块目录中与 core 一致的副本后，运行时从 lib/core
 * 加载同一份内容，行为等价。
 *
 * <p>用法（maven exec:java）：{@code DedupTool <lib 目录>}
 */
public final class DedupTool {

  private static final int BUFFER_SIZE = 64 * 1024;

  private DedupTool() {}

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("用法: DedupTool <lib 目录>");
      System.exit(1);
    }
    Path libDir = Paths.get(args[0]);

    Map<String, Path> coreJars = new HashMap<>();
    Path coreDir = libDir.resolve("core");
    try (DirectoryStream<Path> jars = Files.newDirectoryStream(coreDir, "*.jar")) {
      for (Path jar : jars) {
        coreJars.put(jar.getFileName().toString(), jar);
      }
    }
    System.out.println("[all-in-one dedup] lib/core: " + coreJars.size() + " jars");

    int removed = 0;
    long removedBytes = 0;
    try (DirectoryStream<Path> dirs = Files.newDirectoryStream(libDir)) {
      for (Path dir : dirs) {
        if (!Files.isDirectory(dir) || dir.equals(coreDir)) {
          continue;
        }
        try (DirectoryStream<Path> jars = Files.newDirectoryStream(dir, "*.jar")) {
          for (Path jar : jars) {
            Path coreJar = coreJars.get(jar.getFileName().toString());
            if (coreJar == null || !sameContent(jar, coreJar)) {
              continue;
            }
            long size = Files.size(jar);
            Files.delete(jar);
            removed++;
            removedBytes += size;
          }
        }
      }
    }
    System.out.printf(
        "[all-in-one dedup] removed %d jars (%d MB)%n", removed, removedBytes / 1024 / 1024);
  }

  /** 先比大小、再流式逐块比较，避免一次读入 16MB 级 jar。 */
  private static boolean sameContent(Path a, Path b) throws IOException {
    if (Files.size(a) != Files.size(b)) {
      return false;
    }
    byte[] bufA = new byte[BUFFER_SIZE];
    byte[] bufB = new byte[BUFFER_SIZE];
    try (InputStream inA = new BufferedInputStream(new FileInputStream(a.toFile()));
        InputStream inB = new BufferedInputStream(new FileInputStream(b.toFile()))) {
      while (true) {
        int n = inA.read(bufA);
        if (n < 0) {
          return true;
        }
        int off = 0;
        while (off < n) {
          int m = inB.read(bufB, off, n - off);
          if (m < 0) {
            return false;
          }
          off += m;
        }
        for (int i = 0; i < n; i++) {
          if (bufA[i] != bufB[i]) {
            return false;
          }
        }
      }
    }
  }
}
