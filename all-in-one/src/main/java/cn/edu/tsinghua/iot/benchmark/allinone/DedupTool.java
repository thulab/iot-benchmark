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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * All-in-one 发行包体积优化（构建期后处理）。
 *
 * <p>各数据库模块的发行目录会各自带上 core 的全部依赖（fastutil 16MB 等在每个模块目录 重复一份，18 份总计约 490MB）。本工具对 lib/&lt;db&gt;
 * 做两步收敛（判定均为文件名 + 逐字节比较，同名不同版本的 jar 保留）：
 *
 * <ol>
 *   <li><b>去重</b>：删除与 lib/core 同名且内容完全一致的 jar（core 与模块目录各保留一份）。
 *   <li><b>提升</b>：lib/core 没有、但出现在 &gt;=2 个模块目录、且各副本逐字节一致的 jar， 提升一份到 lib/core 并删除各模块目录副本。
 * </ol>
 *
 * <p>运行时 classpath 顺序是「专属目录在前、lib/core 在后」（见 bin/startup.sh），收敛后 运行时从 lib/core 加载同一份内容，行为等价。
 *
 * <p>日志生态 jar（slf4j-*、logback-*、log4j-*、reload4j-* 前缀）不参与提升：绑定由 classpath 目录顺序决定「哪个目录在前哪个绑定生效」，提升进
 * core 会与 core 的唯一绑定 （slf4j-reload4j）同目录并存，JVM 通配符展开顺序未指定 → 绑定随机化。
 *
 * <p>zip 由 maven-antrun-plugin 在两步之后重建（见 all-in-one/pom.xml）。
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
    Path coreDir = libDir.resolve("core");

    Map<String, Path> coreJars = new HashMap<>();
    try (DirectoryStream<Path> jars = Files.newDirectoryStream(coreDir, "*.jar")) {
      for (Path jar : jars) {
        coreJars.put(jar.getFileName().toString(), jar);
      }
    }
    System.out.println("[all-in-one dedup] lib/core: " + coreJars.size() + " jars");

    // name -> 各模块目录中的副本（已存在的，随删除实时移除）
    Map<String, List<Path>> moduleJars = new HashMap<>();
    try (DirectoryStream<Path> dirs = Files.newDirectoryStream(libDir)) {
      for (Path dir : dirs) {
        if (!Files.isDirectory(dir) || dir.equals(coreDir)) {
          continue;
        }
        try (DirectoryStream<Path> jars = Files.newDirectoryStream(dir, "*.jar")) {
          for (Path jar : jars) {
            moduleJars
                .computeIfAbsent(jar.getFileName().toString(), k -> new ArrayList<>())
                .add(jar);
          }
        }
      }
    }

    // 第一步：去重——删除与 lib/core 同名且逐字节一致的模块目录副本
    int removed = 0;
    long removedBytes = 0;
    for (Map.Entry<String, List<Path>> entry : moduleJars.entrySet()) {
      Path coreJar = coreJars.get(entry.getKey());
      if (coreJar == null) {
        continue;
      }
      Iterator<Path> it = entry.getValue().iterator();
      while (it.hasNext()) {
        Path jar = it.next();
        if (!sameContent(jar, coreJar)) {
          continue;
        }
        long size = Files.size(jar);
        Files.delete(jar);
        it.remove();
        removed++;
        removedBytes += size;
      }
    }
    System.out.printf(
        "[all-in-one dedup] step1 removed %d jars (%d MB)%n", removed, removedBytes / 1024 / 1024);

    // 第二步：提升——core 没有、>=2 个模块目录同名且各副本逐字节一致、且非日志生态
    // 的 jar，复制一份到 lib/core 并删除各模块目录副本
    int promoted = 0;
    long promotedBytes = 0;
    for (Map.Entry<String, List<Path>> entry : moduleJars.entrySet()) {
      String name = entry.getKey();
      List<Path> copies = entry.getValue();
      if (coreJars.containsKey(name) || copies.size() < 2 || isLoggingJar(name)) {
        continue;
      }
      boolean allSame = true;
      for (int i = 1; i < copies.size(); i++) {
        if (!sameContent(copies.get(0), copies.get(i))) {
          allSame = false;
          break;
        }
      }
      if (!allSame) {
        System.out.println("[all-in-one dedup] step2 skip 同名不同版本: " + name);
        continue;
      }
      long size = Files.size(copies.get(0));
      Files.copy(copies.get(0), coreDir.resolve(name));
      for (Path jar : copies) {
        Files.delete(jar);
      }
      promoted++;
      promotedBytes += size * (copies.size() - 1);
    }
    System.out.printf(
        "[all-in-one dedup] step2 promoted %d jars to lib/core (saved %d MB)%n",
        promoted, promotedBytes / 1024 / 1024);
  }

  /** 日志生态 jar 不参与提升：绑定随 classpath 目录顺序生效，提升会破坏绑定确定性。 */
  private static boolean isLoggingJar(String name) {
    return name.startsWith("slf4j-")
        || name.startsWith("logback-")
        || name.startsWith("log4j-")
        || name.startsWith("reload4j-");
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
