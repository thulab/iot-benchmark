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

import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipUtils.class);

  private static final int BUFFER_SIZE = 2 * 1024;
  private static final int RETRY_TIME = 5;

  /**
   * compress Directory
   *
   * @param srcDir path of directory
   * @param zipName name of zip file
   * @param keepDirStructure whether maintain directory while zip
   */
  public static void toZip(String srcDir, String zipName, boolean keepDirStructure) {
    if (srcDir == null || zipName == null) {
      LOGGER.error("Failed to compress, because there are null.");
    }
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    ZipOutputStream zos = null;
    try {
      FileOutputStream out = new FileOutputStream(zipName);
      zos = new ZipOutputStream(out);
      File sourceFile = new File(srcDir);
      compress(sourceFile, zos, sourceFile.getName(), keepDirStructure);
      stopWatch.stop();
      LOGGER.info("Finish compress {}, use {} ms", srcDir, stopWatch.getTime());
    } catch (Exception e) {
      LOGGER.warn("Failed to compress " + srcDir + "\nexception: " + e);
    } finally {
      if (zos != null) {
        try {
          zos.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to compress " + srcDir + "\nexception: " + e);
        }
      }
    }
  }

  /**
   * compress files
   *
   * @param srcFiles the list of filenames
   * @param zipName name of zip file
   * @param needDelete whether to delete files
   */
  public static void toZip(List<String> srcFiles, String zipName, boolean needDelete) {
    if (srcFiles == null || zipName == null) {
      LOGGER.error("Failed to compress, because there are null.");
    }
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    ZipOutputStream zos = null;
    try {
      FileOutputStream out = new FileOutputStream(zipName);
      zos = new ZipOutputStream(out);
      for (String fileName : srcFiles) {
        if (fileName == null) {
          LOGGER.error("Failed to compress, because filename are null.");
        }
        File srcFile = new File(fileName);
        byte[] buf = new byte[BUFFER_SIZE];
        zos.putNextEntry(new ZipEntry(srcFile.getName()));
        int len;
        FileInputStream in = new FileInputStream(srcFile);
        while ((len = in.read(buf)) != -1) {
          zos.write(buf, 0, len);
        }
        zos.closeEntry();
        in.close();
        if (needDelete) {
          int tryTime = 0;
          while (tryTime < RETRY_TIME && !srcFile.delete()) {
            tryTime++;
            LOGGER.warn("Retry to delete file {}, retry time: {}.", srcFile.getName(), tryTime);
          }
        }
      }
      stopWatch.stop();
      LOGGER.info("Finish compress {}, use {} ms", srcFiles, stopWatch.getTime());
    } catch (Exception e) {
      LOGGER.warn("Failed to compress " + srcFiles + "\nexception: " + e);
    } finally {
      if (zos != null) {
        try {
          zos.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to compress " + srcFiles + "\nexception: " + e);
        }
      }
    }
  }

  /**
   * compress file
   *
   * @param sourceFile the source file
   * @param zos the output stream of zip
   * @param name the filename after compressed
   * @param keepDirStructure whether maintain directory while zip
   */
  private static void compress(
      File sourceFile, ZipOutputStream zos, String name, boolean keepDirStructure)
      throws Exception {
    byte[] buf = new byte[BUFFER_SIZE];
    if (sourceFile.isFile()) {
      zos.putNextEntry(new ZipEntry(name));
      int len;
      FileInputStream in = new FileInputStream(sourceFile);
      while ((len = in.read(buf)) != -1) {
        zos.write(buf, 0, len);
      }
      zos.closeEntry();
      in.close();
    } else {
      File[] listFiles = sourceFile.listFiles();
      if (listFiles == null || listFiles.length == 0) {
        if (keepDirStructure) {
          zos.putNextEntry(new ZipEntry(name + System.lineSeparator()));
          zos.closeEntry();
        }
      } else {
        for (File file : listFiles) {
          if (keepDirStructure) {
            compress(file, zos, name + System.lineSeparator() + file.getName(), keepDirStructure);
          } else {
            compress(file, zos, file.getName(), keepDirStructure);
          }
        }
      }
    }
  }
}
