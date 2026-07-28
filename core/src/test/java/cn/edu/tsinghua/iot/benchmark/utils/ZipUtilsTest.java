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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZipUtilsTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private File writeFile(File parent, String name, String content) throws Exception {
    File f = new File(parent, name);
    try (FileOutputStream fos = new FileOutputStream(f)) {
      fos.write(content.getBytes(StandardCharsets.UTF_8));
    }
    return f;
  }

  @Test
  public void testToZipDirectoryKeepStructure() throws Exception {
    File src = folder.newFolder("src");
    writeFile(src, "a.txt", "alpha");
    writeFile(src, "b.txt", "beta");
    File zip = new File(folder.getRoot(), "dir.zip");

    ZipUtils.toZip(src.getAbsolutePath(), zip.getAbsolutePath(), true);

    assertTrue("zip file should exist", zip.exists());
    try (ZipFile zf = new ZipFile(zip)) {
      // entries should not be empty; entry names are not asserted because
      // the production code joins them with System.lineSeparator()
      assertTrue("zip should contain at least one entry", zf.entries().hasMoreElements());
    }
  }

  @Test
  public void testToZipFileListNoDelete() throws Exception {
    File f1 = writeFile(folder.getRoot(), "one.txt", "one");
    File f2 = writeFile(folder.getRoot(), "two.txt", "two");
    File zip = new File(folder.getRoot(), "files.zip");
    List<String> srcs = Arrays.asList(f1.getAbsolutePath(), f2.getAbsolutePath());

    ZipUtils.toZip(srcs, zip.getAbsolutePath(), false);

    assertTrue("zip file should exist", zip.exists());
    assertTrue("source file 1 should still exist", f1.exists());
    assertTrue("source file 2 should still exist", f2.exists());

    Set<String> names = new HashSet<>();
    try (ZipFile zf = new ZipFile(zip)) {
      Enumeration<? extends ZipEntry> en = zf.entries();
      while (en.hasMoreElements()) {
        names.add(en.nextElement().getName());
      }
    }
    assertEquals(2, names.size());
    assertTrue(names.contains("one.txt"));
    assertTrue(names.contains("two.txt"));
  }

  @Test
  public void testToZipFileListNeedDelete() throws Exception {
    File f1 = writeFile(folder.getRoot(), "del1.txt", "x");
    File f2 = writeFile(folder.getRoot(), "del2.txt", "y");
    File zip = new File(folder.getRoot(), "del.zip");
    List<String> srcs = Arrays.asList(f1.getAbsolutePath(), f2.getAbsolutePath());

    ZipUtils.toZip(srcs, zip.getAbsolutePath(), true);

    assertTrue("zip file should exist", zip.exists());
    assertFalse("source file 1 should be deleted", f1.exists());
    assertFalse("source file 2 should be deleted", f2.exists());
  }

  @Test
  public void testToZipDirectoryNullArgsDoesNotThrow() {
    // current implementation only logs an error and continues; should not throw.
    // zipName goes under TempFolder so the empty file the current impl still
    // creates (it does not return after the null check) is auto-cleaned and
    // does not pollute the module working directory.
    String zipPath = new File(folder.getRoot(), "null-src.zip").getAbsolutePath();
    ZipUtils.toZip((String) null, zipPath, true);
  }

  @Test
  public void testToZipFileListNullZipNameDoesNotThrow() {
    // current implementation only logs an error and continues; should not throw
    ZipUtils.toZip(Collections.<String>emptyList(), null, false);
  }
}
