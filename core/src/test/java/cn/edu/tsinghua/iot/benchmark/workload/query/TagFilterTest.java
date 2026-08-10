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

package cn.edu.tsinghua.iot.benchmark.workload.query;

import cn.edu.tsinghua.iot.benchmark.workload.query.impl.PreciseQuery;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TagFilterTest {

  @Test
  public void testDefensiveCopyAndStableOrder() {
    List<String> values = new ArrayList<>(Arrays.asList("value_2", "value_10"));
    TagFilter tagFilter = new TagFilter("tag_1", values);
    values.set(0, "changed");

    assertEquals(Arrays.asList("value_2", "value_10"), tagFilter.getTagValues());
    assertThrows(
        UnsupportedOperationException.class, () -> tagFilter.getTagValues().add("value_11"));
  }

  @Test
  public void testRejectsInvalidPredicate() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new TagFilter(" ", Collections.singletonList("value_0")));
    assertThrows(IllegalArgumentException.class, () -> new TagFilter("tag_0", null));
    assertThrows(
        IllegalArgumentException.class, () -> new TagFilter("tag_0", Collections.emptyList()));
    assertThrows(
        IllegalArgumentException.class,
        () -> new TagFilter("tag_0", Arrays.asList("value_0", "value_0")));
  }

  @Test
  public void testSerializableAndLoggable() throws Exception {
    TagFilter tagFilter = new TagFilter("tag_2", Arrays.asList("value_1", "value_4", "value_9"));

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(tagFilter);
    }
    TagFilter restored;
    try (ObjectInputStream input =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      restored = (TagFilter) input.readObject();
    }
    assertEquals(tagFilter, restored);

    String serializedJson = JSON.toJSONString(tagFilter);
    JSONObject json = JSON.parseObject(serializedJson);
    assertEquals("tag_2", json.getString("tagKey"));
    assertEquals(Arrays.asList("value_1", "value_4", "value_9"), json.getJSONArray("tagValues"));
    assertEquals(tagFilter, JSON.parseObject(serializedJson, TagFilter.class));

    PreciseQuery query = new PreciseQuery(Collections.emptyList(), 1L);
    query.setTagFilter(tagFilter);
    assertTrue(query.getQueryAttrs().toString().contains(tagFilter.toString()));
  }
}
