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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** A tag-column predicate shared by query generation and database adapters. */
public final class TagFilter implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String tagKey;
  private final List<String> tagValues;

  /**
   * Creates a tag filter whose value order is retained when rendered by a database adapter.
   *
   * @param tagKey non-blank tag column name
   * @param tagValues non-empty, duplicate-free list of non-null tag values
   */
  public TagFilter(String tagKey, List<String> tagValues) {
    if (tagKey == null || tagKey.trim().isEmpty()) {
      throw new IllegalArgumentException("tagKey must not be blank");
    }
    if (tagValues == null || tagValues.isEmpty()) {
      throw new IllegalArgumentException("tagValues must not be empty");
    }
    Set<String> distinctValues = new HashSet<>();
    for (String tagValue : tagValues) {
      if (tagValue == null) {
        throw new IllegalArgumentException("tagValues must not contain null");
      }
      if (!distinctValues.add(tagValue)) {
        throw new IllegalArgumentException("tagValues must not contain duplicates");
      }
    }
    this.tagKey = tagKey;
    this.tagValues = Collections.unmodifiableList(new ArrayList<>(tagValues));
  }

  public String getTagKey() {
    return tagKey;
  }

  public List<String> getTagValues() {
    return tagValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TagFilter)) {
      return false;
    }
    TagFilter tagFilter = (TagFilter) o;
    return tagKey.equals(tagFilter.tagKey) && tagValues.equals(tagFilter.tagValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tagKey, tagValues);
  }

  @Override
  public String toString() {
    return "TagFilter{" + "tagKey='" + tagKey + '\'' + ", tagValues=" + tagValues + '}';
  }
}
