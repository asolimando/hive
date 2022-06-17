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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.stastistics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public interface OrderingColumnStats<T extends Comparable<T>> extends ColumnStats, NDVColumnStats {

  @JsonProperty("lowValue")
  Optional<T> lowValue();

  @JsonProperty("highValue")
  Optional<T> highValue();

  @JsonIgnore
  default Optional<T> mergeLowValues (OrderingColumnStats<T> o) {
    return Stream.of(lowValue(), o.lowValue())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .min(T::compareTo);
  }

  @JsonIgnore
  default Optional<T> mergeHighValues(OrderingColumnStats<T> o) {
    return Stream.of(highValue(), o.highValue())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .max(T::compareTo);
  }
}
