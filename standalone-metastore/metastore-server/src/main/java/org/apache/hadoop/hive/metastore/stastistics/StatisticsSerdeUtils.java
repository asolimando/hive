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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import javolution.testing.AssertionException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;

public class StatisticsSerdeUtils {
  @VisibleForTesting
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

  // fully qualified class names to make the IDE happy
  public static final ImmutableMap<String, Class<? extends AbstractColumnStats>> COL_TYPE_STATS_CLASS_MAP
      = new ImmutableMap.Builder<String, Class<? extends AbstractColumnStats>>()
      .put("boolean", org.apache.hadoop.hive.metastore.stastistics.BooleanColumnStats.class)
      .put("string", org.apache.hadoop.hive.metastore.stastistics.StringColumnStats.class)
      .put("varchar", org.apache.hadoop.hive.metastore.stastistics.StringColumnStats.class)
      .put("char", org.apache.hadoop.hive.metastore.stastistics.StringColumnStats.class)
      .put("binary", org.apache.hadoop.hive.metastore.stastistics.BinaryColumnStats.class)
      .put("smallint", org.apache.hadoop.hive.metastore.stastistics.LongColumnStats.class)
      .put("int", org.apache.hadoop.hive.metastore.stastistics.LongColumnStats.class)
      .put("tinyint", org.apache.hadoop.hive.metastore.stastistics.LongColumnStats.class)
      .put("bigint", org.apache.hadoop.hive.metastore.stastistics.LongColumnStats.class)
      .put("double", org.apache.hadoop.hive.metastore.stastistics.DoubleColumnStats.class)
      .put("float", org.apache.hadoop.hive.metastore.stastistics.DoubleColumnStats.class)
      .put("decimal", org.apache.hadoop.hive.metastore.stastistics.DecimalColumnStats.class)
      .put("timestamp", org.apache.hadoop.hive.metastore.stastistics.TimestampColumnStats.class)
      .put("date", org.apache.hadoop.hive.metastore.stastistics.DateColumnStats.class)
      .build();

  private StatisticsSerdeUtils() {
    throw new AssertionException("StatisticsSerdeUtils util class should not be instantiated");
  }

  public static String serializeStatistics(AbstractColumnStats stats) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(stats);
  }

  public static ColumnStatisticsData getColumnStatisticsData(String colType, String statistics) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(statistics, getStatsClassFromColumnType(colType)).getColumnStatsData();
  }

  @SuppressWarnings("unchecked")
  public static <T extends AbstractColumnStats> T deserializeStatistics(
      String colType, String statistics) throws JsonProcessingException {
    return (T) deserializeStatistics(getStatsClassFromColumnType(colType), statistics);
  }

  public static <T extends AbstractColumnStats> T deserializeStatistics(
      Class<T> clazz, String statistics) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(statistics, clazz);
  }

  private static Class<? extends AbstractColumnStats> getStatsClassFromColumnType(String colType) {
    if (!COL_TYPE_STATS_CLASS_MAP.containsKey(colType)) {
      throw new IllegalArgumentException("Unknown column type " + colType);
    }
    return COL_TYPE_STATS_CLASS_MAP.get(colType);
  }
}
