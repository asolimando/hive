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
import com.google.common.collect.ImmutableMap;
import javolution.testing.AssertionException;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;

public class StatisticsSerdeUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
  private static final ImmutableMap<String, Class<?>> TYPE_CLASS_MAP = new ImmutableMap.Builder<String, Class<?>>()
      .put("boolean", BooleanColumnStatsData.class)
      .put("string", StringColumnStatsData.class)
      .put("varchar", StringColumnStatsData.class)
      .put("char", StringColumnStatsData.class)
      .put("binary", BinaryColumnStatsData.class)
      .put("smallint", LongColumnStatsData.class)
      .put("int", LongColumnStatsData.class)
      .put("tinyint", LongColumnStatsData.class)
      .put("bigint", LongColumnStatsData.class)
      .put("double", DoubleColumnStatsData.class)
      .put("float", DoubleColumnStatsData.class)
      .put("decimal", DecimalColumnStatsData.class)
      .put("timestamp", TimestampColumnStatsData.class)
      .put("date", DateColumnStatsData.class)
      .build();

  private StatisticsSerdeUtils() {
    throw new AssertionException("StatisticsSerdeUtils util class should not be instantiated");
  }

  public static String serializeStatistics(AbstractColumnStats stats) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(stats);
  }

  public static String serializeBooleanStats(Long numTrues, Long numFalses, Long numNulls) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(
        org.apache.hadoop.hive.metastore.stastistics.ImmutableBooleanColumnStats.builder()
            .numTrues(numTrues)
            .numFalses(numFalses)
            .numNulls(numNulls)
            .build());
  }

  public static String serializeLongStats(Long numNulls, Long numNDVs, byte[] bitVector, Long lowValue, Long highValue)
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(
        org.apache.hadoop.hive.metastore.stastistics.ImmutableLongColumnStats.builder()
            .numNulls(numNulls)
            .numDVs(numNDVs)
            .bitVector(bitVector)
            .lowValue(lowValue)
            .highValue(highValue)
            .build());
  }

  public static String serializeDoubleStats(Long numNulls, Long numNDVs, byte[] bitVector, Double lowValue, Double highValue)
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(
        org.apache.hadoop.hive.metastore.stastistics.ImmutableDoubleColumnStats.builder()
            .numNulls(numNulls)
            .numDVs(numNDVs)
            .bitVector(bitVector)
            .lowValue(lowValue)
            .highValue(highValue)
            .build());
  }

  public static String serializeDecimalStats(Long numNulls, Long numNDVs, byte[] bitVector, String lowValue, String highValue)
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(
        org.apache.hadoop.hive.metastore.stastistics.ImmutableDecimalColumnStats.builder()
            .numNulls(numNulls)
            .numDVs(numNDVs)
            .bitVector(bitVector)
            .lowValue(lowValue)
            .highValue(highValue)
            .build());
  }

  public static String serializeStringStats(Long numNulls, Long numNDVs, byte[] bitVector, Long maxColLen, Double avgColLen)
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(
        org.apache.hadoop.hive.metastore.stastistics.ImmutableStringColumnStats.builder()
            .numNulls(numNulls)
            .numDVs(numNDVs)
            .bitVector(bitVector)
            .maxColLen(maxColLen)
            .avgColLen(avgColLen)
            .build());
  }

  public static String serializeBinaryStats(Long numNulls, Long maxColLen, Double avgColLen) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(
        org.apache.hadoop.hive.metastore.stastistics.ImmutableBinaryColumnStats.builder()
            .numNulls(numNulls)
            .maxColLen(maxColLen)
            .avgColLen(avgColLen)
            .build());
  }

  public static String serializeDateStats(Long numNulls, Long numNDVs, byte[] bitVector, Long lowValue, Long highValue)
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(
        org.apache.hadoop.hive.metastore.stastistics.ImmutableDateColumnStats.builder()
            .numNulls(numNulls)
            .numDVs(numNDVs)
            .bitVector(bitVector)
            .lowValue(lowValue)
            .highValue(highValue)
            .build());
  }

  public static String serializeTimestampStats(Long numNulls, Long numNDVs, byte[] bitVector, Long lowValue, Long highValue)
      throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(
        org.apache.hadoop.hive.metastore.stastistics.ImmutableTimestampColumnStats.builder()
            .numNulls(numNulls)
            .numDVs(numNDVs)
            .bitVector(bitVector)
            .lowValue(lowValue)
            .highValue(highValue)
            .build());
  }

  public static ColumnStatisticsData getColumnStatisticsData(String colType, String statistics) throws JsonProcessingException {
    return ((AbstractColumnStats) OBJECT_MAPPER.readValue(statistics, TYPE_CLASS_MAP.get(colType)))
        .getColumnStatsData();
  }
}
