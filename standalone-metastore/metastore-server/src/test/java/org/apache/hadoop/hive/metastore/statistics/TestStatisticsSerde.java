/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.statistics;

import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.stastistics.DoubleColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.StatisticsSerdeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(MetastoreUnitTest.class)
public class TestStatisticsSerde {

  private static final HyperLogLog HLL = HyperLogLog.builder().build();
  static {
    HLL.addLong(1);
  }

  private static final DoubleColumnStats DOUBLE_STATS = DoubleColumnStats.builder()
      .numNulls(0)
      .numDVs(1)
      .bitVector(HLL.serialize())
      .lowValue(1.0)
      .highValue(1.0)
      .build();
  private static final DoubleColumnStats DOUBLE_STATS_PARTIAL = DoubleColumnStats.builder()
      .numNulls(0)
      .numDVs(1)
      .build();
  private static final String DOUBLE_STATS_JSON =
      "{\"numNulls\":0,\"numDVs\":1,\"bitVector\":\"SExM4AEBxfO+SA==\",\"lowValue\":1.0,\"highValue\":1.0}";
  private static final String DOUBLE_STATS_JSON_UNKNOWN_PROPERTY =
      "{\"numNulls\":0,\"numDVs\":1,\"bitVector\":\"SExM4AEBxfO+SA==\",\"lowValue\":1.0,\"highValue\":1.0,\"unknown\":1}";
  private static final String DOUBLE_STATS_JSON_PARTIAL = "{\"numNulls\":0,\"numDVs\":1}";

  @Test
  public void testDoubleSerialization() {
    Assert.assertEquals(DOUBLE_STATS_JSON, StatisticsSerdeUtils.serializeStatistics(DOUBLE_STATS));
  }

  @Test
  public void testDoubleDeserialization() {
    Assert.assertEquals(DOUBLE_STATS, StatisticsSerdeUtils.deserializeStatistics("double", DOUBLE_STATS_JSON));
  }

  @Test
  public void testDoubleDeserializationUnknownPropertyOK() {
    Assert.assertEquals(
        DOUBLE_STATS, StatisticsSerdeUtils.deserializeStatistics("double", DOUBLE_STATS_JSON_UNKNOWN_PROPERTY));
  }

  @Test
  public void testDoubleDeserializationPartialOK() {
    Assert.assertEquals(
        DOUBLE_STATS_PARTIAL, StatisticsSerdeUtils.deserializeStatistics("double", DOUBLE_STATS_JSON_PARTIAL));
  }
}
