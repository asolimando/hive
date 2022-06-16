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
package org.apache.hadoop.hive.metastore.columnstats.aggr;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.stastistics.AbstractColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.StatisticsSerdeUtils;
import org.apache.hadoop.hive.metastore.stastistics.StringColumnStats;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Category(MetastoreUnitTest.class)
public class StringColumnStatsAggregatorTest {

  private static final Table TABLE = new Table("dummy", "db", "hive", 0, 0,
      0, null, null, Collections.emptyMap(), null, null,
      TableType.MANAGED_TABLE.toString());
  private static final FieldSchema COL = new FieldSchema("col", "string", "");

  private static final String S_1 = "test";
  private static final String S_2 = "try";
  private static final String S_3 = "longer string";
  private static final String S_4 = "even longer string";
  private static final String S_5 = "some string";
  private static final String S_6 = "some other string";
  private static final String S_7 = "yet another string";

  @Test
  public void testAggregateSingleStat() throws MetaException, JsonProcessingException {
    List<String> partitionNames = Collections.singletonList("part1");

    AbstractColumnStats stats = StringColumnStats.builder()
        .numNulls(1)
        .numDVs(2)
        .avgColLen(8.5)
        .maxColLen(13)
        .bitVector(StatisticsTestUtils.createHll(S_1, S_3).serialize())
        .build();
    ColumnStatisticsData data1 = new ColStatsBuilder().numNulls(1).numDVs(2).avgColLen(8.5).maxColLen(13)
        .hll(S_1, S_3).buildStringStats();

    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(
        data1, StatisticsSerdeUtils.serializeStatistics(stats), TABLE, COL, partitionNames.get(0));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Collections.singletonList(stats1));

    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);

    Assert.assertEquals(stats,
        StatisticsSerdeUtils.deserializeStatistics(StringColumnStats.class, computedStatsObj.getStatistics()));
  }

  @Test
  public void testAggregateMultiStatsWhenAllAvailable() throws MetaException, JsonProcessingException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    AbstractColumnStats s1 = StringColumnStats.builder()
        .numNulls(1)
        .numDVs(3)
        .avgColLen(20.0 / 3)
        .maxColLen(13)
        .bitVector(StatisticsTestUtils.createHll(S_1, S_2, S_3).serialize())
        .build();
    ColumnStatisticsData data1 = new ColStatsBuilder().numNulls(1).numDVs(3).avgColLen(20.0 / 3).maxColLen(13)
        .hll(S_1, S_2, S_3).buildStringStats();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(
        data1, StatisticsSerdeUtils.serializeStatistics(s1), TABLE, COL, partitionNames.get(0));

    AbstractColumnStats s2 = StringColumnStats.builder()
        .numNulls(2)
        .numDVs(3)
        .avgColLen(14)
        .maxColLen(18)
        .bitVector(StatisticsTestUtils.createHll(S_3, S_4, S_5).serialize())
        .build();
    ColumnStatisticsData data2 = new ColStatsBuilder().numNulls(2).numDVs(3).avgColLen(14).maxColLen(18)
        .hll(S_3, S_4, S_5).buildStringStats();
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(
        data2, StatisticsSerdeUtils.serializeStatistics(s2), TABLE, COL, partitionNames.get(1));

    AbstractColumnStats s3 = StringColumnStats.builder()
        .numNulls(3)
        .numDVs(2)
        .avgColLen(17.5)
        .maxColLen(18)
        .bitVector(StatisticsTestUtils.createHll(S_6, S_7).serialize())
        .build();
    ColumnStatisticsData data3 = new ColStatsBuilder().numNulls(3).numDVs(2).avgColLen(17.5).maxColLen(18)
        .hll(S_6, S_7).buildStringStats();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(
        data3, StatisticsSerdeUtils.serializeStatistics(s3), TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, stats2, stats3));

    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();
    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);

    HyperLogLog mergedHLL = StatisticsTestUtils.createHll(S_1, S_2, S_3);
    mergedHLL.merge(StatisticsTestUtils.createHll(S_3, S_4, S_5));
    mergedHLL.merge(StatisticsTestUtils.createHll(S_6, S_7));
    StringColumnStats expectedStats = StringColumnStats.builder().numNulls(6).numDVs(7).avgColLen(17.5).maxColLen(18)
        .bitVector(mergedHLL.serialize()).build();

    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));
  }

  @Test
  public void testAggregateMultiStatsWhenUnmergeableBitVectors() throws MetaException, JsonProcessingException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    FMSketch fmSketch1 = StatisticsTestUtils.createFMSketch(S_1, S_2, S_3);
    AbstractColumnStats s1 = StringColumnStats.builder()
        .numNulls(1)
        .numDVs(3)
        .avgColLen(20.0 / 3)
        .maxColLen(13)
        .bitVector(fmSketch1.serialize())
        .build();
    System.out.println("FM: " + fmSketch1.estimateNumDistinctValues());
    ColumnStatisticsData data1 = new ColStatsBuilder().numNulls(1).numDVs(3).avgColLen(20.0 / 3).maxColLen(13)
        .fmSketch(S_1, S_2, S_3).buildStringStats();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(
        data1, StatisticsSerdeUtils.serializeStatistics(s1), TABLE, COL, partitionNames.get(0));

    AbstractColumnStats s2 = StringColumnStats.builder()
        .numNulls(2)
        .numDVs(3)
        .avgColLen(14)
        .maxColLen(18)
        .bitVector(StatisticsTestUtils.createHll(S_3, S_4, S_5).serialize())
        .build();
    ColumnStatisticsData data2 = new ColStatsBuilder().numNulls(2).numDVs(3).avgColLen(14).maxColLen(18)
        .hll(S_3, S_4, S_5).buildStringStats();
    ColumnStatistics stats2 = StatisticsTestUtils.createColStats(
        data2, StatisticsSerdeUtils.serializeStatistics(s2), TABLE, COL, partitionNames.get(1));

    AbstractColumnStats s3 = StringColumnStats.builder()
        .numNulls(3)
        .numDVs(2)
        .avgColLen(17.5)
        .maxColLen(18)
        .bitVector(StatisticsTestUtils.createHll(S_6, S_7).serialize())
        .build();
    ColumnStatisticsData data3 = new ColStatsBuilder().numNulls(3).numDVs(2).avgColLen(17.5).maxColLen(18)
        .hll(S_6, S_7).buildStringStats();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(
        data3, StatisticsSerdeUtils.serializeStatistics(s3), TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, stats2, stats3));

    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    // the aggregation does not update the bitvector, only numDVs is, it keeps the first bitvector;
    // numDVs is set to the maximum among all stats when non-mergeable bitvectors are detected
    StringColumnStats expectedStats = StringColumnStats.builder().numNulls(6).numDVs(3).avgColLen(17.5).maxColLen(18)
        .bitVector(fmSketch1.serialize()).build();

    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));

    // both useDensityFunctionForNDVEstimation and ndvTuner are ignored by StringColumnStatsAggregator
    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));

    aggregator.useDensityFunctionForNDVEstimation = false;
    aggregator.ndvTuner = 0;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));

    aggregator.ndvTuner = 0.5;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));

    aggregator.ndvTuner = 0.75;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));

    aggregator.ndvTuner = 1;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, true);
    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));
  }

  @Test
  public void testAggregateMultiStatsWhenOnlySomeAvailable() throws MetaException, JsonProcessingException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3", "part4");

    AbstractColumnStats s1 = StringColumnStats.builder()
        .numNulls(1)
        .numDVs(3)
        .avgColLen(20.0 / 3)
        .maxColLen(13)
        .bitVector(StatisticsTestUtils.createHll(S_1, S_2, S_3).serialize())
        .build();
    ColumnStatisticsData data1 = new ColStatsBuilder().numNulls(1).numDVs(3).avgColLen(20.0 / 3).maxColLen(13)
        .hll(S_1, S_2, S_3).buildStringStats();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(
        data1, StatisticsSerdeUtils.serializeStatistics(s1), TABLE, COL, partitionNames.get(0));

    AbstractColumnStats s3 = StringColumnStats.builder()
        .numNulls(3)
        .numDVs(2)
        .avgColLen(17.5)
        .maxColLen(18)
        .bitVector(StatisticsTestUtils.createHll(S_6, S_7).serialize())
        .build();
    ColumnStatisticsData data3 = new ColStatsBuilder().numNulls(3).numDVs(2).avgColLen(17.5).maxColLen(18)
        .hll(S_6, S_7).buildStringStats();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(
        data3, StatisticsSerdeUtils.serializeStatistics(s3), TABLE, COL, partitionNames.get(2));

    AbstractColumnStats s4 = StringColumnStats.builder()
        .numNulls(2)
        .numDVs(3)
        .avgColLen(14)
        .maxColLen(18)
        .bitVector(StatisticsTestUtils.createHll(S_3, S_4, S_5).serialize())
        .build();
    ColumnStatisticsData data4 = new ColStatsBuilder().numNulls(2).numDVs(3).avgColLen(14).maxColLen(18)
        .hll(S_3, S_4, S_5).buildStringStats();
    ColumnStatistics stats4 = StatisticsTestUtils.createColStats(
        data4, StatisticsSerdeUtils.serializeStatistics(s4), TABLE, COL, partitionNames.get(3));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, null, stats3, stats4), Arrays.asList(0, 2, 3));

    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    StringColumnStats expectedStats = StringColumnStats.builder().numNulls(8).numDVs(6)
        .avgColLen(24.0).maxColLen(24).build();

    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));
  }

  @Test
  public void testAggregateMultiStatsOnlySomeAvailableButUnmergeableBitVector()
      throws MetaException, JsonProcessingException {
    List<String> partitionNames = Arrays.asList("part1", "part2", "part3");

    AbstractColumnStats s1 = StringColumnStats.builder()
        .numNulls(1)
        .numDVs(3)
        .avgColLen(20.0 / 3)
        .maxColLen(13)
        .bitVector(StatisticsTestUtils.createFMSketch(S_1, S_2, S_3).serialize())
        .build();
    ColumnStatisticsData data1 = new ColStatsBuilder().numNulls(1).numDVs(3).avgColLen(20.0 / 3).maxColLen(13)
        .fmSketch(S_1, S_2, S_3).buildStringStats();
    ColumnStatistics stats1 = StatisticsTestUtils.createColStats(
        data1, StatisticsSerdeUtils.serializeStatistics(s1), TABLE, COL, partitionNames.get(0));

    AbstractColumnStats s3 = StringColumnStats.builder()
        .numNulls(3)
        .numDVs(2)
        .avgColLen(17.5)
        .maxColLen(18)
        .bitVector(StatisticsTestUtils.createHll(S_6, S_7).serialize())
        .build();
    ColumnStatisticsData data3 = new ColStatsBuilder().numNulls(3).numDVs(2).avgColLen(17.5).maxColLen(18)
        .hll(S_6, S_7).buildStringStats();
    ColumnStatistics stats3 = StatisticsTestUtils.createColStats(
        data3, StatisticsSerdeUtils.serializeStatistics(s3), TABLE, COL, partitionNames.get(2));

    List<ColStatsObjWithSourceInfo> statsList = StatisticsTestUtils.createColStatsObjWithSourceInfoList(
        TABLE, partitionNames, Arrays.asList(stats1, null, stats3), Arrays.asList(0, 2));

    StringColumnStatsAggregator aggregator = new StringColumnStatsAggregator();

    ColumnStatisticsObj computedStatsObj = aggregator.aggregate(statsList, partitionNames, false);
    // hll in case of missing stats is left as null, only numDVs is updated
    StringColumnStats expectedStats = StringColumnStats.builder().numNulls(6).numDVs(3)
        .avgColLen(22.916666666666668).maxColLen(22).build();
    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));

    // both useDensityFunctionForNDVEstimation and ndvTuner are ignored by StringColumnStatsAggregator
    aggregator.useDensityFunctionForNDVEstimation = true;
    computedStatsObj = aggregator.aggregate(statsList, partitionNames, false);
    Assert.assertEquals(expectedStats, StatisticsSerdeUtils.deserializeStatistics(
        StringColumnStats.class, computedStatsObj.getStatistics()));

  }
}
