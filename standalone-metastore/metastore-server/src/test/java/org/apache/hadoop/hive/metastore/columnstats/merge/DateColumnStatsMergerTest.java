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

package org.apache.hadoop.hive.metastore.columnstats.merge;

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.stastistics.DateColumnStats;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class DateColumnStatsMergerTest {

  private static final Date DATE_1 = new Date(1);
  private static final Date DATE_2 = new Date(2);
  private static final Date DATE_3 = new Date(3);

  @Test
  public void testMergeNullLowHighValues() {
    DateColumnStats oldStats = DateColumnStats.builder()
        .numNulls(1)
        .numDVs(2)
        .build();
    DateColumnStats expectedStats = DateColumnStats.builder()
        .numNulls(2)
        .numDVs(2)
        .build();

    Assert.assertEquals(expectedStats, oldStats.merge(oldStats));
  }

  @Test
  public void testMergeNullsWhenLowHighNotNulls() {
    DateColumnStats statsNoHighLow = DateColumnStats.builder()
        .numNulls(1)
        .numDVs(2)
        .build();
    DateColumnStats lowHighStats = statsNoHighLow.withLowValue(DATE_1.getDaysSinceEpoch())
        .withHighValue(DATE_3.getDaysSinceEpoch());

    DateColumnStats mergedStats = (DateColumnStats) statsNoHighLow.merge(lowHighStats);
    DateColumnStats expectedStats = DateColumnStats.builder()
        .numNulls(2)
        .numDVs(2)
        .lowValue(DATE_1.getDaysSinceEpoch())
        .highValue(DATE_3.getDaysSinceEpoch())
        .build();
    Assert.assertEquals(expectedStats, mergedStats);

    expectedStats = expectedStats.withNumNulls(3);
    // we check here that merging with null won't delete them once set
    Assert.assertEquals(expectedStats, mergedStats.merge(statsNoHighLow));
  }

  @Test
  public void testMergeUpdatesLow() {
    DateColumnStats statsLowHigh2 = DateColumnStats.builder()
        .numNulls(0)
        .numDVs(1)
        .lowValue(DATE_2.getDaysSinceEpoch())
        .highValue(DATE_2.getDaysSinceEpoch())
        .build();
    DateColumnStats statsLowHigh3 = DateColumnStats.builder()
        .numNulls(0)
        .numDVs(1)
        .lowValue(DATE_3.getDaysSinceEpoch())
        .highValue(DATE_3.getDaysSinceEpoch())
        .build();
    DateColumnStats mergedStats = (DateColumnStats) statsLowHigh2.merge(statsLowHigh3);

    DateColumnStats statsLowHigh1 = DateColumnStats.builder()
        .numNulls(0)
        .numDVs(1)
        .lowValue(DATE_1.getDaysSinceEpoch())
        .highValue(DATE_1.getDaysSinceEpoch())
        .build();
    mergedStats = (DateColumnStats) mergedStats.merge(statsLowHigh1);

    DateColumnStats expectedStats = statsLowHigh1.withHighValue(DATE_3.getDaysSinceEpoch());
    Assert.assertEquals(expectedStats, mergedStats);
  }

  @Test
  public void testMergeUpdatesHigh() {
    DateColumnStats statsLowHigh1 = DateColumnStats.builder()
        .numNulls(0)
        .numDVs(1)
        .lowValue(DATE_1.getDaysSinceEpoch())
        .highValue(DATE_1.getDaysSinceEpoch())
        .build();
    DateColumnStats statsLowHigh2 = DateColumnStats.builder()
        .numNulls(0)
        .numDVs(1)
        .lowValue(DATE_2.getDaysSinceEpoch())
        .highValue(DATE_2.getDaysSinceEpoch())
        .build();
    DateColumnStats mergedStats = (DateColumnStats) statsLowHigh1.merge(statsLowHigh2);

    DateColumnStats statsLowHigh3 = DateColumnStats.builder()
        .numNulls(0)
        .numDVs(1)
        .lowValue(DATE_3.getDaysSinceEpoch())
        .highValue(DATE_3.getDaysSinceEpoch())
        .build();
    mergedStats = (DateColumnStats) mergedStats.merge(statsLowHigh3);

    DateColumnStats expectedStats = statsLowHigh1.withHighValue(DATE_3.getDaysSinceEpoch());
    Assert.assertEquals(expectedStats, mergedStats);
  }

  @Test
  public void testMergeAssociativity() {
    DateColumnStats statsLowHigh2 = DateColumnStats.builder()
        .numNulls(2)
        .numDVs(5)
        .lowValue(DATE_2.getDaysSinceEpoch())
        .highValue(DATE_2.getDaysSinceEpoch())
        .build();
    DateColumnStats statsLowHigh3 = DateColumnStats.builder()
        .numNulls(3)
        .numDVs(1)
        .lowValue(DATE_3.getDaysSinceEpoch())
        .highValue(DATE_3.getDaysSinceEpoch())
        .build();
    Assert.assertEquals(statsLowHigh2.merge(statsLowHigh3), statsLowHigh3.merge(statsLowHigh2));
  }
}
