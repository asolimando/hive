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

import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.stastistics.DecimalColumnStats;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class DecimalColumnStatsMergerTest {

  private static final String DECIMAL_3 = DecimalUtils.createJdoDecimalString(DecimalUtils.getDecimal(3, 0));
  private static final String DECIMAL_5 = DecimalUtils.createJdoDecimalString(DecimalUtils.getDecimal(5, 0));
  private static final String DECIMAL_20 = DecimalUtils.createJdoDecimalString(DecimalUtils.getDecimal(2, 1));
  private static final String DECIMAL_30 = DecimalUtils.createJdoDecimalString(DecimalUtils.getDecimal(3, 1));

  @Test
  public void testMergeNullValues() {
    DecimalColumnStats oldStats = DecimalColumnStats.builder()
        .numNulls(0)
        .numDVs(2)
        .build();

    Assert.assertEquals(oldStats, oldStats.merge(oldStats));
  }

  @Test
  public void testMergeNonNullAndNullValuesMergedIsNonNull() {
    DecimalColumnStats stats1 = DecimalColumnStats.builder()
        .highValue(DECIMAL_5)
        .numNulls(0)
        .numDVs(1)
        .build();
    DecimalColumnStats stats2 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .numNulls(0)
        .numDVs(1)
        .build();
    DecimalColumnStats expectedStats = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .highValue(DECIMAL_5)
        .numNulls(0)
        .numDVs(1)
        .build();

    Assert.assertEquals(stats1.merge(stats2), stats2.merge(stats1));
    Assert.assertEquals(expectedStats, stats1.merge(stats2));
  }

  @Test
  public void testMergeNonNullAndNullHigherValuesIsNonNull() {
    DecimalColumnStats stats1 = DecimalColumnStats.builder()
        .numNulls(0)
        .numDVs(2)
        .build();
    DecimalColumnStats stats2 = DecimalColumnStats.builder()
        .highValue(DECIMAL_3)
        .numNulls(0)
        .numDVs(2)
        .build();

    Assert.assertEquals(stats1.merge(stats2), stats2.merge(stats1));
    Assert.assertEquals(stats2, stats1.merge(stats2));
  }

  @Test
  public void testMergeNonNullUnscaledValues() {
    DecimalColumnStats stats1 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .highValue(DECIMAL_3)
        .numNulls(0)
        .numDVs(1)
        .build();
    DecimalColumnStats stats2 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_5)
        .highValue(DECIMAL_5)
        .numNulls(0)
        .numDVs(1)
        .build();
    DecimalColumnStats expectedStats = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .highValue(DECIMAL_5)
        .numNulls(0)
        .numDVs(1)
        .build();

    Assert.assertEquals(stats1.merge(stats2), stats2.merge(stats1));
    Assert.assertEquals(expectedStats, stats1.merge(stats2));
  }

  @Test
  public void testMergeNonNullScaledValues() {
    DecimalColumnStats stats1 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_20)
        .highValue(DECIMAL_20)
        .numNulls(1)
        .numDVs(2)
        .build();
    DecimalColumnStats stats2 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_30)
        .highValue(DECIMAL_30)
        .numNulls(1)
        .numDVs(2)
        .build();
    DecimalColumnStats expectedStats = DecimalColumnStats.builder()
        .lowValue(DECIMAL_20)
        .highValue(DECIMAL_30)
        .numNulls(2)
        .numDVs(2)
        .build();

    Assert.assertEquals(stats1.merge(stats2), stats2.merge(stats1));
    Assert.assertEquals(expectedStats, stats1.merge(stats2));
  }

  @Test
  public void testMergeNonNullMixedScaleValues() {
    DecimalColumnStats stats1 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .highValue(DECIMAL_20)
        .numNulls(1)
        .numDVs(2)
        .build();
    DecimalColumnStats stats2 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_30)
        .highValue(DECIMAL_5)
        .numNulls(1)
        .numDVs(2)
        .build();
    DecimalColumnStats expectedStats = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .highValue(DECIMAL_20)
        .numNulls(2)
        .numDVs(2)
        .build();

    Assert.assertEquals(stats1.merge(stats2), stats2.merge(stats1));
    Assert.assertEquals(expectedStats, stats1.merge(stats2));
  }

  @Test
  public void testMergeCompleteStats() {
    HyperLogLog hll1 = StatisticsTestUtils.createHll(3, 5);
    DecimalColumnStats stats1 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .highValue(DECIMAL_5)
        .numNulls(1)
        .numDVs(2)
        .bitVector(hll1.serialize())
        .build();
    HyperLogLog hll2 = StatisticsTestUtils.createHll(3, 30);
    DecimalColumnStats stats2 = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .highValue(DECIMAL_30)
        .numNulls(1)
        .numDVs(2)
        .bitVector(hll2.serialize())
        .build();

    hll1.merge(hll2);
    DecimalColumnStats expectedStats = DecimalColumnStats.builder()
        .lowValue(DECIMAL_3)
        .highValue(DECIMAL_30)
        .numNulls(2)
        .numDVs(3) // value 3 is correctly detected as duplicate by hll
        .bitVector(hll1.serialize())
        .build();

    Assert.assertEquals(stats1.merge(stats2), stats2.merge(stats1));
    Assert.assertEquals(expectedStats, stats1.merge(stats2));
  }

  @Test
  public void testDecimalCompareEqual() {
    Assert.assertEquals(DECIMAL_3, DECIMAL_3);
  }

  @Test
  public void testDecimalCompareNotEqual() {
    Assert.assertNotEquals(DECIMAL_3, DECIMAL_5);
  }
}
