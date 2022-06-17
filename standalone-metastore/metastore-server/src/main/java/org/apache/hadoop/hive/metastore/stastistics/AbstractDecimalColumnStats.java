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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.Optional;

@DefaultImmutableStyle
@Value.Immutable
@JsonDeserialize
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractDecimalColumnStats implements OrderingColumnStats<String> {

  @JsonIgnore
  @Value.Auxiliary
  public Optional<Decimal> lowDecimal() {
    if (lowValue().isPresent()) {
      return Optional.of(DecimalUtils.createThriftDecimal((lowValue().get())));
    }
    return Optional.empty();
  }

  @JsonIgnore
  @Value.Auxiliary
  public Optional<Decimal> highDecimal() {
    if (highValue().isPresent()) {
      return Optional.of(DecimalUtils.createThriftDecimal((highValue().get())));
    }
    return Optional.empty();
  }

  @JsonIgnore
  public ColumnStatisticsData getColumnStatsData() {
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();
    DecimalColumnStatsData columnStatsData = new DecimalColumnStatsData();
    lowValue().ifPresent(v -> columnStatsData.setLowValue(DecimalUtils.createThriftDecimal(v)));
    highValue().ifPresent(v -> columnStatsData.setHighValue(DecimalUtils.createThriftDecimal(v)));
    columnStatsData.setNumNulls(numNulls());
    columnStatsData.setNumDVs(numDVs());
    columnStatsData.setBitVectors(bitVector());
    colStatsData.setDecimalStats(columnStatsData);
    return colStatsData;
  }

  @JsonIgnore
  public ColumnStats merge(ColumnStats other) {
    checkType(DecimalColumnStats.class);
    DecimalColumnStats o = (DecimalColumnStats) other;
    DecimalColumnStats.Builder statsBuilder = DecimalColumnStats.builder();

    Decimal low1 = this.lowValue().isPresent() ? DecimalUtils.createThriftDecimal(this.lowValue().get()) : null;
    Decimal low2 = o.lowValue().isPresent() ? DecimalUtils.createThriftDecimal(o.lowValue().get()) : null;
    Decimal newLow = ObjectUtils.min(low1, low2);
    if (newLow != null) {
      statsBuilder.lowValue(DecimalUtils.createJdoDecimalString(newLow));
    }

    Decimal high1 = this.highValue().isPresent() ? DecimalUtils.createThriftDecimal(this.highValue().get()) : null;
    Decimal high2 = o.highValue().isPresent() ? DecimalUtils.createThriftDecimal(o.highValue().get()) : null;
    Decimal newHigh = ObjectUtils.max(high1, high2);
    if (newHigh != null) {
      statsBuilder.highValue(DecimalUtils.createJdoDecimalString(newHigh));
    }

    statsBuilder.numNulls(this.mergeNumNulls(o));

    Optional<NumDistinctValueEstimator> optEstimator = this.getMergedBitVector(o);
    if (optEstimator.isPresent()) {
      NumDistinctValueEstimator estimator = optEstimator.get();
      byte[] mergedBitVector = estimator.serialize();
      statsBuilder.bitVector(mergedBitVector);
      if (Arrays.equals(this.bitVector(), mergedBitVector)) {
        // in this case the bitvectors could not be merged, do not use it to update NDVs
        statsBuilder.numDVs(this.mergeNDVs(o));
      } else {
        statsBuilder.numDVs(estimator.estimateNumDistinctValues());
      }
    } else {
      statsBuilder.numDVs(this.mergeNDVs(o));
    }

    return statsBuilder.build();
  }
}
