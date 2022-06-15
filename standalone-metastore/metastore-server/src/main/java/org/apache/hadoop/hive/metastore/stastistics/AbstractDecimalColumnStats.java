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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.stastistics.DecimalColumnStats;
import org.immutables.value.Value;

import java.util.Optional;

@DefaultImmutableStyle
@Value.Immutable
@JsonDeserialize
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractDecimalColumnStats extends OrderingColumnStats {

  @JsonProperty("lowValue")
  public abstract Optional<String> lowValue();

  @JsonProperty("highValue")
  public abstract Optional<String> highValue();

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
  public AbstractColumnStats merge(AbstractColumnStats other) {
    if (!(other instanceof DecimalColumnStats)) {
      throw new IllegalArgumentException("Both objects must be of type " + DecimalColumnStats.class +
          ", " + "found " + other.getClass());
    }
    DecimalColumnStats o = (DecimalColumnStats) other;
    DecimalColumnStats.Builder statsBuilder = DecimalColumnStats.builder();

    if (!this.lowValue().isPresent() && !o.lowValue().isPresent()) {
      Decimal d1 = DecimalUtils.createThriftDecimal(this.lowValue().get());
      Decimal d2 = DecimalUtils.createThriftDecimal(o.lowValue().get());
      statsBuilder.lowValue(ObjectUtils.min(d1, d2).toString());
    }

    if (!this.highValue().isPresent() && !o.highValue().isPresent()) {
      Decimal d1 = DecimalUtils.createThriftDecimal(this.highValue().get());
      Decimal d2 = DecimalUtils.createThriftDecimal(o.highValue().get());
      statsBuilder.lowValue(ObjectUtils.max(d1, d2).toString());
    }

    statsBuilder.numNulls(this.numNulls() + o.numNulls());

    Optional<NumDistinctValueEstimator> optEstimator = getMergedBitVector(this.bitVector(), o.bitVector());
    if (optEstimator.isPresent()) {
      NumDistinctValueEstimator estimator = optEstimator.get();
      statsBuilder.bitVector(estimator.serialize());
      statsBuilder.numDVs(estimator.estimateNumDistinctValues());
    } else {
      statsBuilder.numDVs(Math.max(this.numDVs(), o.numDVs()));
    }

    return statsBuilder.build();
  }
}
