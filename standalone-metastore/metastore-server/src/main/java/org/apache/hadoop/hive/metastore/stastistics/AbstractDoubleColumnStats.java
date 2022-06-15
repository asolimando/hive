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
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.stastistics.DoubleColumnStats;
import org.immutables.value.Value;

import java.util.Optional;

@DefaultImmutableStyle
@Value.Immutable
@JsonDeserialize
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractDoubleColumnStats extends OrderingColumnStats {

  @JsonProperty("lowValue")
  public abstract Optional<Double> lowValue();

  @JsonProperty("highValue")
  public abstract Optional<Double> highValue();

  @JsonIgnore
  public ColumnStatisticsData getColumnStatsData() {
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();
    DoubleColumnStatsData columnStatsData = new DoubleColumnStatsData();
    lowValue().ifPresent(columnStatsData::setLowValue);
    highValue().ifPresent(columnStatsData::setHighValue);
    columnStatsData.setNumNulls(numNulls());
    columnStatsData.setNumDVs(numDVs());
    columnStatsData.setBitVectors(bitVector());
    colStatsData.setDoubleStats(columnStatsData);
    return colStatsData;
  }

  @JsonIgnore
  public AbstractColumnStats merge(AbstractColumnStats other) {
    if (!(other instanceof DoubleColumnStats)) {
      throw new IllegalArgumentException("Both objects must be of type " + DoubleColumnStats.class +
          ", " + "found " + other.getClass());
    }
    DoubleColumnStats o = (DoubleColumnStats) other;
    DoubleColumnStats.Builder statsBuilder = DoubleColumnStats.builder();

    statsBuilder.lowValue(mergeLowValues(this.lowValue(), o.lowValue(), Double::compareTo));
    statsBuilder.highValue(mergeHighValues(this.highValue(), o.highValue(), Double::compareTo));

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
