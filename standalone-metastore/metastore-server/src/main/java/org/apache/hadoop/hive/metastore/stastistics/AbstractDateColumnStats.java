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
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.immutables.value.Value;

import java.util.Optional;

@DefaultImmutableStyle
@Value.Immutable
@JsonDeserialize
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractDateColumnStats extends OrderingColumnStats {

  @JsonProperty("lowValue")
  public abstract Optional<Long> lowValue();

  @JsonProperty("highValue")
  public abstract Optional<Long> highValue();

  @JsonIgnore
  public ColumnStatisticsData getColumnStatsData() {
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();
    DateColumnStatsData columnStatsData = new DateColumnStatsData();
    lowValue().ifPresent(v -> columnStatsData.setLowValue(new Date(v)));
    highValue().ifPresent(v -> columnStatsData.setHighValue(new Date(v)));
    columnStatsData.setNumNulls(numNulls());
    columnStatsData.setNumDVs(numDVs());
    columnStatsData.setBitVectors(bitVector());
    colStatsData.setDateStats(columnStatsData);
    columnStatsData.setBitVectors(bitVector());
    return colStatsData;
  }
}
