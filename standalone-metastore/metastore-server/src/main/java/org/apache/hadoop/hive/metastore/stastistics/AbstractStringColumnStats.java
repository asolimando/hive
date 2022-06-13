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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.immutables.value.Value;

@Value.Immutable
public abstract class AbstractStringColumnStats extends VariableLengthColumnStats {

  @JsonProperty("numDVs")
  public abstract long numDVs();

  @Value.Default
  @JsonProperty("bitVector")
  public byte[] bitVector() {
    return new byte[] {'H', 'L'};
  }

  @JsonIgnore
  public ColumnStatisticsData getColumnStatsData() {
    ColumnStatisticsData colStatsData = new ColumnStatisticsData();
    StringColumnStatsData columnStatsData = new StringColumnStatsData();
    columnStatsData.setAvgColLen(avgColLen());
    columnStatsData.setMaxColLen(maxColLen());
    columnStatsData.setNumNulls(numNulls());
    colStatsData.setStringStats(columnStatsData);
    return colStatsData;
  }
}
