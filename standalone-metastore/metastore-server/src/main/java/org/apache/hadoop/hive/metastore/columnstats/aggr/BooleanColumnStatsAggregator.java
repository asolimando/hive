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

import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.stastistics.BooleanColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.StatisticsSerdeUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;

public class BooleanColumnStatsAggregator extends ColumnStatsAggregator {

  @Override
  public ColumnStatisticsObj aggregate(List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo,
      List<String> partNames, boolean areAllPartsFound) throws MetaException {
    BooleanColumnStats stats = null;
    String colType = null;
    String colName = null;
    try {
      for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
        ColumnStatisticsObj cso = csp.getColStatsObj();
        if (stats == null) {
          colName = cso.getColName();
          colType = cso.getColType();
          stats = StatisticsSerdeUtils.deserializeStatistics(colType, cso.getStatistics());
        } else {
          BooleanColumnStats newStats = StatisticsSerdeUtils.deserializeStatistics(colType, cso.getStatistics());
          stats = (BooleanColumnStats) stats.merge(newStats);
        }
      }
      ColumnStatisticsObj statsObj = ColumnStatsAggregatorFactory.newColumnStaticsObj(colName, colType);
      statsObj.setStatistics(stats == null ? "" : StatisticsSerdeUtils.serializeStatistics(stats));
      return statsObj;
    } catch (JsonProcessingException e) {
      throw new MetaException("Exception for statistics' serde: " + e.getMessage());
    }
  }
}
