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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.stastistics.AbstractColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.OrderingColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.StatisticsSerdeUtils;
import org.apache.hadoop.hive.metastore.stastistics.StringColumnStats;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringColumnStatsAggregator extends ColumnStatsAggregator implements
    IExtrapolatePartStatus {

  private static final Logger LOG = LoggerFactory.getLogger(StringColumnStatsAggregator.class);

  @Override
  public ColumnStatisticsObj aggregate(List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo,
      List<String> partNames, boolean areAllPartsFound) throws MetaException {
    StringColumnStats stats = null;
    String colType = null;
    String colName = null;
    boolean doAllPartitionContainStats = partNames.size() == colStatsWithSourceInfo.size();
    boolean areAllEstimatorsMergeable = true;
    try {
      for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
        ColumnStatisticsObj cso = csp.getColStatsObj();
        if (stats == null) {
          colName = cso.getColName();
          colType = cso.getColType();
          stats = StatisticsSerdeUtils.deserializeStatistics(colType, cso.getStatistics());
          LOG.trace("doAllPartitionContainStats for column: {} is: {}", colName, doAllPartitionContainStats);
        } else {
          StringColumnStats newStats = StatisticsSerdeUtils.deserializeStatistics(colType, cso.getStatistics());
          StringColumnStats mergedStats = (StringColumnStats) stats.merge(newStats);
          if (Arrays.equals(stats.bitVector(), mergedStats.bitVector())) {
            areAllEstimatorsMergeable = false;
          }
          stats = mergedStats;
        }
      }
      LOG.debug("all of the bit vectors can merge for {} is {}", colName, areAllEstimatorsMergeable);
      if (!doAllPartitionContainStats && colStatsWithSourceInfo.size() >= 2) {
        LOG.debug("start extrapolation for {}", colName);

        Map<String, Integer> indexMap = new HashMap<>();
        for (int index = 0; index < partNames.size(); index++) {
          indexMap.put(partNames.get(index), index);
        }
        Map<String, Double> adjustedIndexMap = new HashMap<>();
        Map<String, AbstractColumnStats> adjustedStatsMap = new HashMap<>();
        if (!areAllEstimatorsMergeable) {
          // if not every partition uses bitvector for ndv, we just fall back to the traditional extrapolation methods
          for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
            ColumnStatisticsObj cso = csp.getColStatsObj();
            String partName = csp.getPartName();
            adjustedIndexMap.put(partName, (double) indexMap.get(partName));
            adjustedStatsMap.put(partName, StatisticsSerdeUtils.deserializeStatistics(colType, cso.getStatistics()));
          }
        } else {
          // we first merge all the adjacent bitvectors that we could merge and derive new partition names and index
          StringBuilder pseudoPartName = new StringBuilder();
          double pseudoIndexSum = 0;
          int length = 0;
          int currIndex = -1;
          StringColumnStats aggregateData = null;
          for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
            ColumnStatisticsObj cso = csp.getColStatsObj();
            String partName = csp.getPartName();
            StringColumnStats newData = StatisticsSerdeUtils.deserializeStatistics(colType, cso.getStatistics());
            // newData.isSetBitVectors() should be true for sure because we already checked it before
            if (indexMap.get(partName) != currIndex) {
              // There is bitvector, but it is not adjacent to the previous ones.
              if (length > 0) {
                // we have to set ndv
                adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
                Optional<NumDistinctValueEstimator> estimator = OrderingColumnStats.getNDVEstimator(aggregateData.bitVector());
                if (estimator.isPresent()) {
                  aggregateData = StringColumnStats.builder().from(aggregateData)
                      .numDVs(estimator.get().estimateNumDistinctValues())
                      .build();
                }
                adjustedStatsMap.put(pseudoPartName.toString(), aggregateData);
                // reset everything
                pseudoPartName = new StringBuilder();
                pseudoIndexSum = 0;
                length = 0;
              }
              aggregateData = null;
            }
            currIndex = indexMap.get(partName);
            pseudoPartName.append(partName);
            pseudoIndexSum += currIndex;
            length++;
            currIndex++;
            if (aggregateData == null) {
              aggregateData = StringColumnStats.copyOf(newData);
            } else {
              aggregateData = (StringColumnStats) aggregateData.merge(newData);
            }
          }
          if (length > 0) {
            adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
            adjustedStatsMap.put(pseudoPartName.toString(), aggregateData);
          }
        }
        stats = (StringColumnStats) myExtrapolate(partNames.size(), colStatsWithSourceInfo.size(),
            adjustedIndexMap, adjustedStatsMap, -1);
      }
      LOG.debug("Ndv estimation for {} is {}, # of partitions requested: {}, # of partitions found: {}",
          colName, stats.numDVs(), partNames.size(), colStatsWithSourceInfo.size());

      ColumnStatisticsObj statsObj = ColumnStatsAggregatorFactory.newColumnStaticsObj(colName, colType);
      statsObj.setStatistics(stats == null ? "" : StatisticsSerdeUtils.serializeStatistics(stats));
      return statsObj;
    } catch (JsonProcessingException e) {
      throw new MetaException("Exception for statistics' serde: " + e.getMessage());
    }
  }

  private AbstractColumnStats myExtrapolate(int numParts, int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, AbstractColumnStats> adjustedStatsMap, double densityAvg) throws JsonProcessingException {

    Map<String, StringColumnStats> extractedAdjustedStatsMap = new HashMap<>();
    for (Map.Entry<String, AbstractColumnStats> entry : adjustedStatsMap.entrySet()) {
      extractedAdjustedStatsMap.put(entry.getKey(), (StringColumnStats) entry.getValue());
    }
    List<Map.Entry<String, StringColumnStats>> list = new LinkedList<>(extractedAdjustedStatsMap.entrySet());
    // get the avgLen
    list.sort(Comparator.comparingDouble(o -> o.getValue().avgColLen()));
    double minInd = adjustedIndexMap.get(list.get(0).getKey());
    double maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double avgColLen;
    double min = list.get(0).getValue().avgColLen();
    double max = list.get(list.size() - 1).getValue().avgColLen();
    if (minInd == maxInd) {
      avgColLen = min;
    } else if (minInd < maxInd) {
      // right border is the max
      avgColLen = (min + (max - min) * (numParts - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      avgColLen = (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the maxLen
    list.sort(Comparator.comparingLong(o -> o.getValue().maxColLen()));
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double maxColLen;
    min = list.get(0).getValue().avgColLen();
    max = list.get(list.size() - 1).getValue().avgColLen();
    if (minInd == maxInd) {
      maxColLen = min;
    } else if (minInd < maxInd) {
      // right border is the max
      maxColLen = (min + (max - min) * (numParts - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      maxColLen = (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the #nulls
    long numNulls = 0;
    for (Map.Entry<String, StringColumnStats> entry : extractedAdjustedStatsMap.entrySet()) {
      numNulls += entry.getValue().numNulls();
    }
    // we scale up sumNulls based on the number of partitions
    numNulls = numNulls * numParts / numPartsWithStats;

    // get the ndv
    long ndv;
    list.sort(Comparator.comparingLong(o -> o.getValue().numDVs()));
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    min = list.get(0).getValue().numDVs();
    max = list.get(list.size() - 1).getValue().numDVs();
    if (minInd == maxInd) {
      ndv = (long) min;
    } else if (minInd < maxInd) {
      // right border is the max
      ndv = (long) (min + (max - min) * (numParts - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      ndv = (long) (min + (max - min) * minInd / (minInd - maxInd));
    }
    return StringColumnStats.builder()
        .avgColLen(avgColLen)
        .maxColLen((long) maxColLen)
        .numNulls(numNulls)
        .numDVs(ndv)
        .build();
  }

  @Override
  public void extrapolate(ColumnStatisticsData extrapolateData, int numParts,
      int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStatisticsData> adjustedStatsMap, double densityAvg) {
    int rightBorderInd = numParts;
    StringColumnStatsDataInspector extrapolateStringData =
        new StringColumnStatsDataInspector();
    Map<String, StringColumnStatsData> extractedAdjustedStatsMap = new HashMap<>();
    for (Map.Entry<String, ColumnStatisticsData> entry : adjustedStatsMap.entrySet()) {
      extractedAdjustedStatsMap.put(entry.getKey(), entry.getValue().getStringStats());
    }
    List<Map.Entry<String, StringColumnStatsData>> list = new LinkedList<>(
        extractedAdjustedStatsMap.entrySet());
    // get the avgLen
    Collections.sort(list, new Comparator<Map.Entry<String, StringColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, StringColumnStatsData> o1,
          Map.Entry<String, StringColumnStatsData> o2) {
        return Double.compare(o1.getValue().getAvgColLen(), o2.getValue().getAvgColLen());
      }
    });
    double minInd = adjustedIndexMap.get(list.get(0).getKey());
    double maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double avgColLen = 0;
    double min = list.get(0).getValue().getAvgColLen();
    double max = list.get(list.size() - 1).getValue().getAvgColLen();
    if (minInd == maxInd) {
      avgColLen = min;
    } else if (minInd < maxInd) {
      // right border is the max
      avgColLen = (min + (max - min) * (rightBorderInd - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      avgColLen = (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the maxLen
    Collections.sort(list, new Comparator<Map.Entry<String, StringColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, StringColumnStatsData> o1,
          Map.Entry<String, StringColumnStatsData> o2) {
        return Long.compare(o1.getValue().getMaxColLen(), o2.getValue().getMaxColLen());
      }
    });
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double maxColLen = 0;
    min = list.get(0).getValue().getAvgColLen();
    max = list.get(list.size() - 1).getValue().getAvgColLen();
    if (minInd == maxInd) {
      maxColLen = min;
    } else if (minInd < maxInd) {
      // right border is the max
      maxColLen = (min + (max - min) * (rightBorderInd - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      maxColLen = (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the #nulls
    long numNulls = 0;
    for (Map.Entry<String, StringColumnStatsData> entry : extractedAdjustedStatsMap.entrySet()) {
      numNulls += entry.getValue().getNumNulls();
    }
    // we scale up sumNulls based on the number of partitions
    numNulls = numNulls * numParts / numPartsWithStats;

    // get the ndv
    long ndv = 0;
    Collections.sort(list, new Comparator<Map.Entry<String, StringColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, StringColumnStatsData> o1,
          Map.Entry<String, StringColumnStatsData> o2) {
        return Long.compare(o1.getValue().getNumDVs(), o2.getValue().getNumDVs());
      }
    });
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    min = list.get(0).getValue().getNumDVs();
    max = list.get(list.size() - 1).getValue().getNumDVs();
    if (minInd == maxInd) {
      ndv = (long) min;
    } else if (minInd < maxInd) {
      // right border is the max
      ndv = (long) (min + (max - min) * (rightBorderInd - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      ndv = (long) (min + (max - min) * minInd / (minInd - maxInd));
    }
    extrapolateStringData.setAvgColLen(avgColLen);
    extrapolateStringData.setMaxColLen((long) maxColLen);
    extrapolateStringData.setNumNulls(numNulls);
    extrapolateStringData.setNumDVs(ndv);
    extrapolateData.setStringStats(extrapolateStringData);
  }

}
