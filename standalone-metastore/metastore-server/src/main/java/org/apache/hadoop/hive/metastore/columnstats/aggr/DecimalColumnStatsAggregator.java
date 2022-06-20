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

import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.merge.DecimalColumnStatsMerger;
import org.apache.hadoop.hive.metastore.stastistics.ColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.DecimalColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.StatisticsSerdeUtils;
import org.apache.hadoop.hive.metastore.stastistics.DecimalColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.StringColumnStats;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.columnstats.ColumnsStatsUtils.decimalInspectorFromStats;

public class DecimalColumnStatsAggregator extends ColumnStatsAggregator implements
    IExtrapolatePartStatus {

  private static final Logger LOG = LoggerFactory.getLogger(DecimalColumnStatsAggregator.class);

  @Override
  public ColumnStatisticsObj aggregate(List<ColStatsObjWithSourceInfo> colStatsWithSourceInfo,
      List<String> partNames, boolean areAllPartsFound) throws MetaException {
    DecimalColumnStats stats = null;
    String colType = null;
    String colName = null;
    boolean doAllPartitionContainStats = partNames.size() == colStatsWithSourceInfo.size();
    boolean areAllEstimatorsMergeable = true;
    for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
      ColumnStatisticsObj cso = csp.getColStatsObj();
      if (stats == null) {
        colName = cso.getColName();
        colType = cso.getColType();
        stats = csp.getStats();
        LOG.trace("doAllPartitionContainStats for column: {} is: {}", colName, doAllPartitionContainStats);
      } else {
        DecimalColumnStats newStats = csp.getStats();
        DecimalColumnStats mergedStats = (DecimalColumnStats) stats.merge(newStats);
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
      Map<String, ColumnStats> adjustedStatsMap = new HashMap<>();
      if (!areAllEstimatorsMergeable) {
        // if not every partition uses bitvector for ndv, we just fall back to the traditional extrapolation methods
        for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
          String partName = csp.getPartName();
          adjustedIndexMap.put(partName, (double) indexMap.get(partName));
          adjustedStatsMap.put(partName, csp.getStats());
        }
      } else {
        // we first merge all the adjacent bitvectors that we could merge and derive new partition names and index
        StringBuilder pseudoPartName = new StringBuilder();
        double pseudoIndexSum = 0;
        int length = 0;
        int currIndex = -1;
        DecimalColumnStats aggregateData = null;
        for (ColStatsObjWithSourceInfo csp : colStatsWithSourceInfo) {
          String partName = csp.getPartName();
          DecimalColumnStats newData = csp.getStats();
          // newData.isSetBitVectors() should be true for sure because we already checked it before
          if (indexMap.get(partName) != currIndex) {
            // There is bitvector, but it is not adjacent to the previous ones.
            if (length > 0) {
              // we have to set ndv
              adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
              Optional<NumDistinctValueEstimator> estimator = aggregateData.getNDVEstimator();
              if (estimator.isPresent()) {
                aggregateData = DecimalColumnStats.builder().from(aggregateData)
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
            aggregateData = DecimalColumnStats.copyOf(newData);
          } else {
            aggregateData = (DecimalColumnStats) aggregateData.merge(newData);
          }
        }
        if (length > 0) {
          adjustedIndexMap.put(pseudoPartName.toString(), pseudoIndexSum / length);
          adjustedStatsMap.put(pseudoPartName.toString(), aggregateData);
        }
      }
      stats = (DecimalColumnStats) myExtrapolate(partNames.size(), colStatsWithSourceInfo.size(),
          adjustedIndexMap, adjustedStatsMap, -1);
    }
    LOG.debug("Ndv estimation for {} is {}, # of partitions requested: {}, # of partitions found: {}",
        colName, stats.numDVs(), partNames.size(), colStatsWithSourceInfo.size());

    ColumnStatisticsObj statsObj = ColumnStatsAggregatorFactory.newColumnStaticsObj(colName, colType);
    statsObj.setStatistics(StatisticsSerdeUtils.serializeStatistics(stats));
    return statsObj;
  }

  private ColumnStats myExtrapolate(int numParts, int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStats> adjustedStatsMap, double densityAvg) {

    DecimalColumnStatsDataInspector extrapolateDecimalData = new DecimalColumnStatsDataInspector();
    Map<String, DecimalColumnStats> extractedAdjustedStatsMap = new HashMap<>();
    for (Map.Entry<String, ColumnStats> entry : adjustedStatsMap.entrySet()) {
      extractedAdjustedStatsMap.put(entry.getKey(), (DecimalColumnStats) entry.getValue().);
    }
    List<Map.Entry<String, DecimalColumnStats>> list = new LinkedList<>(extractedAdjustedStatsMap.entrySet());
    // get the lowValue
    list.sort(Comparator.comparing(o -> o.getValue().lowDecimal().orElse(DecimalUtils.createThriftDecimal())));
    double minInd = adjustedIndexMap.get(list.get(0).getKey());
    double maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double lowValue = 0;
    double min = MetaStoreServerUtils.decimalToDouble(list.get(0).getValue().lowDecimal().get());
    double max = MetaStoreServerUtils.decimalToDouble(list.get(list.size() - 1).getValue().lowDecimal().get());
    if (minInd == maxInd) {
      lowValue = min;
    } else if (minInd < maxInd) {
      // left border is the min
      lowValue = (max - (max - min) * maxInd / (maxInd - minInd));
    } else {
      // right border is the min
      lowValue = (max - (max - min) * (numParts - maxInd) / (minInd - maxInd));
    }

    // get the highValue
    list.sort(Comparator.comparing(o -> o.getValue().getHighValue()));
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double highValue = 0;
    min = MetaStoreServerUtils.decimalToDouble(list.get(0).getValue().getHighValue());
    max = MetaStoreServerUtils.decimalToDouble(list.get(list.size() - 1).getValue().getHighValue());
    if (minInd == maxInd) {
      highValue = min;
    } else if (minInd < maxInd) {
      // right border is the max
      highValue = (min + (max - min) * (numParts - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      highValue = (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the #nulls
    long numNulls = 0;
    for (Map.Entry<String, DecimalColumnStatsData> entry : extractedAdjustedStatsMap.entrySet()) {
      numNulls += entry.getValue().getNumNulls();
    }
    // we scale up sumNulls based on the number of partitions
    numNulls = numNulls * numParts / numPartsWithStats;

    // get the ndv
    long ndv;
    long ndvMin;
    long ndvMax;
    list.sort(Comparator.comparingLong(o -> o.getValue().getNumDVs()));
    long lowerBound = list.get(list.size() - 1).getValue().getNumDVs();
    long higherBound = 0;
    for (Map.Entry<String, DecimalColumnStatsData> entry : list) {
      higherBound += entry.getValue().getNumDVs();
    }
    if (useDensityFunctionForNDVEstimation && densityAvg != 0.0) {
      ndv = (long) ((highValue - lowValue) / densityAvg);
      if (ndv < lowerBound) {
        ndv = lowerBound;
      } else if (ndv > higherBound) {
        ndv = higherBound;
      }
    } else {
      minInd = adjustedIndexMap.get(list.get(0).getKey());
      maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
      ndvMin = list.get(0).getValue().getNumDVs();
      ndvMax = list.get(list.size() - 1).getValue().getNumDVs();
      if (minInd == maxInd) {
        ndv = ndvMin;
      } else if (minInd < maxInd) {
        // right border is the max
        ndv = (long) (ndvMin + (ndvMax - ndvMin) * (numParts - minInd) / (maxInd - minInd));
      } else {
        // left border is the max
        ndv = (long) (ndvMin + (ndvMax - ndvMin) * minInd / (minInd - maxInd));
      }
    }

    return DecimalColumnStats.builder()
        .lowValue(DecimalUtils.createJdoDecimalString(DecimalUtils.createThriftDecimal(String.valueOf(lowValue))))
        .highValue(DecimalUtils.createJdoDecimalString(DecimalUtils.createThriftDecimal(String.valueOf(highValue))))
        .numNulls(numNulls)
        .numDVs(ndv)
        .build();
  }
  
  @Override
  public void extrapolate(ColumnStatisticsData extrapolateData, int numParts,
      int numPartsWithStats, Map<String, Double> adjustedIndexMap,
      Map<String, ColumnStatisticsData> adjustedStatsMap, double densityAvg) {
    int rightBorderInd = numParts;
    DecimalColumnStatsDataInspector extrapolateDecimalData = new DecimalColumnStatsDataInspector();
    Map<String, DecimalColumnStatsData> extractedAdjustedStatsMap = new HashMap<>();
    for (Map.Entry<String, ColumnStatisticsData> entry : adjustedStatsMap.entrySet()) {
      extractedAdjustedStatsMap.put(entry.getKey(), entry.getValue().getDecimalStats());
    }
    List<Map.Entry<String, DecimalColumnStatsData>> list = new LinkedList<>(
        extractedAdjustedStatsMap.entrySet());
    // get the lowValue
    Collections.sort(list, new Comparator<Map.Entry<String, DecimalColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, DecimalColumnStatsData> o1,
          Map.Entry<String, DecimalColumnStatsData> o2) {
        return o1.getValue().getLowValue().compareTo(o2.getValue().getLowValue());
      }
    });
    double minInd = adjustedIndexMap.get(list.get(0).getKey());
    double maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double lowValue = 0;
    double min = MetaStoreServerUtils.decimalToDouble(list.get(0).getValue().getLowValue());
    double max = MetaStoreServerUtils.decimalToDouble(list.get(list.size() - 1).getValue().getLowValue());
    if (minInd == maxInd) {
      lowValue = min;
    } else if (minInd < maxInd) {
      // left border is the min
      lowValue = (max - (max - min) * maxInd / (maxInd - minInd));
    } else {
      // right border is the min
      lowValue = (max - (max - min) * (rightBorderInd - maxInd) / (minInd - maxInd));
    }

    // get the highValue
    Collections.sort(list, new Comparator<Map.Entry<String, DecimalColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, DecimalColumnStatsData> o1,
          Map.Entry<String, DecimalColumnStatsData> o2) {
        return o1.getValue().getHighValue().compareTo(o2.getValue().getHighValue());
      }
    });
    minInd = adjustedIndexMap.get(list.get(0).getKey());
    maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
    double highValue = 0;
    min = MetaStoreServerUtils.decimalToDouble(list.get(0).getValue().getHighValue());
    max = MetaStoreServerUtils.decimalToDouble(list.get(list.size() - 1).getValue().getHighValue());
    if (minInd == maxInd) {
      highValue = min;
    } else if (minInd < maxInd) {
      // right border is the max
      highValue = (min + (max - min) * (rightBorderInd - minInd) / (maxInd - minInd));
    } else {
      // left border is the max
      highValue = (min + (max - min) * minInd / (minInd - maxInd));
    }

    // get the #nulls
    long numNulls = 0;
    for (Map.Entry<String, DecimalColumnStatsData> entry : extractedAdjustedStatsMap.entrySet()) {
      numNulls += entry.getValue().getNumNulls();
    }
    // we scale up sumNulls based on the number of partitions
    numNulls = numNulls * numParts / numPartsWithStats;

    // get the ndv
    long ndv = 0;
    long ndvMin = 0;
    long ndvMax = 0;
    Collections.sort(list, new Comparator<Map.Entry<String, DecimalColumnStatsData>>() {
      @Override
      public int compare(Map.Entry<String, DecimalColumnStatsData> o1,
          Map.Entry<String, DecimalColumnStatsData> o2) {
        return Long.compare(o1.getValue().getNumDVs(), o2.getValue().getNumDVs());
      }
    });
    long lowerBound = list.get(list.size() - 1).getValue().getNumDVs();
    long higherBound = 0;
    for (Map.Entry<String, DecimalColumnStatsData> entry : list) {
      higherBound += entry.getValue().getNumDVs();
    }
    if (useDensityFunctionForNDVEstimation && densityAvg != 0.0) {
      ndv = (long) ((highValue - lowValue) / densityAvg);
      if (ndv < lowerBound) {
        ndv = lowerBound;
      } else if (ndv > higherBound) {
        ndv = higherBound;
      }
    } else {
      minInd = adjustedIndexMap.get(list.get(0).getKey());
      maxInd = adjustedIndexMap.get(list.get(list.size() - 1).getKey());
      ndvMin = list.get(0).getValue().getNumDVs();
      ndvMax = list.get(list.size() - 1).getValue().getNumDVs();
      if (minInd == maxInd) {
        ndv = ndvMin;
      } else if (minInd < maxInd) {
        // right border is the max
        ndv = (long) (ndvMin + (ndvMax - ndvMin) * (rightBorderInd - minInd) / (maxInd - minInd));
      } else {
        // left border is the max
        ndv = (long) (ndvMin + (ndvMax - ndvMin) * minInd / (minInd - maxInd));
      }
    }
    extrapolateDecimalData.setLowValue(DecimalUtils.createThriftDecimal(String
        .valueOf(lowValue)));
    extrapolateDecimalData.setHighValue(DecimalUtils.createThriftDecimal(String
        .valueOf(highValue)));
    extrapolateDecimalData.setNumNulls(numNulls);
    extrapolateDecimalData.setNumDVs(ndv);
    extrapolateData.setDecimalStats(extrapolateDecimalData);
  }
}
