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

package org.apache.hadoop.hive.metastore;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.fasterxml.jackson.core.JsonProcessingException;
import javolution.testing.AssertionException;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnStatistics;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.model.MTableColumnStatistics;
import org.apache.hadoop.hive.metastore.stastistics.BinaryColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.BooleanColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.DateColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.DecimalColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.DoubleColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.LongColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.StatisticsSerdeUtils;
import org.apache.hadoop.hive.metastore.stastistics.StringColumnStats;
import org.apache.hadoop.hive.metastore.stastistics.TimestampColumnStats;

/**
 * This class contains conversion logic that creates Thrift stat objects from
 * JDO stat objects and plain arrays from DirectSQL.
 * It is hidden here so that we wouldn't have to look at it in elsewhere.
 */
public class StatObjectConverter {

  private StatObjectConverter() {
    throw new AssertionException("Private constructor to avoid instantiation");
  }

  // JDO
  public static MTableColumnStatistics convertToMTableColumnStatistics(MTable table,
      ColumnStatisticsDesc statsDesc, ColumnStatisticsObj statsObj, String engine) throws MetaException, InvalidObjectException {
     if (statsObj == null || statsDesc == null) {
       throw new InvalidObjectException("Invalid column stats object");
     }

     MTableColumnStatistics mColStats = new MTableColumnStatistics();
     mColStats.setTable(table);
     mColStats.setDbName(statsDesc.getDbName());
     mColStats.setCatName(table.getDatabase().getCatalogName());
     mColStats.setTableName(statsDesc.getTableName());
     mColStats.setLastAnalyzed(statsDesc.getLastAnalyzed());
     mColStats.setColName(statsObj.getColName());
     mColStats.setColType(statsObj.getColType());
     mColStats.setStatistics(getStatisticsString(statsObj));
     mColStats.setEngine(engine);
     return mColStats;
  }

  public static void setFieldsIntoOldStats(
      MTableColumnStatistics mStatsObj, MTableColumnStatistics oldStatsObj) {
    if (mStatsObj.getStatistics() != null) {
      oldStatsObj.setStatistics(mStatsObj.getStatistics());
    }
    oldStatsObj.setEngine(mStatsObj.getEngine());
    oldStatsObj.setLastAnalyzed(mStatsObj.getLastAnalyzed());
  }

  public static void setFieldsIntoOldStats(
      MPartitionColumnStatistics mStatsObj, MPartitionColumnStatistics oldStatsObj) {
    if (mStatsObj.getStatistics() != null) {
          oldStatsObj.setStatistics(mStatsObj.getStatistics());
    }
    oldStatsObj.setEngine(mStatsObj.getEngine());
  }

  public static String getUpdatedColumnSql(MPartitionColumnStatistics mStatsObj) {
    StringBuilder setStmt = new StringBuilder();
    if (mStatsObj.getStatistics() != null) {
      setStmt.append("\"STATISTICS\" = ? ,");
    }
    setStmt.append("\"ENGINE\" = ? ");
    return setStmt.toString();
  }

  public static void initUpdatedColumnStatement(MPartitionColumnStatistics mStatsObj,
                                                      PreparedStatement pst) throws SQLException {
    int colIdx = 1;
    if (mStatsObj.getStatistics() != null) {
      pst.setObject(colIdx++, mStatsObj.getStatistics());
    }
    pst.setLong(colIdx++, mStatsObj.getLastAnalyzed());
    pst.setString(colIdx, mStatsObj.getEngine());
  }

  public static ColumnStatisticsObj getTableColumnStatisticsObj(MTableColumnStatistics mStatsObj) throws MetaException {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColType(mStatsObj.getColType());
    statsObj.setColName(mStatsObj.getColName());
    String colType = mStatsObj.getColType().toLowerCase();
    try {
      statsObj.setStatsData(StatisticsSerdeUtils.getColumnStatisticsData(colType, mStatsObj.getStatistics()));
    } catch (JsonProcessingException e) {
      throw new MetaException("Exception while deserializing table column statistics object: " + e.getMessage());
    }
    return statsObj;
  }

  public static ColumnStatisticsDesc getTableColumnStatisticsDesc(
      MTableColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(true);
    statsDesc.setCatName(mStatsObj.getCatName());
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  public static MPartitionColumnStatistics convertToMPartitionColumnStatistics(MPartition partition,
      ColumnStatisticsDesc statsDesc, ColumnStatisticsObj statsObj, String engine) throws MetaException {
    if (statsDesc == null || statsObj == null) {
      return null;
    }

    MPartitionColumnStatistics mColStats = new MPartitionColumnStatistics();
    if (partition != null) {
      mColStats.setCatName(partition.getTable().getDatabase().getCatalogName());
      mColStats.setPartition(partition);
    } else {
      // Assume that the statsDesc has already set catalogName when partition is null
      mColStats.setCatName(statsDesc.getCatName());
    }
    mColStats.setDbName(statsDesc.getDbName());
    mColStats.setTableName(statsDesc.getTableName());
    mColStats.setPartitionName(statsDesc.getPartName());
    mColStats.setLastAnalyzed(statsDesc.getLastAnalyzed());
    mColStats.setColName(statsObj.getColName());
    mColStats.setColType(statsObj.getColType());
    mColStats.setStatistics(getStatisticsString(statsObj));
    mColStats.setEngine(engine);
    return mColStats;
  }

  private static String getStatisticsString(ColumnStatisticsObj statsObj) throws MetaException {
    try {
      String statistics;
      if (statsObj.getStatsData().isSetBooleanStats()) {
        BooleanColumnStatsData boolStats = statsObj.getStatsData().getBooleanStats();
        BooleanColumnStats.Builder statsBuilder = BooleanColumnStats.builder();
        if (boolStats.isSetNumTrues()) {
          statsBuilder.numTrues(boolStats.getNumTrues());
        }
        if (boolStats.isSetNumFalses()) {
          statsBuilder.numFalses(boolStats.getNumFalses());
        }
        if (boolStats.isSetNumNulls()) {
          statsBuilder.numNulls(boolStats.getNumNulls());
        }
        statistics = StatisticsSerdeUtils.serializeStatistics(statsBuilder.build());
      } else if (statsObj.getStatsData().isSetLongStats()) {
        LongColumnStatsData longStats = statsObj.getStatsData().getLongStats();
        LongColumnStats.Builder statsBuilder = LongColumnStats.builder();
        if (longStats.isSetNumNulls()) {
          statsBuilder.numNulls(longStats.getNumNulls());
        }
        if (longStats.isSetNumDVs()) {
          statsBuilder.numDVs(longStats.getNumDVs());
        }
        if (longStats.isSetBitVectors()) {
          statsBuilder.bitVector(longStats.getBitVectors());
        }
        if (longStats.isSetLowValue()) {
          statsBuilder.lowValue(longStats.getLowValue());
        }
        if (longStats.isSetHighValue()) {
          statsBuilder.highValue(longStats.getHighValue());
        }
        statistics = StatisticsSerdeUtils.serializeStatistics(statsBuilder.build());
      } else if (statsObj.getStatsData().isSetDoubleStats()) {
        DoubleColumnStatsData doubleStats = statsObj.getStatsData().getDoubleStats();
        DoubleColumnStats.Builder statsBuilder = DoubleColumnStats.builder();
        if (doubleStats.isSetNumNulls()) {
          statsBuilder.numNulls(doubleStats.getNumNulls());
        }
        if (doubleStats.isSetNumDVs()) {
          statsBuilder.numDVs(doubleStats.getNumDVs());
        }
        if (doubleStats.isSetBitVectors()) {
          statsBuilder.bitVector(doubleStats.getBitVectors());
        }
        if (doubleStats.isSetLowValue()) {
          statsBuilder.lowValue(doubleStats.getLowValue());
        }
        if (doubleStats.isSetHighValue()) {
          statsBuilder.highValue(doubleStats.getHighValue());
        }
        statistics = StatisticsSerdeUtils.serializeStatistics(statsBuilder.build());
      } else if (statsObj.getStatsData().isSetDecimalStats()) {
        DecimalColumnStatsData decimalStats = statsObj.getStatsData().getDecimalStats();
        DecimalColumnStats.Builder statsBuilder = DecimalColumnStats.builder();
        if (decimalStats.isSetNumNulls()) {
          statsBuilder.numNulls(decimalStats.getNumNulls());
        }
        if (decimalStats.isSetNumDVs()) {
          statsBuilder.numDVs(decimalStats.getNumDVs());
        }
        if (decimalStats.isSetBitVectors()) {
          statsBuilder.bitVector(decimalStats.getBitVectors());
        }
        if (decimalStats.isSetLowValue()) {
          statsBuilder.lowValue(DecimalUtils.createJdoDecimalString(decimalStats.getLowValue()));
        }
        if (decimalStats.isSetHighValue()) {
          statsBuilder.highValue(DecimalUtils.createJdoDecimalString(decimalStats.getHighValue()));
        }
        statistics = StatisticsSerdeUtils.serializeStatistics(statsBuilder.build());
      } else if (statsObj.getStatsData().isSetStringStats()) {
        StringColumnStatsData stringStats = statsObj.getStatsData().getStringStats();
        StringColumnStats.Builder statsBuilder = StringColumnStats.builder();
        if (stringStats.isSetNumNulls()) {
          statsBuilder.numNulls(stringStats.getNumNulls());
        }
        if (stringStats.isSetNumDVs()) {
          statsBuilder.numDVs(stringStats.getNumDVs());
        }
        if (stringStats.isSetBitVectors()) {
          statsBuilder.bitVector(stringStats.getBitVectors());
        }
        if (stringStats.isSetMaxColLen()) {
          statsBuilder.maxColLen(stringStats.getMaxColLen());
        }
        if (stringStats.isSetAvgColLen()) {
          statsBuilder.avgColLen(stringStats.getAvgColLen());
        }
        statistics = StatisticsSerdeUtils.serializeStatistics(statsBuilder.build());
      } else if (statsObj.getStatsData().isSetBinaryStats()) {
        BinaryColumnStatsData binaryStats = statsObj.getStatsData().getBinaryStats();
        StringColumnStats.Builder statsBuilder = StringColumnStats.builder();
        if (binaryStats.isSetNumNulls()) {
          statsBuilder.numNulls(binaryStats.getNumNulls());
        }
        if (binaryStats.isSetMaxColLen()) {
          statsBuilder.maxColLen(binaryStats.getMaxColLen());
        }
        if (binaryStats.isSetAvgColLen()) {
          statsBuilder.avgColLen(binaryStats.getAvgColLen());
        }
        statistics = StatisticsSerdeUtils.serializeStatistics(statsBuilder.build());
      } else if (statsObj.getStatsData().isSetDateStats()) {
        DateColumnStatsData dateStats = statsObj.getStatsData().getDateStats();
        DateColumnStats.Builder statsBuilder = DateColumnStats.builder();
        if (dateStats.isSetNumNulls()) {
          statsBuilder.numNulls(dateStats.getNumNulls());
        }
        if (dateStats.isSetNumDVs()) {
          statsBuilder.numDVs(dateStats.getNumDVs());
        }
        if (dateStats.isSetBitVectors()) {
          statsBuilder.bitVector(dateStats.getBitVectors());
        }
        if (dateStats.isSetLowValue()) {
          statsBuilder.lowValue(dateStats.getLowValue().getDaysSinceEpoch());
        }
        if (dateStats.isSetHighValue()) {
          statsBuilder.highValue(dateStats.getHighValue().getDaysSinceEpoch());
        }
        statistics = StatisticsSerdeUtils.serializeStatistics(statsBuilder.build());
      } else if (statsObj.getStatsData().isSetTimestampStats()) {
        TimestampColumnStatsData timestampStats = statsObj.getStatsData().getTimestampStats();
        DateColumnStats.Builder statsBuilder = DateColumnStats.builder();
        if (timestampStats.isSetNumNulls()) {
          statsBuilder.numNulls(timestampStats.getNumNulls());
        }
        if (timestampStats.isSetNumDVs()) {
          statsBuilder.numDVs(timestampStats.getNumDVs());
        }
        if (timestampStats.isSetBitVectors()) {
          statsBuilder.bitVector(timestampStats.getBitVectors());
        }
        if (timestampStats.isSetLowValue()) {
          statsBuilder.lowValue(timestampStats.getLowValue().getSecondsSinceEpoch());
        }
        if (timestampStats.isSetHighValue()) {
          statsBuilder.highValue(timestampStats.getHighValue().getSecondsSinceEpoch());
        }
        statistics = StatisticsSerdeUtils.serializeStatistics(statsBuilder.build());
      } else {
        throw new IllegalArgumentException("Unrecognized statistics object's type");
      }
      return statistics;
    } catch (JsonProcessingException e) {
      throw new MetaException("Exception while serializing table partition column statistics:" + e.getMessage());
    }
  }

  public static ColumnStatisticsObj getPartitionColumnStatisticsObj(MPartitionColumnStatistics mStatsObj) throws MetaException {
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColType(mStatsObj.getColType());
    statsObj.setColName(mStatsObj.getColName());
    String colType = mStatsObj.getColType().toLowerCase();
    try {
      statsObj.setStatsData(StatisticsSerdeUtils.getColumnStatisticsData(colType, mStatsObj.getStatistics()));
    } catch (JsonProcessingException e) {
      throw new MetaException("Exception while deserializing table column statistics object: " + e.getMessage());
    }
    return statsObj;
  }

  public static ColumnStatisticsDesc getPartitionColumnStatisticsDesc(
    MPartitionColumnStatistics mStatsObj) {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
    statsDesc.setIsTblLevel(false);
    statsDesc.setCatName(mStatsObj.getCatName());
    statsDesc.setDbName(mStatsObj.getDbName());
    statsDesc.setTableName(mStatsObj.getTableName());
    statsDesc.setPartName(mStatsObj.getPartitionName());
    statsDesc.setLastAnalyzed(mStatsObj.getLastAnalyzed());
    return statsDesc;
  }

  public static byte[] getBitVector(byte[] bytes) {
    // workaround for DN bug in persisting nulls in pg bytea column
    // instead set empty bit vector with header.
    // https://issues.apache.org/jira/browse/HIVE-17836
    if (bytes != null && bytes.length == 2 && bytes[0] == 'H' && bytes[1] == 'L') {
      return null;
    }
    return bytes;
  }

  public static String fillColumnStatisticsData(String colType, Object longLowValue, Object longHighValue,
      Object doubleLowValue, Object doubleHighValue, Object decimalLowValue, Object decimalHighValue,
      Object numNulls, Object numDVs, Object bitVector, Object avgColLen, Object maxColLen, Object numTrues,
      Object numFalses) throws MetaException {
    colType = colType.toLowerCase();
    String statistics;

    try {
      if (colType.equals("boolean")) {
        statistics = StatisticsSerdeUtils.serializeStatistics(
            BooleanColumnStats.builder()
                .numTrues((Long) numTrues)
                .numFalses((Long) numFalses)
                .numNulls((Long) numNulls)
                .build());
      } else if (colType.equals("string") || colType.startsWith("varchar") || colType.startsWith("char")) {
        statistics = StatisticsSerdeUtils.serializeStatistics(
            StringColumnStats.builder()
                .numNulls((Long) numNulls)
                .numDVs((Long) numDVs)
                .bitVector((byte[]) bitVector)
                .maxColLen((Long) maxColLen)
                .avgColLen((Double) avgColLen)
                .build());
      } else if (colType.equals("binary")) {
        statistics = StatisticsSerdeUtils.serializeStatistics(
            BinaryColumnStats.builder()
            .numNulls((Long) numNulls)
            .maxColLen((Long) maxColLen)
            .avgColLen((Double) avgColLen)
            .build());
      } else if (colType.equals("bigint") || colType.equals("int") || colType.equals("smallint") || colType.equals("tinyint")) {
        statistics = StatisticsSerdeUtils.serializeStatistics(
            LongColumnStats.builder()
                .numNulls((Long) numNulls)
                .numDVs((Long) numDVs)
                .bitVector((byte[]) bitVector)
                .lowValue((Long) longLowValue)
                .highValue((Long) longHighValue)
                .build());
      } else if (colType.equals("double") || colType.equals("float")) {
        statistics = StatisticsSerdeUtils.serializeStatistics(
            DoubleColumnStats.builder()
                .numNulls((Long) numNulls)
                .numDVs((Long) numDVs)
                .bitVector((byte[]) bitVector)
                .lowValue((Double) doubleLowValue)
                .highValue((Double) doubleHighValue)
                .build());
      } else if (colType.startsWith("decimal")) {
        statistics = StatisticsSerdeUtils.serializeStatistics(
            DecimalColumnStats.builder()
                .numNulls((Long) numNulls)
                .numDVs((Long) numDVs)
                .bitVector((byte[]) bitVector)
                .lowValue((String) decimalLowValue)
                .highValue((String) decimalHighValue)
                .build());
      } else if (colType.equals("date")) {
        statistics = StatisticsSerdeUtils.serializeStatistics(
            DateColumnStats.builder()
                .numNulls((Long) numNulls)
                .numDVs((Long) numDVs)
                .bitVector((byte[]) bitVector)
                .lowValue((Long) longLowValue)
                .highValue((Long) longHighValue)
                .build());
      } else if (colType.equals("timestamp")) {
        statistics = StatisticsSerdeUtils.serializeStatistics(
            TimestampColumnStats.builder()
                .numNulls((Long) numNulls)
                .numDVs((Long) numDVs)
                .bitVector((byte[]) bitVector)
                .lowValue((Long) longLowValue)
                .highValue((Long) longHighValue)
                .build());
      } else {
        throw new MetaException("Unknown column type " + colType);
      }
    } catch (JsonProcessingException e) {
      throw new MetaException("Exception while serializing column statistics: " + e.getMessage());
    }

    return statistics;
  }

  /**
   * Set field values in oldStatObj from newStatObj
   * @param oldStatObj the old statistics object
   * @param newStatObj the new statistics object
   */
  public static void setFieldsIntoOldStats(ColumnStatisticsObj oldStatObj,
      ColumnStatisticsObj newStatObj) {
    _Fields typeNew = newStatObj.getStatsData().getSetField();
    _Fields typeOld = oldStatObj.getStatsData().getSetField();
    if (typeNew != typeOld) {
      throw new IllegalArgumentException("Types for old and new stat must be equal, found " +
          typeOld + " and " + typeNew + ", respectively");
    }
    switch (typeNew) {
    case BOOLEAN_STATS:
      BooleanColumnStatsData oldBooleanStatsData = oldStatObj.getStatsData().getBooleanStats();
      BooleanColumnStatsData newBooleanStatsData = newStatObj.getStatsData().getBooleanStats();
      if (newBooleanStatsData.isSetNumTrues()) {
        oldBooleanStatsData.setNumTrues(newBooleanStatsData.getNumTrues());
      }
      if (newBooleanStatsData.isSetNumFalses()) {
        oldBooleanStatsData.setNumFalses(newBooleanStatsData.getNumFalses());
      }
      if (newBooleanStatsData.isSetNumNulls()) {
        oldBooleanStatsData.setNumNulls(newBooleanStatsData.getNumNulls());
      }
      if (newBooleanStatsData.isSetBitVectors()) {
        oldBooleanStatsData.setBitVectors(newBooleanStatsData.getBitVectors());
      }
      break;
    case LONG_STATS: {
      LongColumnStatsData oldLongStatsData = oldStatObj.getStatsData().getLongStats();
      LongColumnStatsData newLongStatsData = newStatObj.getStatsData().getLongStats();
      if (newLongStatsData.isSetHighValue()) {
        oldLongStatsData.setHighValue(newLongStatsData.getHighValue());
      }
      if (newLongStatsData.isSetLowValue()) {
        oldLongStatsData.setLowValue(newLongStatsData.getLowValue());
      }
      if (newLongStatsData.isSetNumNulls()) {
        oldLongStatsData.setNumNulls(newLongStatsData.getNumNulls());
      }
      if (newLongStatsData.isSetNumDVs()) {
        oldLongStatsData.setNumDVs(newLongStatsData.getNumDVs());
      }
      if (newLongStatsData.isSetBitVectors()) {
        oldLongStatsData.setBitVectors(newLongStatsData.getBitVectors());
      }
      break;
    }
    case DOUBLE_STATS: {
      DoubleColumnStatsData oldDoubleStatsData = oldStatObj.getStatsData().getDoubleStats();
      DoubleColumnStatsData newDoubleStatsData = newStatObj.getStatsData().getDoubleStats();
      if (newDoubleStatsData.isSetHighValue()) {
        oldDoubleStatsData.setHighValue(newDoubleStatsData.getHighValue());
      }
      if (newDoubleStatsData.isSetLowValue()) {
        oldDoubleStatsData.setLowValue(newDoubleStatsData.getLowValue());
      }
      if (newDoubleStatsData.isSetNumNulls()) {
        oldDoubleStatsData.setNumNulls(newDoubleStatsData.getNumNulls());
      }
      if (newDoubleStatsData.isSetNumDVs()) {
        oldDoubleStatsData.setNumDVs(newDoubleStatsData.getNumDVs());
      }
      if (newDoubleStatsData.isSetBitVectors()) {
        oldDoubleStatsData.setBitVectors(newDoubleStatsData.getBitVectors());
      }
      break;
    }
    case STRING_STATS: {
      StringColumnStatsData oldStringStatsData = oldStatObj.getStatsData().getStringStats();
      StringColumnStatsData newStringStatsData = newStatObj.getStatsData().getStringStats();
      if (newStringStatsData.isSetMaxColLen()) {
        oldStringStatsData.setMaxColLen(newStringStatsData.getMaxColLen());
      }
      if (newStringStatsData.isSetAvgColLen()) {
        oldStringStatsData.setAvgColLen(newStringStatsData.getAvgColLen());
      }
      if (newStringStatsData.isSetNumNulls()) {
        oldStringStatsData.setNumNulls(newStringStatsData.getNumNulls());
      }
      if (newStringStatsData.isSetNumDVs()) {
        oldStringStatsData.setNumDVs(newStringStatsData.getNumDVs());
      }
      if (newStringStatsData.isSetBitVectors()) {
        oldStringStatsData.setBitVectors(newStringStatsData.getBitVectors());
      }
      break;
    }
    case BINARY_STATS:
      BinaryColumnStatsData oldBinaryStatsData = oldStatObj.getStatsData().getBinaryStats();
      BinaryColumnStatsData newBinaryStatsData = newStatObj.getStatsData().getBinaryStats();
      if (newBinaryStatsData.isSetMaxColLen()) {
        oldBinaryStatsData.setMaxColLen(newBinaryStatsData.getMaxColLen());
      }
      if (newBinaryStatsData.isSetAvgColLen()) {
        oldBinaryStatsData.setAvgColLen(newBinaryStatsData.getAvgColLen());
      }
      if (newBinaryStatsData.isSetNumNulls()) {
        oldBinaryStatsData.setNumNulls(newBinaryStatsData.getNumNulls());
      }
      if (newBinaryStatsData.isSetBitVectors()) {
        oldBinaryStatsData.setBitVectors(newBinaryStatsData.getBitVectors());
      }
      break;
    case DECIMAL_STATS: {
      DecimalColumnStatsData oldDecimalStatsData = oldStatObj.getStatsData().getDecimalStats();
      DecimalColumnStatsData newDecimalStatsData = newStatObj.getStatsData().getDecimalStats();
      if (newDecimalStatsData.isSetHighValue()) {
        oldDecimalStatsData.setHighValue(newDecimalStatsData.getHighValue());
      }
      if (newDecimalStatsData.isSetLowValue()) {
        oldDecimalStatsData.setLowValue(newDecimalStatsData.getLowValue());
      }
      if (newDecimalStatsData.isSetNumNulls()) {
        oldDecimalStatsData.setNumNulls(newDecimalStatsData.getNumNulls());
      }
      if (newDecimalStatsData.isSetNumDVs()) {
        oldDecimalStatsData.setNumDVs(newDecimalStatsData.getNumDVs());
      }
      if (newDecimalStatsData.isSetBitVectors()) {
        oldDecimalStatsData.setBitVectors(newDecimalStatsData.getBitVectors());
      }
      break;
    }
    case DATE_STATS: {
      DateColumnStatsData oldDateStatsData = oldStatObj.getStatsData().getDateStats();
      DateColumnStatsData newDateStatsData = newStatObj.getStatsData().getDateStats();
      if (newDateStatsData.isSetHighValue()) {
        oldDateStatsData.setHighValue(newDateStatsData.getHighValue());
      }
      if (newDateStatsData.isSetLowValue()) {
        oldDateStatsData.setLowValue(newDateStatsData.getLowValue());
      }
      if (newDateStatsData.isSetNumNulls()) {
        oldDateStatsData.setNumNulls(newDateStatsData.getNumNulls());
      }
      if (newDateStatsData.isSetNumDVs()) {
        oldDateStatsData.setNumDVs(newDateStatsData.getNumDVs());
      }
      if (newDateStatsData.isSetBitVectors()) {
        oldDateStatsData.setBitVectors(newDateStatsData.getBitVectors());
      }
      break;
    }
    case TIMESTAMP_STATS: {
      TimestampColumnStatsData oldTimestampStatsData = oldStatObj.getStatsData().getTimestampStats();
      TimestampColumnStatsData newTimestampStatsData = newStatObj.getStatsData().getTimestampStats();
      if (newTimestampStatsData.isSetHighValue()) {
        oldTimestampStatsData.setHighValue(newTimestampStatsData.getHighValue());
      }
      if (newTimestampStatsData.isSetLowValue()) {
        oldTimestampStatsData.setLowValue(newTimestampStatsData.getLowValue());
      }
      if (newTimestampStatsData.isSetNumNulls()) {
        oldTimestampStatsData.setNumNulls(newTimestampStatsData.getNumNulls());
      }
      if (newTimestampStatsData.isSetNumDVs()) {
        oldTimestampStatsData.setNumDVs(newTimestampStatsData.getNumDVs());
      }
      if (newTimestampStatsData.isSetBitVectors()) {
        oldTimestampStatsData.setBitVectors(newTimestampStatsData.getBitVectors());
      }
      break;
    }
    default:
      throw new IllegalArgumentException("Unknown stats type: " + typeNew);
    }
  }
}
