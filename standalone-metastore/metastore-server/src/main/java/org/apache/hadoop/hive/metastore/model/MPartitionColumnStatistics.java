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
package org.apache.hadoop.hive.metastore.model;

/**
 * MPartitionColumnStatistics - Represents Hive's partiton level Column Statistics Description.
 * The fields in this class, except the partition, are persisted in the metastore.
 * In case of partition, part_id is persisted in its place.
 */
public class MPartitionColumnStatistics {

  private MPartition partition;
  private String catName;
  private String dbName;
  private String tableName;
  private String partitionName;
  private String colName;
  private String colType;
  private String statistics;
  private String engine;
  private long lastAnalyzed;

  public MPartitionColumnStatistics() {
    // empty constructor
  }

  public MPartition getPartition() {
    return partition;
  }

  public void setPartition(MPartition partition) {
    this.partition = partition;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public String getColType() {
    return colType;
  }

  public void setColType(String colType) {
    this.colType = colType;
  }

  public long getLastAnalyzed() {
    return lastAnalyzed;
  }

  public void setLastAnalyzed(long lastAnalyzed) {
    this.lastAnalyzed = lastAnalyzed;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getCatName() {
    return catName;
  }

  public void setCatName(String catName) {
    this.catName = catName;
  }

  public String getStatistics() {
    return statistics;
  }

  public void setStatistics(String statistics) {
    this.statistics = statistics;
  }

  public String getEngine() {
    return engine;
  }

  public void setEngine(String engine) {
    this.engine = engine;
  }
}
