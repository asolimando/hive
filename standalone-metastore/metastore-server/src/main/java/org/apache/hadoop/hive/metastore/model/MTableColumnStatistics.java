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
 *
 * MTableColumnStatistics - Represents Hive's Column Statistics Description. The fields in this
 * class with the exception of table are persisted in the metastore. In case of table, tbl_id is
 * persisted in its place.
 *
 */
public class MTableColumnStatistics {

  private MTable table;
  private String catName;
  private String dbName;
  private String tableName;
  private String colName;
  private String colType;
  private String engine;
  private String statistics;
  private long lastAnalyzed;

  public MTableColumnStatistics() {}

  public MTable getTable() {
    return table;
  }

  public void setTable(MTable table) {
    this.table = table;
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
