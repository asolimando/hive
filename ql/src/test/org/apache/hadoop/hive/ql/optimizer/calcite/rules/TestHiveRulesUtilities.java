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

package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import javolution.testing.AssertionException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public class TestHiveRulesUtilities {

  private TestHiveRulesUtilities() {
    // to prevent instantiation via reflection
    throw new AssertionException("TestHiveRulesUtilities utility class should not be instantiated");
  }

  public static RexNode or(RelBuilder builder, RexNode... args) {
    return builder.call(SqlStdOperatorTable.OR, args);
  }

  public static RexNode and(RelBuilder builder, RexNode... args) {
    return builder.call(SqlStdOperatorTable.AND, args);
  }

  public static RexNode eq(RelBuilder builder, String field, Number value) {
    return builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(field), builder.literal(value));
  }

  public static RexNode rexEq(RelBuilder builder, RelDataType relDataType1, int iref1,
      RelDataType relDataType2, int iref2) {
    RexBuilder rexBuilder = builder.getRexBuilder();
    return rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(relDataType1, iref1),
        rexBuilder.makeInputRef(relDataType2, iref2));
  }
}
