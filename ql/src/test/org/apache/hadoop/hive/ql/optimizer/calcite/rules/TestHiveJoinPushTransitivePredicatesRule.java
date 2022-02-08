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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HivePlannerContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestHiveRulesUtilities.and;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestHiveRulesUtilities.or;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestHiveRulesUtilities.eq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestHiveRulesUtilities.rexEq;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveJoinPushTransitivePredicatesRule {

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock1;
  @Mock
  RelOptHiveTable tableMock2;
  @Mock
  Table hiveTableMDMock1;
  @Mock
  Table hiveTableMDMock2;

  private HepPlanner planner;
  private RelBuilder builder;
  private RelDataType rowTypeMock1;
  private RelDataType rowTypeMock2;

  @SuppressWarnings("unused")
  private static class MyRecord1 {
    public int f1;
    public int f2;
    public int f3;
    public double f4;
  }

  private static class MyRecord2 {
    public int d1;
    public double d2;
  }

  @Before
  public void before() {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addRuleInstance(HiveJoinPushTransitivePredicatesRule.INSTANCE_JOIN);

    HivePlannerContext context = new HivePlannerContext(null, new HiveRulesRegistry(), null,
        null, null, null);
    planner = new HepPlanner(programBuilder.build(), context);

    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptCluster optCluster = RelOptCluster.create(planner, rexBuilder);
    rowTypeMock1 = typeFactory.createStructType(MyRecord1.class);
    rowTypeMock2 = typeFactory.createStructType(MyRecord2.class);
    doReturn(rowTypeMock1).when(tableMock1).getRowType();
    doReturn(rowTypeMock2).when(tableMock2).getRowType();
    LogicalTableScan tableScan1 = LogicalTableScan.create(optCluster, tableMock1, Collections.emptyList());
    doReturn(tableScan1).when(tableMock1).toRel(ArgumentMatchers.any());
    doReturn(tableMock1).when(schemaMock).getTableForMember(ImmutableList.of("t1"));
    lenient().doReturn(hiveTableMDMock1).when(tableMock1).getHiveTableMD();
    LogicalTableScan tableScan2 = LogicalTableScan.create(optCluster, tableMock2, Collections.emptyList());
    doReturn(tableScan2).when(tableMock2).toRel(ArgumentMatchers.any());
    doReturn(tableMock2).when(schemaMock).getTableForMember(ImmutableList.of("t2"));
    lenient().doReturn(hiveTableMDMock2).when(tableMock2).getHiveTableMD();

    builder = HiveRelFactories.HIVE_BUILDER.create(optCluster, schemaMock);
  }

  @Test
  public void testSimpleCase() {

    // @formatter:off
    final RelNode basePlan = builder
        .scan("t1")
        .filter(
          and(builder,
            eq(builder, "f1",1),
            eq(builder, "f2",3)
          )
        )
        .scan("t2")
        .filter(
            eq(builder, "d2",5.0)
        )
        .join(JoinRelType.INNER,
            rexEq(builder, rowTypeMock1, 0, rowTypeMock2, 4)) // t1.f1 = t2.d1
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveJoin join = (HiveJoin) optimizedRelNode;
    String expectedPlan = "HiveJoin(condition=[=($0, $2)], joinType=[inner], algorithm=[none], cost=[not available])\n"
        + "  HiveFilter(condition=[=($2, 1)])\n"
        + "    HiveFilter(condition=[AND(=($0, 1), =($1, 3))])\n"
        + "      LogicalTableScan(table=[[]])\n"
        + "  HiveFilter(condition=[=($3, 5.0E0:DOUBLE)])\n"
        + "    LogicalTableScan(table=[[]])";

    assertEquals(expectedPlan, RelOptUtil.toString(join));
  }

  @Test
  public void testCaseInAndEqualsWithConstantsOfDifferentType() {

    // @formatter:off
    final RelNode basePlan = builder
        .scan("t")
        .filter(
            and(builder,
                or(builder,
                    eq(builder, "f1",1),
                    eq(builder, "f1",2)
                ),
                eq(builder, "f1",1),
                or(builder,
                    eq(builder, "f4",3.0),
                    eq(builder, "f4",4.1)
                ),
                eq(builder, "f4",4.1)
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("AND(=($0, 1), =($3, 4.1E0:DOUBLE))", condition.toString());
  }

}
