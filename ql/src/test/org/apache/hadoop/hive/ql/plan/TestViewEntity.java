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
package org.apache.hadoop.hive.ql.plan;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestViewEntity {
  /**
   * Hook used in the test to capture the set of ReadEntities
   */
  public static class CheckInputReadEntity extends
      AbstractSemanticAnalyzerHook {
    public static ReadEntity[] readEntities;

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
        List<Task<?>> rootTasks) throws SemanticException {
      readEntities = context.getInputs().toArray(new ReadEntity[0]);
    }

  }

  private static Driver driver;
  private final String NAME_PREFIX = "TestViewEntity5".toLowerCase();

  @BeforeClass
  public static void onetimeSetup() throws Exception {
    HiveConf conf = new HiveConfForTest(TestViewEntity.class);
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.setVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK,
        CheckInputReadEntity.class.getName());
    HiveConf
        .setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    SessionState.start(conf);
    driver = new Driver(conf);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    driver.close();
    driver.destroy();
  }

  /**
   * Verify that the parent entities are captured correctly for union views
   * @throws Exception
   */
  @Test
  public void testUnionView() throws Exception {
    String prefix = "tunionview" + NAME_PREFIX;
    final String tab1 = prefix + "t1";
    final String tab2 = prefix + "t2";
    final String view1 = prefix + "v1";
    driver.run("create table " + tab1 + "(id int)");
    driver.run("create table " + tab2 + "(id int)");
    driver.run("create view " + view1 + " as select t.id from "
            + "(select " + tab1 + ".id from " + tab1 + " union all select " + tab2 + ".id from " + tab2 + ") as t");

    driver.compile("select * from " + view1, true);
    // view entity
    assertEquals("default@" + view1, CheckInputReadEntity.readEntities[0].getName());

    // first table in union query with view as parent
    assertEquals("default@" + tab1, CheckInputReadEntity.readEntities[1].getName());
    assertFalse("Table is not direct input", CheckInputReadEntity.readEntities[1].isDirect());
    assertEquals("default@" + view1, CheckInputReadEntity.readEntities[1]
        .getParents()
        .iterator().next().getName());
    // second table in union query with view as parent
    assertEquals("default@" + tab2, CheckInputReadEntity.readEntities[2].getName());
    assertFalse("Table is not direct input", CheckInputReadEntity.readEntities[2].isDirect());
    assertEquals("default@" + view1, CheckInputReadEntity.readEntities[2]
        .getParents()
        .iterator().next().getName());

  }


  /**
   * Verify that the parent entities are captured correctly for view in subquery
   * @throws Exception
   */
  @Test
  public void testViewInSubQuery() throws Exception {
    String prefix = "tvsubquery" + NAME_PREFIX;
    final String tab1 = prefix + "t";
    final String view1 = prefix + "v";

    driver.run("create table " + tab1 + "(id int)");
    driver.run("create view " + view1 + " as select * from " + tab1);

    driver.compile("select * from " + view1, true);
    // view entity
    assertEquals("default@" + view1, CheckInputReadEntity.readEntities[0].getName());

    // table as second read entity
    assertEquals("default@" + tab1, CheckInputReadEntity.readEntities[1].getName());
    assertFalse("Table is not direct input", CheckInputReadEntity.readEntities[1].isDirect());

  }


  /**
   * Verify that the parent entities are captured correctly for view in subquery with WHERE
   * subquery referencing a view. Optimizer: Cost-based
   * @throws Exception
   */
  @Test
  public void testViewInSubQueryWithWhereClauseCbo() throws Exception {
    driver.getConf().setBoolVar(HiveConf.ConfVars.HIVE_CBO_ENABLED, true);
    testViewInSubQueryWithWhereClause();
  }

  /**
   * Verify that the parent entities are captured correctly for view in subquery with WHERE
   * subquery referencing a view. Optimizer: Rule-based
   *
   * @throws Exception
   */
  @Test
  public void testViewInSubQueryWithWhereClauseRbo() throws Exception {
    driver.getConf().setBoolVar(HiveConf.ConfVars.HIVE_CBO_ENABLED, false);
    testViewInSubQueryWithWhereClause();
  }

  private void testViewInSubQueryWithWhereClause() throws CommandProcessorException {
    String prefix = "tvsubquerywithwhereclause" + NAME_PREFIX;
    final String tab1 = prefix + "t";
    final String view1 = prefix + "v";
    final String view2 = prefix + "v2";
    final String tab1row1 = "'x','y','z'";
    final String tab1row2 = "'a','b','c'";

    //drop all if exists
    driver.run("drop table if exists " + tab1);
    driver.run("drop view if exists " + view1);
    driver.run("drop view if exists " + view2);

    //create tab1
    driver.run("create table " + tab1 + "(col1 string, col2 string, col3 string)");
    driver.run("insert into " + tab1 + " values (" + tab1row1 + ")");

    //create view1
    driver.run("create view " + view1 + " as select " +
        tab1 + ".col1, " + tab1 + ".col2, " + tab1 + ".col3 " +
        " from " + tab1);

    driver.run("insert into " + tab1 + " values (" + tab1row2 + ")");

    //create view2
    driver.run(
        "create view " + view2 + " as select " +
            tab1 + ".col1, " + tab1 + ".col2, " + tab1 + ".col3 " +
            " from " + tab1 +
            " where " + tab1 + ".col1 NOT IN (" +
            "SELECT " + view1 + ".col1 FROM " + view1 + ")");

    //select from view2
    driver.compile("select * from " + view2, true);

    //verify that only view2 is direct input in above query
    ReadEntity[] readEntities = CheckInputReadEntity.readEntities;
    for (ReadEntity readEntity : readEntities) {
      String name = readEntity.getName();
      if (name.equals("default@" + tab1)) {
        assertFalse("Table should not be direct input", readEntity.isDirect());
      } else if (name.equals("default@" + view1)) {
        assertFalse("View1 should not be direct input", readEntity.isDirect());
      } else if (name.equals("default@" + view2)) {
        assertTrue("View2 should be direct input", readEntity.isDirect());
      } else {
        fail("Unrecognized ReadEntity input");
      }
    }
  }


  /**
   * Verify that the the query with the subquery inside a view will have the correct
   * direct and indirect inputs.
   * @throws Exception
   */
  @Test
  public void testSubQueryInSubView() throws Exception {
    String prefix = "tvsubqueryinsubview" + NAME_PREFIX;
    final String tab1 = prefix + "t";
    final String view1 = prefix + "v";
    final String view2 = prefix + "v2";

    driver.run("create table " + tab1 + "(id int)");
    driver.run("create view " + view1 + " as select * from " + tab1);

    driver.run("create view " + view2 + " as select * from (select * from " + view1 + ") x");

    driver.compile("select * from " + view2, true);
    // view entity
    assertEquals("default@" + view2, CheckInputReadEntity.readEntities[0].getName());

    // table1 and view1 as second read entity
    assertEquals("default@" + view1, CheckInputReadEntity.readEntities[1].getName());
    assertFalse("Table is not direct input", CheckInputReadEntity.readEntities[1].isDirect());
    Set<ReadEntity> parents = CheckInputReadEntity.readEntities[1].getParents();
    assertTrue("Table does not have parent", parents != null && parents.size() > 0);
    assertEquals("default@" + tab1, CheckInputReadEntity.readEntities[2].getName());
    assertFalse("Table is not direct input", CheckInputReadEntity.readEntities[2].isDirect());

  }

  /**
   * Verify that the the query with the subquery inside a view will have the correct
   * direct and indirect inputs.
   * @throws Exception
   */
  @Test
  public void testUnionAllInSubView() throws Exception {
    String prefix = "tvunionallinsubview" + NAME_PREFIX;
    final String tab1 = prefix + "t";
    final String view1 = prefix + "v";
    final String view2 = prefix + "v2";

    driver.run("create table " + tab1 + "(id int)");
    driver.run("create view " + view1 + " as select * from " + tab1);

    driver.run("create view " + view2 + " as select * from (select * from " + view1 + " union all select * from " + view1 + ") x");

    driver.compile("select * from " + view2, true);
    // view entity
    assertEquals("default@" + view2, CheckInputReadEntity.readEntities[0].getName());

    // table1 and view1 as second read entity
    assertEquals("default@" + view1, CheckInputReadEntity.readEntities[1].getName());
    assertFalse("Table is not direct input", CheckInputReadEntity.readEntities[1].isDirect());
    Set<ReadEntity> parents = CheckInputReadEntity.readEntities[1].getParents();
    assertTrue("Table does not have parent", parents != null && parents.size() > 0);
    assertEquals("default@" + tab1, CheckInputReadEntity.readEntities[2].getName());
    assertFalse("Table is not direct input", CheckInputReadEntity.readEntities[2].isDirect());

  }

  /**
   * Verify create/alter view on another view's underlying table is always indirect
   * direct and indirect inputs.
   * @throws CommandProcessorException
   */
  @Test
  public void alterView() throws CommandProcessorException {

    driver.run("create table test_table (id int)");
    driver.run("create view test_view as select * from test_table");


    driver.compile("create view test_view_1 as select * from test_view", true);
    assertEquals("default@test_view", CheckInputReadEntity.readEntities[0].getName());
    assertTrue("default@test_view", CheckInputReadEntity.readEntities[0].isDirect());
    assertEquals("default@test_table", CheckInputReadEntity.readEntities[1].getName());
    assertFalse("default@test_table", CheckInputReadEntity.readEntities[1].isDirect());

    driver.run("create view test_view_1 as select * from test_view");

    driver.compile("alter view test_view_1 as select * from test_view", true);
    assertEquals("default@test_view", CheckInputReadEntity.readEntities[0].getName());
    assertTrue("default@test_view", CheckInputReadEntity.readEntities[0].isDirect());
    assertEquals("default@test_table", CheckInputReadEntity.readEntities[1].getName());
    assertFalse("default@test_table", CheckInputReadEntity.readEntities[1].isDirect());
  }
}
