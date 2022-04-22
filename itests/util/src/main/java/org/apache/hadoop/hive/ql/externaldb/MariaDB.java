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
package org.apache.hadoop.hive.ql.externaldb;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MariaDB extends AbstractExternalDB {

  @Override
  public String getRootUser() {
    return "root";
  }

  @Override
  public String getJdbcUrl() throws IOException, InterruptedException {
    return "jdbc:mariadb://" + getContainerHostAddress() + ":" + getContainerHostPorts()[0] + "/" + DB_NAME;
  }
  
  public String getJdbcDriver() {
    return "org.mariadb.jdbc.Driver";
  }

  public String getDockerImageName() { return "mariadb:10.2"; }

  public String[] getDockerAdditionalArgs() {
    return new String[] {"-P",
        "-e", "MARIADB_ROOT_PASSWORD=" + getRootPassword(),
        "-e", "MARIADB_DATABASE=" + DB_NAME,
        "-d"
    };
  }

  public boolean isContainerReady(ProcessResults pr) {
    Pattern pat = Pattern.compile("ready for connections");
    Matcher m = pat.matcher(pr.stderr);
    return m.find() && m.find();
  }
}
