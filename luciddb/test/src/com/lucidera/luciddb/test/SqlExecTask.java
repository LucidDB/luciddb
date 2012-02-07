/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

package com.lucidera.luciddb.test;

import java.util.Properties;

import org.eigenbase.blackhawk.core.AntProperties;
import org.eigenbase.blackhawk.core.test.*;
import org.eigenbase.blackhawk.extension.exectask.junit.*;

/**
 * Uses JUnitRunner to run a junit test.
 **/
public class SqlExecTask extends JunitExecTask
{

    String sqlfile = null;

    public TestLogicTask getTestLogicTask()
    {

        Properties props = getParameters().getAll();
        props.setProperty("sql-file", this.sqlfile);
        BasicInternalParameters params = new BasicInternalParameters(props);

        return new JunitTestLogicTask(
            getTestClassName(),
            params,
            getResultHandler(),
            getMethodNamesToRun(),
            AntProperties.isJunitValidateMethodNamesEnabled(),
            AntProperties.isJunitLogOnlyFailureEnabled());
    }

    public void setFile(String in)
    {
        testClassName = "com.lucidera.luciddb.test.SqlTest";
        this.sqlfile = in;
    }

}
