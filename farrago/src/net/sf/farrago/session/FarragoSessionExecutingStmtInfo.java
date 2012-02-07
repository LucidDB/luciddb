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
package net.sf.farrago.session;

import java.util.*;


/**
 * FarragoSessionExecuctingStmtInfo contains information about executing
 * statements.
 */
public interface FarragoSessionExecutingStmtInfo
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the executing statement itself, as a FarragoSessionStmtContext
     */
    FarragoSessionStmtContext getStmtContext();

    /**
     * Returns the unique identifier for this executing statement.
     *
     * @return Unique statement ID
     */
    long getId();

    /**
     * Returns the SQL statement being executed.
     *
     * @return SQL statement
     */
    String getSql();

    /**
     * Returns any dynamic parameters used to execute this statement.
     *
     * @return List of dynamic parameters to the statement
     */
    List<Object> getParameters();

    /**
     * Returns time the statement began executing, in ms.
     *
     * @return Start time in ms
     */
    long getStartTime();

    /**
     * Returns an array of catalog object mofIds in use by this statement.
     *
     * @return List of catalog object mofIds
     */
    List<String> getObjectsInUse();
}

// End FarragoSessionExecutingStmtInfo.java
