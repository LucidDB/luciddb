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
package net.sf.farrago.db;

import java.util.*;

import net.sf.farrago.session.*;


/**
 * Implements the {@link FarragoSessionExecutingStmtInfo} interface in the
 * context of a {@link FarragoDbStmtContext}.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class FarragoDbSessionExecutingStmtInfo
    implements FarragoSessionExecutingStmtInfo
{
    //~ Instance fields --------------------------------------------------------

    private long id;
    private FarragoSessionStmtContext stmt;
    private FarragoDatabase database;
    private String sql;
    private long startTime;
    private List<Object> parameters;
    private List<String> objectsInUse;

    //~ Constructors -----------------------------------------------------------

    FarragoDbSessionExecutingStmtInfo(
        FarragoSessionStmtContext stmt,
        FarragoDatabase database,
        String sql,
        List<Object> parameters,
        List<String> objectsInUse)
    {
        this.stmt = stmt;
        this.database = database;
        this.id = database.getUniqueId();
        this.sql = sql;
        this.startTime = System.currentTimeMillis();
        this.parameters = Collections.unmodifiableList(parameters);
        this.objectsInUse = Collections.unmodifiableList(objectsInUse);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionExecutingStmtInfo
    public FarragoSessionStmtContext getStmtContext()
    {
        return stmt;
    }

    FarragoDatabase getDatabase()
    {
        return database;
    }

    // implement FarragoSessionExecutingStmtInfo
    public long getId()
    {
        return id;
    }

    // implement FarragoSessionExecutingStmtInfo
    public String getSql()
    {
        return sql;
    }

    // implement FarragoSessionExecutingStmtInfo
    public List<Object> getParameters()
    {
        return parameters;
    }

    // implement FarragoSessionExecutingStmtInfo
    public long getStartTime()
    {
        return startTime;
    }

    // implement FarragoSessionExecutingStmtInfo
    public List<String> getObjectsInUse()
    {
        return objectsInUse;
    }
}

// End FarragoDbSessionExecutingStmtInfo.java
