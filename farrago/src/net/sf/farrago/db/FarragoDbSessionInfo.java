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
import java.util.concurrent.*;

import net.sf.farrago.session.*;


/**
 * Implements the {@link FarragoSessionInfo} interface in the context of a
 * {@link FarragoDbSession}.
 *
 * @author Jason Ouellette
 * @version $Id$
 */
public class FarragoDbSessionInfo
    implements FarragoSessionInfo
{
    //~ Instance fields --------------------------------------------------------

    private long id;
    private FarragoSession session;
    private FarragoDatabase database;
    private Map<Long, FarragoSessionExecutingStmtInfo> statements;
    // REVIEW mberkowitz 28-Mar-2006: maybe have 1 map id->info in
    // FarragoDatabase.

    //~ Constructors -----------------------------------------------------------

    FarragoDbSessionInfo(FarragoSession session, FarragoDatabase database)
    {
        this.id = database.getUniqueId();
        this.session = session;
        this.database = database;
        statements =
            new ConcurrentHashMap<Long, FarragoSessionExecutingStmtInfo>();
    }

    //~ Methods ----------------------------------------------------------------

    public FarragoSession getSession()
    {
        return session;
    }

    FarragoDatabase getDatabase()
    {
        return database;
    }

    public long getId()
    {
        return id;
    }

    // implement FarragoSessionInfo
    public List<Long> getExecutingStmtIds()
    {
        Set<Long> s = statements.keySet();
        Long [] k = statements.keySet().toArray(new Long[s.size()]);
        return Collections.unmodifiableList(Arrays.asList(k));
    }

    // implement FarragoSessionInfo
    public FarragoSessionExecutingStmtInfo getExecutingStmtInfo(Long id)
    {
        return statements.get(id);
    }

    /**
     * Adds a running statement.
     *
     * @param info Info object for the running statement
     */
    public void addExecutingStmtInfo(FarragoSessionExecutingStmtInfo info)
    {
        statements.put(
            info.getId(),
            info);
    }

    /**
     * Removes a running statement.
     *
     * @param id Unique identifier of a running statement
     */
    public void removeExecutingStmtInfo(long id)
    {
        statements.remove(id);
    }
}

// End FarragoDbSessionInfo.java
