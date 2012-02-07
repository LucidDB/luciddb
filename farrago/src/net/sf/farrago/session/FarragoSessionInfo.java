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


public interface FarragoSessionInfo
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return the session itself.
     */
    FarragoSession getSession();

    /**
     * Returns unique identifier for this session.
     *
     * @return unique session identifier
     */
    long getId();

    /**
     * Returns a list of identifiers of currently running statements.
     *
     * @return List of unique statement identifiers
     */
    List<Long> getExecutingStmtIds();

    /**
     * Given a statement identifier, return an object containing its details.
     *
     * @param id Unique identifier of a statement
     *
     * @return FarragoSessionExecutingStmtInfo containing statement details
     */
    FarragoSessionExecutingStmtInfo getExecutingStmtInfo(Long id);
}

// End FarragoSessionInfo.java
