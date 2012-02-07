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
package org.eigenbase.jmi;

import java.sql.*;

import java.util.*;

import javax.jmi.reflect.*;

// REVIEW jvs 21-May-2005:  Make this a heavyweight allocation?


/**
 * JmiPreparedQuery represents a prepared query returned by {@link
 * JmiQueryProcessor}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface JmiPreparedQuery
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Gets a description of the parameters to this query.
     *
     * @return map from parameter name (String) to expected type (type
     * representation is implementation-dependent)
     */
    public Map<String, ?> describeParameters();

    /**
     * Gets an explanation of the plan to be used to execute this query.
     *
     * @return plan text; representation is implementation-defined, typically
     * multi-line
     */
    public String explainPlan();

    /**
     * Executes the prepared query. Whether it is legal to simultaneously
     * execute the same query from different threads is
     * implementation-dependent. The query transaction scope is determined by
     * the current thread context and JMI implementation.
     *
     * @param connection JDBC connection to use for processing SQL subqueries,
     * or null if none available
     * @param args map from parameter name (String) to argument value (allowable
     * value type is implementation-dependent)
     *
     * @return collection of JMI objects (instances of {@link RefObject}) found
     * by the query
     */
    public Collection<RefObject> execute(
        Connection connection,
        Map<String, ?> args)
        throws JmiQueryException;
}

// End JmiPreparedQuery.java
