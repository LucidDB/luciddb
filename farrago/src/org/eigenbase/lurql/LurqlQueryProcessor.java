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
package org.eigenbase.lurql;

import java.io.*;

import java.sql.*;

import java.util.*;

import javax.jmi.reflect.*;

import org.eigenbase.jmi.*;
import org.eigenbase.lurql.parser.*;

import org.netbeans.api.mdr.*;


/**
 * LurqlQueryProcessor implements the {@link JmiQueryProcessor} interface for
 * LURQL with the following implementation-specific behavior:
 *
 * <ul>
 * <li>parameters are not yet supported
 * <li>multiple threads may execute the same prepared query concurrently
 * <li>the repository must be MDR
 * </ul>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlQueryProcessor
    implements JmiQueryProcessor
{
    //~ Instance fields --------------------------------------------------------

    private final MDRepository repos;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new LurqlQueryProcessor.
     */
    public LurqlQueryProcessor(MDRepository repos)
    {
        this.repos = repos;
    }

    //~ Methods ----------------------------------------------------------------

    // implement JmiQueryProcessor
    public JmiPreparedQuery prepare(JmiModelView modelView, String queryText)
        throws JmiQueryException
    {
        LurqlParser parser = new LurqlParser(new StringReader(queryText));
        LurqlQuery query;
        try {
            query = parser.LurqlQuery();
        } catch (Throwable ex) {
            throw new JmiQueryException("LURQL parse failed", ex);
        }
        LurqlPlan plan =
            new LurqlPlan(
                modelView,
                query);
        return new PreparedQuery(plan);
    }

    //~ Inner Classes ----------------------------------------------------------

    private class PreparedQuery
        implements JmiPreparedQuery
    {
        private final LurqlPlan plan;

        PreparedQuery(LurqlPlan plan)
        {
            this.plan = plan;
        }

        // implement JmiPreparedQuery
        public Map<String, ?> describeParameters()
        {
            return plan.getParamMap();
        }

        // implement JmiPreparedQuery
        public String explainPlan()
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            plan.explain(pw);
            pw.close();
            return sw.toString();
        }

        // implement JmiPreparedQuery
        public Collection<RefObject> execute(
            Connection connection,
            Map<String, ?> args)
            throws JmiQueryException
        {
            LurqlReflectiveExecutor executor =
                new LurqlReflectiveExecutor(repos, plan, connection, args);
            return executor.execute();
        }
    }
}

// End LurqlQueryProcessor.java
