/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.lucidera.lurql;

import com.lucidera.lurql.parser.*;

import java.io.*;

import java.sql.*;

import java.util.*;

import org.eigenbase.jmi.*;

import org.netbeans.api.mdr.*;

import javax.jmi.reflect.RefObject;


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
        LurqlPlan plan = new LurqlPlan(
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
        public Map describeParameters()
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
            Map<String,?> args)
            throws JmiQueryException
        {
            LurqlReflectiveExecutor executor =
                new LurqlReflectiveExecutor(repos, plan, connection, args);
            return executor.execute();
        }
    }
}

// End LurqlQueryProcessor.java
