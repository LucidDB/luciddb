/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.jmi;

import javax.jmi.reflect.RefObject;
import java.sql.*;

import java.util.*;

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
    public Map describeParameters();

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
        Map<String,?> args)
        throws JmiQueryException;
}

// End JmiPreparedQuery.java
