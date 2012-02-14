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
package net.sf.farrago.query;

import java.sql.*;
import java.util.List;

import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.oj.stmt.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FarragoExecutableExplainStmt implements FarragoSessionExecutableStmt for an
 * EXPLAIN PLAN statement.
 *
 * <p>NOTE: be sure to read superclass warnings before modifying this class.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoExecutableExplainStmt
    extends FarragoExecutableStmtImpl
{
    //~ Instance fields --------------------------------------------------------

    private final String explanation;

    //~ Constructors -----------------------------------------------------------

    FarragoExecutableExplainStmt(
        RelDataType dynamicParamRowType,
        String explanation)
    {
        super(
            dynamicParamRowType,
            false,
            null,
            new TableAccessMap());

        this.explanation = explanation;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionExecutableStmt
    public RelDataType getRowType()
    {
        // TODO:  make a proper type descriptor (and use it for execute also)
        throw new UnsupportedOperationException();
    }

    // implement FarragoSessionExecutableStmt
    public List<List<String>> getFieldOrigins()
    {
        throw new UnsupportedOperationException();
    }

    // implement FarragoSessionExecutableStmt
    public ResultSet execute(FarragoSessionRuntimeContext runtimeContext)
    {
        // don't need a context or repository session
        runtimeContext.closeAllocation();
        runtimeContext.getSession().getRepos().endReposSession();
        return PreparedExplanation.executeStatic(explanation);
    }

    // implement FarragoSessionExecutableStmt
    public long getMemoryUsage()
    {
        return FarragoUtil.getStringMemoryUsage(explanation);
    }
}

// End FarragoExecutableExplainStmt.java
