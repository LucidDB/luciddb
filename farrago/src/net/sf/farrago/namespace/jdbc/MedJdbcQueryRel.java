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
package net.sf.farrago.namespace.jdbc;

import net.sf.farrago.util.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.SqlString;
import org.eigenbase.util.*;

import java.util.*;

/**
 * MedJdbcQueryRel adapts JdbcQuery to the SQL/MED framework.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedJdbcQueryRel
    extends JdbcQuery
{
    //~ Instance fields --------------------------------------------------------

    MedJdbcDataServer server;
    MedJdbcColumnSet columnSet;
    Set<BitSet> uniqueKeys;

    //~ Constructors -----------------------------------------------------------

    public MedJdbcQueryRel(
        MedJdbcDataServer server,
        MedJdbcColumnSet columnSet,
        RelOptCluster cluster,
        RelDataType rowType,
        RelOptConnection connection,
        SqlDialect dialect,
        SqlSelect sql)
    {
        this(
            server, columnSet, cluster, rowType, connection, dialect,
            sql, null);
    }

    public MedJdbcQueryRel(
        MedJdbcDataServer server,
        MedJdbcColumnSet columnSet,
        RelOptCluster cluster,
        RelDataType rowType,
        RelOptConnection connection,
        SqlDialect dialect,
        SqlSelect sql,
        Set<BitSet> uniqueKeys)
    {
        super(
            cluster,
            rowType,
            connection,
            dialect,
            sql,
            new JdbcDataSource(""));
        this.server = server;
        this.columnSet = columnSet;
        this.uniqueKeys = uniqueKeys;
    }

    //~ Methods ----------------------------------------------------------------

    // override JdbcQuery
    public ParseTree implement(JavaRelImplementor implementor)
    {
        Variable connectionVariable =
            new Variable(OJPreparingStmt.connectionVariable);

        SqlString sql = MedJdbcNameDirectory.normalizeQueryString(queryString);

        Expression allocExpression =
            new CastExpression(
                OJClass.forClass(FarragoStatementAllocation.class),
                new MethodCall(
                    connectionVariable,
                    "getDataServerRuntimeSupport",
                    new ExpressionList(
                        Literal.makeLiteral(
                            server.getServerMofId()),
                        Literal.makeLiteral(sql.getSql()))));
        return allocExpression;
    }

    // override JdbcQuery
    public MedJdbcQueryRel clone()
    {
        MedJdbcQueryRel clone =
            new MedJdbcQueryRel(
                getServer(),
                getColumnSet(),
                getCluster(),
                getRowType(),
                getConnection(),
                getDialect(),
                getSql(),
                uniqueKeys);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    /**
     * @return the server accessed by this query
     */
    public MedJdbcDataServer getServer()
    {
        return server;
    }

    /**
     * @return the column set accessed by this query, or null
     * if it accesses more than one column set
     */
    public MedJdbcColumnSet getColumnSet()
    {
        return columnSet;
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        boolean omitServerMofId = false;
        switch (pw.getDetailLevel()) {
        case NO_ATTRIBUTES:
        case EXPPLAN_ATTRIBUTES:
            omitServerMofId = true;
            break;
        }
        if (server == null) {
            omitServerMofId = true;
        }
        if (omitServerMofId) {
            super.explain(pw);
            return;
        }
        // For plan digests, we need to include the server MOFID
        // so that two identical queries against different servers
        // do not get merged by the optimizer.
        String serverMofId = server.getServerMofId();
        pw.explain(
            this,
            new String[] { "foreignSql", "serverMofId" },
            new Object[] { getForeignSql(), serverMofId });
    }

}

// End MedJdbcQueryRel.java
