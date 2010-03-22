/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

    MedJdbcColumnSet columnSet;
    Set<BitSet> uniqueKeys;

    //~ Constructors -----------------------------------------------------------

    public MedJdbcQueryRel(
        MedJdbcColumnSet columnSet,
        RelOptCluster cluster,
        RelDataType rowType,
        RelOptConnection connection,
        SqlDialect dialect,
        SqlSelect sql)
    {
        this(columnSet, cluster, rowType, connection, dialect, sql, null);
    }

    public MedJdbcQueryRel(
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
        this.columnSet = columnSet;
        this.uniqueKeys = uniqueKeys;
    }

    //~ Methods ----------------------------------------------------------------

    // override JdbcQuery
    public ParseTree implement(JavaRelImplementor implementor)
    {
        Variable connectionVariable =
            new Variable(OJPreparingStmt.connectionVariable);

        SqlString sql = columnSet.directory.normalizeQueryString(queryString);

        Expression allocExpression =
            new CastExpression(
                OJClass.forClass(FarragoStatementAllocation.class),
                new MethodCall(
                    connectionVariable,
                    "getDataServerRuntimeSupport",
                    new ExpressionList(
                        Literal.makeLiteral(
                            columnSet.directory.server.getServerMofId()),
                        Literal.makeLiteral(sql.getSql()))));
        return allocExpression;
    }

    // override JdbcQuery
    public MedJdbcQueryRel clone()
    {
        MedJdbcQueryRel clone =
            new MedJdbcQueryRel(
                columnSet,
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
     * @return the column set accessed by this query
     * (FIXME jvs 21-Mar-2010:  this should be null when we have pushed
     * down a multi-table construct such as join or union; we should
     * be tracking the common name directory or server
     * when the query spans multipl multiple column sets)
     */
    public MedJdbcColumnSet getColumnSet()
    {
        return columnSet;
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        boolean omitServerMofId = false;
        switch(pw.getDetailLevel()) {
        case NO_ATTRIBUTES:
        case EXPPLAN_ATTRIBUTES:
            omitServerMofId = true;
            break;
        }
        if ((columnSet == null)
            || (columnSet.getDirectory() == null)
            || (columnSet.getDirectory().getServer() == null))
        {
            omitServerMofId = true;
        }
        if (omitServerMofId) {
            super.explain(pw);
            return;
        }
        // For plan digests, we need to include the server MOFID
        // so that two identical queries against different servers
        // do not get merged by the optimizer.
        String serverMofId =
            columnSet.getDirectory().getServer().getServerMofId();
        pw.explain(
            this,
            new String[] { "foreignSql", "serverMofId" },
            new Object[] { getForeignSql(), serverMofId });
    }

}

// End MedJdbcQueryRel.java
