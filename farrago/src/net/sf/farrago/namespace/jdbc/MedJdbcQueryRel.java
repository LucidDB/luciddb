/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
import org.eigenbase.util.*;


/**
 * MedJdbcQueryRel adapts JdbcQuery to the SQL/MED framework.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedJdbcQueryRel
    extends JdbcQuery
{
    //~ Instance fields --------------------------------------------------------

    MedJdbcColumnSet columnSet;
    RelOptConnection connection;
    SqlDialect dialect;

    //~ Constructors -----------------------------------------------------------

    MedJdbcQueryRel(
        MedJdbcColumnSet columnSet,
        RelOptCluster cluster,
        RelDataType rowType,
        RelOptConnection connection,
        SqlDialect dialect,
        SqlSelect sql)
    {
        super(
            cluster,
            rowType,
            connection,
            dialect,
            sql,
            new JdbcDataSource(""));
        this.columnSet = columnSet;
        this.connection = connection;
        this.dialect = dialect;
    }

    //~ Methods ----------------------------------------------------------------

    // override JdbcQuery
    public ParseTree implement(JavaRelImplementor implementor)
    {
        Variable connectionVariable =
            new Variable(OJPreparingStmt.connectionVariable);

        String sql = columnSet.directory.normalizeQueryString(queryString);

        Expression allocExpression =
            new CastExpression(
                OJClass.forClass(FarragoStatementAllocation.class),
                new MethodCall(
                    connectionVariable,
                    "getDataServerRuntimeSupport",
                    new ExpressionList(
                        Literal.makeLiteral(
                            columnSet.directory.server.getServerMofId()),
                        Literal.makeLiteral(sql))));
        return new MethodCall(
            allocExpression,
            "getResultSet",
            new ExpressionList());
    }

    // override JdbcQuery
    public MedJdbcQueryRel clone()
    {
        MedJdbcQueryRel clone =
            new MedJdbcQueryRel(
                columnSet,
                getCluster(),
                getRowType(),
                connection,
                dialect,
                getSql());
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End MedJdbcQueryRel.java
