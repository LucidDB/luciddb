/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.namespace.jdbc;

import java.sql.*;

import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRelImplementor;
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
class MedJdbcQueryRel extends JdbcQuery
{
    //~ Instance fields -------------------------------------------------------

    private MedJdbcColumnSet columnSet;

    //~ Constructors ----------------------------------------------------------

    MedJdbcQueryRel(
        MedJdbcColumnSet columnSet,
        RelOptCluster cluster,
        RelDataType rowType,
        RelOptConnection connection,
        SqlDialect dialect,
        SqlSelect sql)
    {
        super(cluster, rowType, connection, dialect, sql,
            new JdbcDataSource(""));
        this.columnSet = columnSet;
    }

    //~ Methods ---------------------------------------------------------------

    // override JdbcQuery
    public ParseTree implement(JavaRelImplementor implementor)
    {
        Variable connectionVariable =
            new Variable(OJStatement.connectionVariable);

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
}


// End MedJdbcQueryRel.java
