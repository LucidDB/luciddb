/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.rel.jdbc;

import javax.sql.DataSource;

import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rel.ResultSetRel;
import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.util.JdbcDataSource;
import org.eigenbase.util.Util;


/**
 * A <code>JdbcQuery</code> is a relational expression whose source is a SQL
 * statement executed against a JDBC data source. It has {@link
 * CallingConvention#RESULT_SET result set calling convention}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 2 August, 2002
 */
public class JdbcQuery extends AbstractRelNode implements ResultSetRel
{
    //~ Instance fields -------------------------------------------------------

    public final DataSource dataSource;

    /** The expression which yields the connection object. */
    protected RelOptConnection connection;
    SqlDialect dialect;
    SqlSelect sql;

    /** For debug. Set on register. */
    protected String queryString;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>JdbcQuery</code>.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param connection a {@link RelOptConnection};
     *       must also implement {@link DataSource}, because that's how we will
     *       acquire the JDBC connection
     * @param sql SQL parse tree, may be null, otherwise must be a SELECT
     *       statement
     * @param dataSource Provides a JDBC connection to run this query against.
     *       If the query is implementing a JDBC table, then the connection's
     *       schema will implement {@link net.sf.saffron.ext.JdbcSchema}, and
     *       data source will typically be the same as calling
     *       {@link net.sf.saffron.ext.JdbcSchema#getDataSource}. But non-JDBC
     *       schemas are also acceptable.
     *
     * @pre connection != null
     * @pre sql == null || sql.isA(SqlNode.Kind.Select)
     * @pre dataSource != null
     */
    public JdbcQuery(
        RelOptCluster cluster,
        RelDataType rowType,
        RelOptConnection connection,
        SqlDialect dialect,
        SqlSelect sql,
        DataSource dataSource)
    {
        super(cluster);
        Util.pre(connection != null, "connection != null");
        Util.pre(dataSource != null, "dataSource != null");
        this.rowType = rowType;
        this.connection = connection;
        this.dialect = dialect;
        if (sql == null) {
            sql = SqlStdOperatorTable.instance().selectOperator.createCall(null,
                null, null, null, null, null, null, null,
                ParserPosition.ZERO);
        } else {
            Util.pre(
                sql.isA(SqlKind.Select),
                "sql == null || sql.isA(SqlNode.Kind.Select)");
        }
        this.sql = sql;
        this.dataSource = dataSource;
    }

    //~ Methods ---------------------------------------------------------------

    public RelOptConnection getConnection()
    {
        return connection;
    }

    public CallingConvention getConvention()
    {
        return CallingConvention.RESULT_SET;
    }

    public String getQualifier()
    {
        if (queryString == null) {
            queryString = sql.toSqlString(dialect);
        }
        return "[" + queryString + "]";
    }

    public Object clone()
    {
        return new JdbcQuery(cluster, rowType, connection, dialect,
            (SqlSelect) sql.clone(), dataSource);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // Very difficult to estimate the cost of a remote query: (a) we don't
        // know what plans are available to the remote RDBMS, (b) we don't
        // know relative speed of the other CPU, or the bandwidth. This
        // estimate selfishly deals with the cost to THIS system, but it still
        // neglects the effects of latency.
        double rows = getRows() / 2;

        // Very difficult to estimate the cost of a remote query: (a) we don't
        // know what plans are available to the remote RDBMS, (b) we don't
        // know relative speed of the other CPU, or the bandwidth. This
        // estimate selfishly deals with the cost to THIS system, but it still
        // neglects the effects of latency.
        double cpu = 0;

        // Very difficult to estimate the cost of a remote query: (a) we don't
        // know what plans are available to the remote RDBMS, (b) we don't
        // know relative speed of the other CPU, or the bandwidth. This
        // estimate selfishly deals with the cost to THIS system, but it still
        // neglects the effects of latency.
        double io = 0 /*rows*/;
        return planner.makeCost(rows, cpu, io);
    }

    public void onRegister(RelOptPlanner planner)
    {
        super.onRegister(planner);
        Util.discard(getQualifier()); // compute query string now
    }

    public static void register(RelOptPlanner planner)
    {
        // FIXME jvs 29-Aug-2004

        /*
        planner.addRule(new TableAccessToQueryRule());
        */
        planner.addRule(new AddFilterToQueryRule());
        planner.addRule(new AddProjectToQueryRule());
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        // Generate
        //   ((javax.sql.DataSource) connection).getConnection().
        //       createStatement().executeQuery(<<query string>>);
        //
        // The above assumes that the datasource expression is the default,
        // namely
        //
        //   (javax.sql.DataSource) connection
        //
        // Issue#1. We should really wrap this in
        //
        // Statement statement = null;
        // try {
        //   ...
        //   statement = connection.getConnection.createStatement();
        //   ...
        // } catch (java.sql.SQLException e) {
        //    throw new saffron.runtime.SaffronError(e);
        // } finally {
        //    if (stmt != null) {
        //       try {
        //          stmt.close();
        //       } catch {}
        //    }
        // }
        //
        // This is all a horrible hack. Need a way to 'freeze' a DataSource
        // into a Java expression which can be 'thawed' into a DataSource
        // at run-time. We should use the OJConnectionRegistry somehow.
        assert dataSource instanceof JdbcDataSource; // hack

        // DriverManager.getConnection("jdbc...", "scott", "tiger");
        final String url = ((JdbcDataSource) dataSource).url;
        final MethodCall connectionExpr =
            new MethodCall(
                OJUtil.typeNameForClass(java.sql.DriverManager.class),
                "getConnection",
                new ExpressionList(
                    Literal.makeLiteral(url),
                    Literal.makeLiteral("SA"),
                    Literal.makeLiteral("")));
        return new MethodCall(
            new MethodCall(connectionExpr, "createStatement", null),
            "executeQuery",
            new ExpressionList(Literal.makeLiteral(queryString)));
    }
}


// End JdbcQuery.java
