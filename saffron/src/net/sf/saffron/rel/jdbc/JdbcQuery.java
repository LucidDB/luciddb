/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.rel.jdbc;

import net.sf.saffron.core.SaffronConnection;
import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.oj.OJConnectionRegistry;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.PlanCost;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.sql.*;
import net.sf.saffron.util.JdbcDataSource;
import net.sf.saffron.util.Util;
import openjava.ptree.ExpressionList;
import openjava.ptree.Literal;
import openjava.ptree.MethodCall;
import openjava.ptree.TypeName;

import javax.sql.DataSource;


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
public class JdbcQuery extends SaffronRel
{
    public final DataSource dataSource;
    //~ Instance fields -------------------------------------------------------

    public SaffronConnection getConnection() {
        return connection;
    }

    /** The expression which yields the connection object. */
    protected SaffronConnection connection;
    SqlDialect dialect;
    SqlSelect sql;

    /** For debug. Set on register. */
    protected String queryString;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>JdbcQuery</code>.
     *
     * @param cluster {@link VolcanoCluster} this relational expression
     *        belongs to
     * @param connection a {@link SaffronConnection};
     *       must also implement {@link DataSource}, because that's how we will
     *       acquire the JDBC connection;
     *       if you are generating code, the must have registered the connection
     *       with {@link OJConnectionRegistry}
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
        VolcanoCluster cluster,
        SaffronType rowType,
        SaffronConnection connection,
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
        if (sql != null) {
            Util.pre(sql.isA(SqlKind.Select),
                    "sql == null || sql.isA(SqlNode.Kind.Select)");
        } else {
            sql = (SqlSelect) SqlOperatorTable.instance().selectOperator
                    .createCall(new SqlNode[SqlSelect.OPERAND_COUNT]);
        }
        this.sql = sql;
        this.dataSource = dataSource;
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.RESULT_SET;
    }

    public String getQualifier()
    {
        if (queryString == null) {
            queryString = sql.toString(dialect);
        }
        return "[" + queryString + "]";
    }

    public Object clone()
    {
        return new JdbcQuery(
            cluster,
            rowType,
            connection,
            dialect,
            (SqlSelect) sql.clone(),
            dataSource);
    }

    public PlanCost computeSelfCost(SaffronPlanner planner)
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
        return planner.makeCost(rows,cpu,io);
    }

    public void onRegister(SaffronPlanner planner)
    {
        super.onRegister(planner);
        Util.discard(getQualifier()); // compute query string now
    }

    public static void register(SaffronPlanner planner)
    {
        planner.addRule(new TableAccessToQueryRule());
        planner.addRule(new AddFilterToQueryRule());
        planner.addRule(new AddProjectToQueryRule());
    }

    public Object implement(RelImplementor implementor,int ordinal)
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
        // but I don't know how.
        switch (ordinal) {
        case -1: // called from parent
            // This is all a horrible hack. Need away to 'freeze' a DataSource
            // into a Java expression which can be 'thawed' into a DataSource
            // at run-time. We should use the OJConnectionRegistry somehow.
            assert dataSource instanceof JdbcDataSource; // hack
            // DriverManager.getConnection("jdbc...", "scott", "tiger");
            final String url = ((JdbcDataSource) dataSource)._url;
            final MethodCall connectionExpr = new MethodCall(
                    TypeName.forClass(java.sql.DriverManager.class),
                    "getConnection",
                    new ExpressionList(Literal.makeLiteral(url),
                            Literal.makeLiteral("SA"),
                            Literal.makeLiteral("")));
            return new MethodCall(
                    new MethodCall(
                            connectionExpr,
                            "createStatement",
                            null),
                    "executeQuery",
                    new ExpressionList(Literal.makeLiteral(queryString)));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    protected SaffronType deriveRowType()
    {
        return rowType;
    }
}


// End JdbcQuery.java
