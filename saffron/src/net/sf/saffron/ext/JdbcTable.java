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

package net.sf.saffron.ext;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.OJConnectionRegistry;
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.TableAccessRel;
import openjava.ptree.*;


/**
 * A <code>JdbcTable</code> implements {@link SaffronTable} by connecting to a
 * JDBC database.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 10 November, 2001
 */
public class JdbcTable extends AbstractTable implements ImplementableTable
{
    //~ Instance fields -------------------------------------------------------

    JdbcColumn [] columns;

    //~ Constructors ----------------------------------------------------------

    public JdbcTable(
        SaffronSchema schema,
        String name,
        SaffronType rowType,
        JdbcColumn [] columns)
    {
        super(schema,name,rowType);
        this.columns = columns;
    }

    public JdbcTable(SaffronSchema schema,String name,SaffronType rowType)
    {
        super(schema,name,rowType);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the column that the named field maps to.
     */
    public JdbcColumn getColumn(String fieldName)
    {
        if (columns == null) {
            return new JdbcColumn(fieldName,fieldName);
        } else {
            for (int i = 0; i < columns.length; i++) {
                JdbcColumn column = columns[i];
                if (column.fieldName.equals(fieldName)) {
                    return column;
                }
            }
            return null;
        }
    }

    // implement ImplementableTable
    public void implement(SaffronRel rel,RelImplementor implementor)
    {
        // Generate
        //
        // java.sql.Connection jdbcConnection = null;
        // java.sql.Statement stmt = null;
        // try {
        //    jdbcCon = ((javax.sql.DataSource) connection).getConnection();
        //    stmt = jdbcConnection.createStatement();
        //    java.sql.ResultSet resultSet = stmt.executeQuery(
        //       "select * from table");
        //    while (resultSet.next()) {
        //      Emp emp = new Emp(resultSet);
        //      <<child>>
        //    }
        // } catch (java.sql.SQLException e) {
        //    throw new saffron.runtime.SaffronError(e);
        // } finally {
        //    if (stmt != null) {
        //       try {
        //          stmt.close();
        //       } catch (java.sql.SQLException e) {}
        //    }
        //    if (jdbcCon != null) {
        //       try {
        //          jdbcCon.close();
        //       } catch (java.sql.SQLException e) {}
        //    }
        // }
        StatementList stmtList = implementor.getStatementList();
        Variable varJdbcCon = implementor.newVariable();
        Variable varStmt = implementor.newVariable();
        Variable varRs = implementor.newVariable();
        Variable varRow = implementor.newVariable();
        Variable varEx = implementor.newVariable();

        final TypeName rowTypeName = OJUtil.toTypeName(rowType);
        StatementList whileBody =
            new StatementList(
                
            // Emp emp = new Emp(resultSet);
            // <<body>>
            new VariableDeclaration(
                    rowTypeName,
                    varRow.toString(),
                    new AllocationExpression(
                        rowTypeName,
                        new ExpressionList(varRs))));
        implementor.bind(rel,varRow);
        implementor.generateParentBody(rel,whileBody);

        // java.sql.Statement jdbcCon = null;
        // java.sql.Statement stmt = null;
        stmtList.add(
                new VariableDeclaration(
                        TypeName.forClass(java.sql.Connection.class),
                        varJdbcCon.toString(),
                        Literal.constantNull()));
        stmtList.add(
                new VariableDeclaration(
                        TypeName.forClass(java.sql.Statement.class),
                        varStmt.toString(),
                        Literal.constantNull()));

        String queryString = "select * from " + getName();
        final SaffronConnection connection =
                ((TableAccessRel) rel).getConnection();
        final OJConnectionRegistry.ConnectionInfo connectionInfo =
                OJConnectionRegistry.instance.get(connection);
        stmtList.add(
                new TryStatement(

                        // try {
                        new StatementList(

                                // jdbcCon = ((javax.sql.DataSource) connection).getConnection();
                                new ExpressionStatement(
                                        new AssignmentExpression(
                                                varJdbcCon,
                                                AssignmentExpression.EQUALS,
                                                new MethodCall(
                                                        new CastExpression(
                                                                TypeName.forClass(javax.sql.DataSource.class),
                                                                connectionInfo.expr
                                                        ),
                                                        "getConnection",
                                                        null))),

                                // statement = jdbcCon.createStatement();
                                new ExpressionStatement(
                                        new AssignmentExpression(
                                                varStmt,
                                                AssignmentExpression.EQUALS,
                                                new MethodCall(
                                                        varJdbcCon,
                                                        "createStatement",
                                                        null))),

                                // java.sql.ResultSet resultSet =
                                //   statement.executeQuery(
                                //     "select * from table");
                                new VariableDeclaration(
                                        new TypeName("java.sql.ResultSet"),
                                        varRs.toString(),
                                        new MethodCall(
                                                varStmt,
                                                "executeQuery",
                                                new ExpressionList(
                                                        Literal.makeLiteral(queryString)))),

                                // while (resultSet.next()) {
                                new WhileStatement(new MethodCall(varRs,"next",null),
                                        // Emp emp = new Emp(resultSet);
                                        // <<body>>
                                        whileBody)),
                        new CatchList(

                                // catch (java.sql.SQLException e) {
                                //   throw new saffron.runtime.SaffronError(e);
                                // }
                                new CatchBlock(new Parameter(
                                        new TypeName("java.sql.SQLException"),
                                        varEx.toString()),
                                        new StatementList(
                                                new ThrowStatement(
                                                        new AllocationExpression(
                                                                TypeName.forClass(
                                                                        net.sf.saffron.runtime.SaffronError.class),
                                                                new ExpressionList(varEx)))))),

                        // finally {
                        //    if (stmt != null) {
                        //       try {
                        //          stmt.close();
                        //       } catch (java.sql.SQLException e) {}
                        //    }
                        //    if (jdbcCon != null) {
                        //       try {
                        //          jdbcCon.close();
                        //       } catch (java.sql.SQLException e) {}
                        //    }
                        // }
                        new StatementList(
                                new IfStatement(
                                        new BinaryExpression(
                                                varStmt,
                                                BinaryExpression.NOTEQUAL,
                                                Literal.constantNull()),
                                        new StatementList(
                                                new TryStatement(
                                                        new StatementList(
                                                                new ExpressionStatement(
                                                                        new MethodCall(varStmt,"close",null))),
                                                        new CatchList(
                                                                new CatchBlock(
                                                                        new Parameter(
                                                                                new TypeName(
                                                                                        "java.sql.SQLException"),
                                                                                varEx.toString()),
                                                                        new StatementList()))))),
                                new IfStatement(
                                        new BinaryExpression(
                                                varJdbcCon,
                                                BinaryExpression.NOTEQUAL,
                                                Literal.constantNull()),
                                        new StatementList(
                                                new TryStatement(
                                                        new StatementList(
                                                                new ExpressionStatement(
                                                                        new MethodCall(varJdbcCon,"close",null))),
                                                        new CatchList(
                                                                new CatchBlock(
                                                                        new Parameter(
                                                                                new TypeName(
                                                                                        "java.sql.SQLException"),
                                                                                varEx.toString()),
                                                                        new StatementList()))))))));
    }

    public SaffronRel toRel(VolcanoCluster cluster,SaffronConnection connection)
    {
        return new TableAccessRel(cluster,this,connection);
    }

    //~ Inner Classes ---------------------------------------------------------

    // classes

    /**
     * <code>JdbcColumn</code> records the mapping of fields onto columns, for
     * the purposes of SQL generation.
     */
    public static class JdbcColumn
    {
        String columnName;
        String fieldName;

        JdbcColumn(String fieldName,String columnName)
        {
            this.fieldName = fieldName;
            this.columnName = columnName;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public String getFieldName()
        {
            return fieldName;
        }
    }
}


// End JdbcTable.java
