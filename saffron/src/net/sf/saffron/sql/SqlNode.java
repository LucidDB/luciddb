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

package net.sf.saffron.sql;

import net.sf.saffron.util.BarfingInvocationHandler;
import net.sf.saffron.sql.type.SqlTypeName;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;


/**
 * A <code>SqlNode</code> is a SQL parse tree. It may be an {@link SqlOperator
 * operator}, {@link SqlLiteral literal}, {@link SqlIdentifier identifier},
 * and so forth.
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 12, 2003
 */
public abstract class SqlNode
{
    private SqlTypeName type = null;

    //~ Constructors ----------------------------------------------------------

    SqlNode()
    {
    }

    //~ Methods ---------------------------------------------------------------

    public abstract Object clone();

    /**
     * Returns whether this node is a particular kind.
     *
     * @param kind a {@link net.sf.saffron.sql.SqlKind} value
     */
    public boolean isA(SqlKind kind)
    {
        // REVIEW jvs 6-Feb-2004:  either this should be
        // getKind().isA(kind), or this method should be renamed to
        // avoid confusion
        return getKind() == kind;
    }

    /**
     * Returns the type of node this is, or {@link net.sf.saffron.sql.SqlKind#Other} if it's
     * nothing special.
     *
     * @return a {@link SqlKind} value, never null
     */
    public SqlKind getKind()
    {
        return SqlKind.Other;
    }

    public static SqlNode [] cloneArray(SqlNode [] nodes)
    {
        SqlNode [] clones = (SqlNode []) nodes.clone();
        for (int i = 0; i < clones.length; i++) {
            SqlNode node = clones[i];
            if (node != null) {
                clones[i] = (SqlNode) node.clone();
            }
        }
        return clones;
    }

    public void setType(SqlTypeName type)
    {
        this.type = type;
    }

    public SqlTypeName getType()
    {
        return type;
    }

    public String toString()
    {
        return toString(null);
    }

    /**
     * Returns the text of the tree of which this <code>SqlNode</code> is the
     * root.
     */
    public String toString(SqlDialect dialect)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        if (dialect == null) {
            dialect = new SqlDialect(dummyDatabaseMetaData());
        }
        SqlWriter writer = new SqlWriter(dialect,pw);
        unparse(writer,0,0);
        pw.flush();
        return sw.toString();
    }

    /**
     * Writes a SQL representation of this node to a writer.
     *
     * <p>The <code>leftPrec</code> and <code>rightPrec</code> parameters
     * give us enough context to decide whether we need to enclose the
     * expression in parentheses. For example, we need parentheses around
     * "2 + 3" if preceded by "5 *". This is because the precedence of the "*"
     * operator is greater than the precedence of the "+" operator.
     *
     * <p>The algorithm handles left- and right-associative operators by giving
     * them slightly different left- and right-precedence.
     *
     * <p>If {@link SqlWriter#alwaysUseParentheses} is true, we use parentheses
     * even when they are not required by the precedence rules.
     *
     * <p>For the details of this algorithm, see {@link SqlCall#unparse}.
     *
     * @param writer Target writer
     * @param leftPrec The precedence of the {@link SqlNode} immediately
     *   preceding this node in a depth-first scan of the parse tree
     * @param rightPrec The precedence of the {@link SqlNode} immediately
     *   following this node in a depth-first scan of the parse tree
     */
    public abstract void unparse(SqlWriter writer,int leftPrec,int rightPrec);

    private static DatabaseMetaData dummyDatabaseMetaData()
    {
        return (DatabaseMetaData) Proxy.newProxyInstance(
            null,
            new Class [] { DatabaseMetaData.class },
            new DatabaseMetaDataInvocationHandler());
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Handles particular {@link DatabaseMetaData} methods; invocations of
     * other methods will fall through to the base class, {@link
     * BarfingInvocationHandler}, which will throw an error.
     */
    public static class DatabaseMetaDataInvocationHandler
        extends BarfingInvocationHandler
    {
        public String getDatabaseProductName() throws SQLException
        {
            return "fooBar";
        }

        public String getIdentifierQuoteString() throws SQLException
        {
            return "`";
        }
    }


}


// End SqlNode.java
