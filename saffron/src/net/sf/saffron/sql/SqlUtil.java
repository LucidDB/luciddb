/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2004 Disruptive Tech
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

import net.sf.saffron.util.Util;
import net.sf.saffron.util.BarfingInvocationHandler;
import net.sf.saffron.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.lang.reflect.Proxy;


/**
 * Contains utility functions related to SQL parsing, all static.
 *
 * @author jhyde
 * @since Nov 26, 2003
 * @version $Id$
 */
public abstract class SqlUtil
{

    //~ Static fields/initializers --------------------------------------------

    /**
     * A {@link SqlDialect} useful for generating generic SQL. If you need to
     * do something database-specific like quoting identifiers, don't rely on
     * this dialect to do what you want.
     */
    public static final SqlDialect dummyDialect =
            new SqlDialect(dummyDatabaseMetaData());

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the input with a given alias.
     */
    public static SqlNode getFromNode(SqlSelect query,String alias)
    {
        ArrayList list = flatten(query.getFrom());
        for (int i = 0; i < list.size(); i++) {
            SqlNode node = (SqlNode) list.get(i);
            if (getAlias(node).equals(alias)) {
                return node;
            }
        }
        return null;
    }

    static SqlNode andExpressions(SqlNode node1,SqlNode node2)
    {
        if (node1 == null) {
            return node2;
        }
        ArrayList list = new ArrayList();
        if (node1.isA(SqlKind.And)) {
            list.addAll(Arrays.asList(((SqlCall) node1).operands));
        } else {
            list.add(node1);
        }
        if (node2.isA(SqlKind.And)) {
            list.addAll(Arrays.asList(((SqlCall) node2).operands));
        } else {
            list.add(node2);
        }
        return SqlOperatorTable.std().andOperator.createCall(
                (SqlNode []) list.toArray(new SqlNode[list.size()]),null);
    }

    static ArrayList flatten(SqlNode node)
    {
        ArrayList list = new ArrayList();
        flatten(node,list);
        return list;
    }

    public static String getAlias(SqlNode node)
    {
        if (node instanceof SqlIdentifier) {
            String [] names = ((SqlIdentifier) node).names;
            return names[names.length - 1];
        } else {
            throw Util.newInternal("cannot derive alias for '" + node + "'");
        }
    }

    /**
     * Returns the <code>n</code>th (0-based) input to a join expression.
     */
    public static SqlNode getFromNode(SqlSelect query,int ordinal)
    {
        ArrayList list = flatten(query.getFrom());
        return (SqlNode) list.get(ordinal);
    }

    private static void flatten(SqlNode node,ArrayList list)
    {
        switch (node.getKind().getOrdinal()) {
        case SqlKind.JoinORDINAL:
            SqlJoin join = (SqlJoin) node;
            flatten(join.getLeft(),list);
            flatten(join.getRight(),list);
            return;
        case SqlKind.AsORDINAL:
            SqlCall call = (SqlCall) node;
            flatten(call.operands[0],list);
            return;
        default:
            list.add(node);
            return;
        }
    }

    /**
     * Returns whether a node represents the NULL value.
     *
     * <p>Examples:<ul>
     * <li>For {@link SqlLiteral} Unknown, returns false.
     * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if
     *     <code>allowCast</code> is true, false otherwise.
     * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>,
     *     returns false.
     * </ul>
     */
    public static boolean isNullLiteral(SqlNode node, boolean allowCast)
    {
        if (node instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) node;
            if (literal._typeName == SqlTypeName.Null) {
                assert(null==literal.getValue());
                return true;
            } else {
                // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as NULL.
                return false;
            }
        }
        if (allowCast) {
            if (node.isA(SqlKind.Cast)) {
                SqlCall call = (SqlCall) node;
                if (isNullLiteral(call.operands[0], false)) {
                    // node is "CAST(NULL as type)"
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a node is a literal.
     *
     * <p>Many constructs which require literals also accept
     * <code>CAST(NULL AS <i>type</i>)</code>. This method does not accept
     * casts, so you should call {@link #isNullLiteral} first.
     *
     * @param node The node, never null.
     * @return Whether the node is a literal
     * @pre node != null
     */
    public static boolean isLiteral(SqlNode node) {
        Util.pre(node != null, "node != null");
        return node instanceof SqlLiteral;
    }

    /**
     * Creates a {@link DatabaseMetaData} object good enough to create a
     * {@link SqlDialect} object with, but not good for much else.
     */
    private static DatabaseMetaData dummyDatabaseMetaData()
    {
        return (DatabaseMetaData) Proxy.newProxyInstance(
            null,
            new Class [] { DatabaseMetaData.class },
            new DatabaseMetaDataInvocationHandler());
    }

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


// End SqlUtil.java
