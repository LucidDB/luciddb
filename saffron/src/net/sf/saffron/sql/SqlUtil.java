/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

import java.util.ArrayList;


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
        } else if (node1.isA(SqlKind.And)) {
            ((SqlCall) node1).addOperand(node2);
            return node1;
        } else {
            return SqlOperatorTable.instance().andOperator.createCall(
                new SqlNode [] { node1,node2 });
        }
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

}


// End SqlUtil.java
