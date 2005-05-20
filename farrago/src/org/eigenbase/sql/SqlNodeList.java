/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.sql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Iterator;

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;


/**
 * A <code>SqlNodeList</code> is a list of {@link SqlNode}s. It is also a
 * {@link SqlNode}, so may appear in a parse tree.
 */
public class SqlNodeList extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    private ArrayList list;

    /**
     * An immutable, empty SqlNodeList.
     */
    public static final SqlNodeList Empty =
        new SqlNodeList(SqlParserPos.ZERO) {
            public void add(SqlNode node) {
                throw new UnsupportedOperationException();
            }
        };

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an empty <code>SqlNodeList</code>.
     */
    public SqlNodeList(SqlParserPos pos)
    {
        super(pos);
        list = new ArrayList();
    }

    /**
     * Creates a <code>SqlNodeList</code> containing the nodes in
     * <code>list</code>. The list is copied, but the nodes in it are not.
     */
    public SqlNodeList(
        Collection collection,
        SqlParserPos pos)
    {
        super(pos);
        list = new ArrayList(collection);
    }

    //~ Methods ---------------------------------------------------------------

    public List getList()
    {
        return list;
    }

    public void add(SqlNode node)
    {
        list.add(node);
    }

    public Object clone()
    {
        return new SqlNodeList(
            list,
            getParserPosition());
    }

    public SqlNode get(int n)
    {
        return (SqlNode) list.get(n);
    }

    public int size()
    {
        return list.size();
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        if ((leftPrec > 0) || (rightPrec > 0)) {
            writer.print('(');
            unparse(writer, 0, 0);
            writer.print(')');
        } else {
            for (int i = 0; i < list.size(); i++) {
                SqlNode node = (SqlNode) list.get(i);
                if (i > 0) {
                    writer.print(", ");
                }
                node.unparse(writer, 0, 0);
            }
        }
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        Iterator iter = getList().iterator();
        while (iter.hasNext()) {
            final SqlNode child = (SqlNode) iter.next();
            child.validate(validator, scope);
        }
    }

    public void accept(SqlVisitor visitor)
    {
        visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node)
    {
        if (!(node instanceof SqlNodeList)) {
            return false;
        }
        SqlNodeList that = (SqlNodeList) node;
        if (this.size() != that.size()) {
            return false;
        }
        for (int i = 0; i < list.size(); i++) {
            SqlNode thisChild = (SqlNode) list.get(i);
            final SqlNode thatChild = that.get(i);
            if (!thisChild.equalsDeep(thatChild)) {
                return false;
            }
        }
        return true;
    }

    public SqlNode [] toArray()
    {
        return (SqlNode []) list.toArray(new SqlNode[list.size()]);
    }
}


// End SqlNodeList.java
