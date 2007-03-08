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

import java.util.*;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;


/**
 * A <code>SqlNodeList</code> is a list of {@link SqlNode}s. It is also a {@link
 * SqlNode}, so may appear in a parse tree.
 */
public class SqlNodeList
extends SqlNode
implements Iterable<SqlNode>
{

    //~ Static fields/initializers ---------------------------------------------

    /**
     * An immutable, empty SqlNodeList.
     */
    public static final SqlNodeList Empty =
        new SqlNodeList(SqlParserPos.ZERO) {
        public void add(SqlNode node)
        {
            throw new UnsupportedOperationException();
        }
    };


    //~ Instance fields --------------------------------------------------------

    private final List<SqlNode> list;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an empty <code>SqlNodeList</code>.
     */
    public SqlNodeList(SqlParserPos pos)
    {
        super(pos);
        list = new ArrayList<SqlNode>();
    }

    /**
     * Creates a <code>SqlNodeList</code> containing the nodes in <code>
     * list</code>. The list is copied, but the nodes in it are not.
     */
    public SqlNodeList(
        Collection collection,
        SqlParserPos pos)
    {
        super(pos);
        list = new ArrayList<SqlNode>(collection);
    }

    //~ Methods ----------------------------------------------------------------

    // implement Iterable<SqlNode>
    public Iterator<SqlNode> iterator()
    {
        return list.iterator();
    }

    public List<SqlNode> getList()
    {
        return list;
    }

    public void add(SqlNode node)
    {
        list.add(node);
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlNodeList(
            list,
            pos);
    }

    public SqlNode get(int n)
    {
        return list.get(n);
    }

    public SqlNode set(int n, SqlNode node)
    {
        return list.set(n, node);
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
        final SqlWriter.Frame frame =
            ((leftPrec > 0) || (rightPrec > 0)) ? writer.startList("(", ")")
                : writer.startList("", "");
            commaList(writer);
            writer.endList(frame);
    }

    void commaList(SqlWriter writer)
    {
        for (int i = 0; i < list.size(); i++) {
            SqlNode node = list.get(i);
            writer.sep(",");
            node.unparse(writer, 0, 0);
        }
    }

    void andOrList(SqlWriter writer, SqlKind sepKind)
    {
        int lprec, rprec;
        for (int i = 0; i < list.size(); i++) {
            SqlNode node = list.get(i);
            writer.sep(sepKind.getName(), false);
            lprec = rprec = 0;
            if (node instanceof SqlCall && (
                ((SqlCall)node).getKind().isA(SqlKind.And)
                ||((SqlCall)node).getKind().isA(SqlKind.Or))) {
                lprec = ((SqlCall)node).getOperator().getLeftPrec();
                rprec = ((SqlCall)node).getOperator().getRightPrec();
            }
            node.unparse(writer,lprec, rprec);
        }
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        for (SqlNode child : list) {
            child.validate(validator, scope);
        }
    }

    public <R> R accept(SqlVisitor<R> visitor)
    {
        return visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node, boolean fail)
    {
        if (!(node instanceof SqlNodeList)) {
            assert !fail : this + "!=" + node;
        return false;
        }
        SqlNodeList that = (SqlNodeList) node;
        if (this.size() != that.size()) {
            assert !fail : this + "!=" + node;
        return false;
        }
        for (int i = 0; i < list.size(); i++) {
            SqlNode thisChild = list.get(i);
            final SqlNode thatChild = that.list.get(i);
            if (!thisChild.equalsDeep(thatChild, fail)) {
                return false;
            }
        }
        return true;
    }

    public SqlNode [] toArray()
    {
        return list.toArray(new SqlNode[list.size()]);
    }

    public static boolean isEmptyList(final SqlNode node)
    {
        if (node instanceof SqlNodeList) {
            if (0 == ((SqlNodeList) node).size()) {
                return true;
            }
        }
        return false;
    }

    public void validateExpr(SqlValidator validator, SqlValidatorScope scope)
    {
        // While a SqlNodeList is not always a valid expression, this
        // implementation makes that assumption. It just validates the members
        // of the list.
        //
        // One example where this is valid is the IN operator. The expression
        //
        //    empno IN (10, 20)
        //
        // results in a call with operands
        //
        //    {  SqlIdentifier({"empno"}),
        //       SqlNodeList(SqlLiteral(10), SqlLiteral(20))  }

        for (SqlNode node : list) {
            node.validateExpr(validator, scope);
        }
    }
}

//End SqlNodeList.java
