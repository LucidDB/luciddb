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

import net.sf.saffron.sql.parser.ParserPosition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * A <code>SqlNodeList</code> is a list of {@link SqlNode}s. It is also a
 * {@link SqlNode}, so may appear in a parse tree.
 */
public class SqlNodeList extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    private ArrayList list;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an empty <code>SqlNodeList</code>.
     */
    public SqlNodeList(ParserPosition parserPosition)
    {
        super(parserPosition);
        list = new ArrayList();
    }

    /**
     * Creates a <code>SqlNodeList</code> containing the nodes in
     * <code>list</code>. The list is copied, but the nodes in it are not.
     */
    public SqlNodeList(Collection collection, ParserPosition parserPosition)
    {
        super(parserPosition);
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
        return new SqlNodeList(list, getParserPosition());
    }

    public SqlNode get(int n)
    {
        return (SqlNode) list.get(n);
    }

    public int size()
    {
        return list.size();
    }

    public void unparse(SqlWriter writer,int leftPrec,int rightPrec)
    {
        if ((leftPrec > 0) || (rightPrec > 0)) {
            writer.print('(');
            unparse(writer,0,0);
            writer.print(')');
        } else {
            for (int i = 0; i < list.size(); i++) {
                SqlNode node = (SqlNode) list.get(i);
                if (i > 0) {
                    writer.print(", ");
                }
                node.unparse(writer,0,0);
            }
        }
    }

    public SqlNode[] toArray() {
        return (SqlNode[]) list.toArray(new SqlNode[list.size()]);
    }
}


// End SqlNodeList.java
