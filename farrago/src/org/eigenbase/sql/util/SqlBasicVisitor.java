/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
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
package org.eigenbase.sql.util;

import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.*;

/**
 * Basic implementation of {@link SqlVisitor} which does nothing at each
 * node.
 *
 * This class is useful as a base class for classes which implement the
 * {@link SqlVisitor} interface. The derived class can override whichever
 * methods it chooses.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlBasicVisitor implements SqlVisitor
 {
    public void visit(SqlLiteral literal)
    {
    }

    public void visit(SqlCall call)
    {
        for (int i = 0; i < call.operands.length; i++) {
            SqlNode operand = call.operands[i];
            if (operand != null) {
                operand.accept(this);
            }
        }
    }

    public void visit(SqlNodeList nodeList)
    {
        for (int i = 0; i < nodeList.size(); i++) {
            SqlNode node = nodeList.get(i);
            node.accept(this);
        }
    }

    public void visit(SqlIdentifier id)
    {
    }

    public void visit(SqlDataType type)
    {
    }

    public void visit(SqlDynamicParam param)
    {
    }

    public void visit(SqlIntervalQualifier intervalQualifier)
    {
    }
}

// End SqlBasicVisitor.java
