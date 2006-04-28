/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql.util;

import org.eigenbase.sql.*;

/**
 * Basic implementation of {@link SqlVisitor} which does nothing at each
 * node.
 *
 * <p>This class is useful as a base class for classes which implement the
 * {@link SqlVisitor} interface. The derived class can override whichever
 * methods it chooses.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlBasicVisitor<R> implements SqlVisitor<R>
 {
     /**
      * Used to keep track of the current SqlNode parent of a visiting node.
      * A value of null mean no parent.
      * NOTE: In case of extending SqlBasicVisitor, remember that
      * parent value might not be set depending on if and how
      * visit(SqlCall) and visit(SqlNodeList) is implemented.
      */
     protected SqlNode currentParent = null;
     
     /**
      *  Only valid if currentParrent is a SqlCall or SqlNodeList
      *  Describes the offset within the parent
      */
     protected Integer currentOffset = null;

     public SqlNode getCurrentParent()
     {
         return currentParent;
     }

     public Integer getCurrentOffset()
     {
         return currentOffset;
     }
     
     public R visit(SqlLiteral literal)
     {
         return null;
     }

     public R visit(SqlCall call)
     {
         return call.getOperator().acceptCall(this, call);
     }

     public R visit(SqlNodeList nodeList)
     {
         R result = null;
         for (int i = 0; i < nodeList.size(); i++) {
             currentParent = nodeList;
             currentOffset = new Integer(i);
             SqlNode node = nodeList.get(i);
             result = node.accept(this);
         }
         currentParent = null;
         return result;
     }

     public R visit(SqlIdentifier id)
     {
         return null;
     }

     public R visit(SqlDataTypeSpec type)
     {
         return null;
     }

     public R visit(SqlDynamicParam param)
     {
         return null;
     }

     public R visit(SqlIntervalQualifier intervalQualifier)
     {
         return null;
     }

     public R visitChild(SqlNode parent, int ordinal, SqlNode child)
     {
         currentParent = parent;
         currentOffset = new Integer(ordinal);
         R result = child.accept(this);
         currentParent = null;
         return result;
     }
}

// End SqlBasicVisitor.java
