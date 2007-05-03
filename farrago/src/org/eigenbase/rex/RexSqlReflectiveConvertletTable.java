/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package org.eigenbase.rex;

import java.util.*;
import org.eigenbase.sql.*;


/**
 * Implementation of {@link RexSqlConvertletTable}
 *
 * @author
 * @version $Id$
 */
public class RexSqlReflectiveConvertletTable
    implements RexSqlConvertletTable
{

    //~ Instance fields --------------------------------------------------------

    private final Map<Object,Object> map = new HashMap<Object, Object>();

    //~ Constructors -----------------------------------------------------------

    public RexSqlReflectiveConvertletTable()
    {
    }

    //~ Methods ----------------------------------------------------------------

    public RexSqlConvertlet get(RexCall call)
    {
        RexSqlConvertlet convertlet;
        final SqlOperator op = call.getOperator();

        // Is there a convertlet for this operator
        // (e.g. SqlStdOperatorTable.plusOperator)?
        convertlet = (RexSqlConvertlet) map.get(op);
        if (convertlet != null) {
            return convertlet;
        }

        // Is there a convertlet for this class of operator
        // (e.g. SqlBinaryOperator)?
        Class<? extends Object> clazz = op.getClass();
        while (clazz != null) {
            convertlet = (RexSqlConvertlet) map.get(clazz);
            if (convertlet != null) {
                return convertlet;
            }
            clazz = clazz.getSuperclass();
        }

        // Is there a convertlet for this class of expression
        // (e.g. SqlCall)?
        clazz = call.getClass();
        while (clazz != null) {
            convertlet = (RexSqlConvertlet) map.get(clazz);
            if (convertlet != null) {
                return convertlet;
            }
            clazz = clazz.getSuperclass();
        }
        return null;
    }

    /**
     * Registers a convertlet for a given operator instance
     *
     * @param op Operator instance, say {@link
     * SqlStdOperatorTable#minusOperator}
     * @param convertlet Convertlet
     */
    protected void registerOp(SqlOperator op, RexSqlConvertlet convertlet)
    {
        map.put(op, convertlet);
    }
}

// End RexSqlReflectiveConvertletTable.java
