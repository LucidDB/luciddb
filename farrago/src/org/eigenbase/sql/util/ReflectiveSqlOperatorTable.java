/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.MultiMap;
import org.eigenbase.util.Util;

import java.lang.reflect.Field;
import java.util.*;

/**
 * ReflectiveSqlOperatorTable implements the {@link SqlOperatorTable }
 * interface by reflecting the public fields of a subclass.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class ReflectiveSqlOperatorTable implements SqlOperatorTable
{
    //~ Instance fields -------------------------------------------------------

    private final MultiMap operators = new MultiMap();
    private final HashMap mapNameToOp = new HashMap();

    //~ Constructors ----------------------------------------------------------

    protected ReflectiveSqlOperatorTable()
    {
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Performs post-constructor initialization of an operator table. It can't
     * be part of the constructor, because the subclass constructor needs to
     * complete first.
     */
    public final void init()
    {
        // Use reflection to register the expressions stored in public fields.
        Field [] fields = getClass().getFields();
        for (int i = 0; i < fields.length; i++) {
            try {
                Field field = fields[i];
                if (SqlFunction.class.isAssignableFrom(field.getType())) {
                    SqlFunction op = (SqlFunction) field.get(this);
                    if (op != null) {
                        register(op);
                    }
                } else if (SqlOperator.class.isAssignableFrom(field.getType())) {
                    SqlOperator op = (SqlOperator) field.get(this);
                    register(op);

                }
            } catch (IllegalArgumentException e) {
                throw Util.newInternal(e,
                    "Error while initializing operator table");
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e,
                    "Error while initializing operator table");
            }
        }
    }

    // implement SqlOperatorTable
    public List lookupOperatorOverloads(
        SqlIdentifier opName,
        SqlFunctionCategory category,
        SqlSyntax syntax)
    {
        // NOTE jvs 3-Mar-2005:  ignore category until someone cares
        
        List overloads = new ArrayList();
        String simpleName;
        if (opName.names.length > 1) {
            if (opName.names[opName.names.length - 2].equals(
                    "INFORMATION_SCHEMA"))
            {
                // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
                simpleName = opName.names[opName.names.length - 1];
            } else {
                return overloads;
            }
        } else {
            simpleName = opName.getSimple();
        }
        final List list = operators.getMulti(simpleName);
        for (int i = 0, n = list.size(); i < n; i++) {
            SqlOperator op = (SqlOperator) list.get(i);
            if (op.getSyntax() == syntax) {
                overloads.add(op);
            } else if ((syntax == SqlSyntax.Function)
                && (op instanceof SqlFunction))
            {
                // this special case is needed for operators like CAST,
                // which are treated as functions but have special syntax
                overloads.add(op);
            }
        }

        // REVIEW jvs 1-Jan-2005:  why is this extra lookup required?
        // Shouldn't it be covered by search above?
        Object extra = null;
        switch (syntax.ordinal) {
        case SqlSyntax.Binary_ordinal:
            extra = mapNameToOp.get(simpleName + ":BINARY");
        case SqlSyntax.Prefix_ordinal:
            extra = mapNameToOp.get(simpleName + ":PREFIX");
        case SqlSyntax.Postfix_ordinal:
            extra = mapNameToOp.get(simpleName + ":POSTFIX");
        default:
            break;
        }

        if ((extra != null) && !overloads.contains(extra)) {
            overloads.add(extra);
        }

        return overloads;
    }

    public void register(SqlOperator op)
    {
        operators.putMulti(op.name, op);
        if (op instanceof SqlBinaryOperator) {
            mapNameToOp.put(op.name + ":BINARY", op);
        } else if (op instanceof SqlPrefixOperator) {
            mapNameToOp.put(op.name + ":PREFIX", op);
        } else if (op instanceof SqlPostfixOperator) {
            mapNameToOp.put(op.name + ":POSTFIX", op);
        }
    }

    /**
     * Registers a function in the table.
     * @param function
     */
    public void register(SqlFunction function)
    {
        operators.putMulti(function.name, function);
        SqlFunctionCategory funcType = function.getFunctionType();
        assert (funcType != null) : "Function type for " + function.name
        + " not set";
    }

    // implement SqlOperatorTable
    public List getOperatorList()
    {
        ArrayList list = new ArrayList();

        Iterator it = operators.entryIterMulti();
        while (it.hasNext()) {
            Map.Entry mapEntry = (Map.Entry) it.next();
            list.add(mapEntry.getValue());
        }

        return list;
    }
}

// End ReflectiveSqlOperatorTable.java
