/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.sql2rel;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * DefaultValueFactory supplies default values for INSERT, UPDATE, and NEW.
 *
 * <p>TODO jvs 26-Feb-2005: rename this to InitializerExpressionFactory, since
 * it is in the process of being generalized to handle constructor invocations
 * and eventually generated columns.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface DefaultValueFactory
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Whether a column is always generated. If a column is always generated,
     * then non-generated values cannot be inserted into the column.
     */
    public boolean isGeneratedAlways(
        RelOptTable table,
        int iColumn);

    /**
     * Creates an expression which evaluates to the default value for a
     * particular column.
     *
     * @param table the table containing the column
     * @param iColumn the 0-based offset of the column in the table
     *
     * @return default value expression
     */
    public RexNode newColumnDefaultValue(
        RelOptTable table,
        int iColumn);

    /**
     * Creates an expression which evaluates to the initializer expression for a
     * particular attribute of a structured type.
     *
     * @param type the structured type
     * @param constructor the constructor invoked to initialize the type
     * @param iAttribute the 0-based offset of the attribute in the type
     * @param constructorArgs arguments passed to the constructor invocation
     *
     * @return default value expression
     */
    public RexNode newAttributeInitializer(
        RelDataType type,
        SqlFunction constructor,
        int iAttribute,
        RexNode [] constructorArgs);
}

// End DefaultValueFactory.java
