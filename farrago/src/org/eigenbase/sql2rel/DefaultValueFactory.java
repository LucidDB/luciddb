/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
     * Returns whether a column is always generated. If a column is always
     * generated, then non-generated values cannot be inserted into the column.
     *
     * <p>The column ordinal is 0-based, and references system columns before
     * regular columns. If a table has 2 system columns and 8 regular columns,
     * {@code iColumn} can be in the range 0..9. 0 denotes system column #0,
     * 1 denotes system column #0, 2 denotes real column #0, and so forth.
     *
     * <p>The default implementation assumes that system coumns are always
     * generated.
     *
     * @param table Table containing the column
     * @param iColumn Ordinal of the column in the table
     *
     * @return whether a column is always generated
     */
    public boolean isGeneratedAlways(
        RelOptTable table,
        int iColumn);

    /**
     * Creates an expression which evaluates to the default value for a
     * particular column.
     *
     * <p>The column ordinal is 0-based, and references system columns before
     * regular columns. If a table has 2 system columns and 8 regular columns,
     * {@code iColumn} can be in the range 0..9. 0 denotes system column #0,
     * 1 denotes system column #0, 2 denotes real column #0, and so forth.
     *
     * @param table Table containing the column
     * @param iColumn Ordinal of the column in the table
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
