/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.sql2rel;

import org.eigenbase.reltype.*;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.rex.RexNode;


/**
 * DefaultValueFactory supplies default values for INSERT, UPDATE, and NEW.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface DefaultValueFactory
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Creates an expression which evaluates to the default value for a
     * particular column.
     *
     * @param table the table containing the column
     *
     * @param iColumn the 0-based offset of the column in the table
     *
     * @return default value expression
     */
    public RexNode newColumnDefaultValue(
        RelOptTable table,
        int iColumn);

    /**
     * Creates an expression which evaluates to the default value for a
     * particular attribute of a structured type.
     *
     * @param type the structured type
     *
     * @param the 0-based offset of the attribute in the type
     *
     * @retunr default value expression
     */
    public RexNode newAttributeDefaultValue(
        RelDataType type,
        int iAttribute);
}


// End DefaultValueFactory.java
