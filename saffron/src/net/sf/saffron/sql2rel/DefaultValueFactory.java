/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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

package net.sf.saffron.sql2rel;

import net.sf.saffron.core.SaffronTable;
import net.sf.saffron.rex.RexNode;

/**
 * DefaultValueFactory supplies default values for INSERT and UPDATE.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface DefaultValueFactory
{
    /**
     * Create an expression which evaluates to the default value for a
     * particular column.
     *
     * @param table the table containing the column
     *
     * @param iColumn the 0-based offset of the column in the table
     *
     * @return default value expression
     */
    public RexNode newDefaultValue(
        SaffronTable table,
        int iColumn);
}

// End DefaultValueFactory.java
