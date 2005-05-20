/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
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
package org.eigenbase.sql.validate;

import org.eigenbase.reltype.RelDataType;

/**
 * Supplies a {@link SqlValidator} with the
 * metadata for a table.
 *
 * @see SqlValidatorCatalogReader
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public interface SqlValidatorTable
{
    RelDataType getRowType();

    String [] getQualifiedName();

    /**
     * Returns whether a given column is monotonic.
     */
    boolean isMonotonic(String columnName);
}

// End SqlValidatorTable.java
