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
package org.eigenbase.sql;

import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.parser.ParserPosition;

/**
 * Abstract base for chararacter and binary string literals.
 *
 * @author wael
 * @version $Id$
 */
abstract class SqlAbstractStringLiteral extends SqlLiteral
{
    protected SqlAbstractStringLiteral(
        Object value,
        SqlTypeName typeName,
        ParserPosition pos)
    {
        super(value, typeName, pos);
    }

    /**
     * Helper routine for {@link SqlUtil#concatenateLiterals}.
     * @param lits homogeneous StringLiteral[] args.
     * @return StringLiteral with concatenated value.
     * this == lits[0], used only for method dispatch.
     */
    protected abstract SqlAbstractStringLiteral concat1(
        SqlLiteral [] lits);
}

// End SqlAbstractStringLiteral.java