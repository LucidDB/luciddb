/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
package org.eigenbase.sql.fun;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * An operator which performs set operations on multisets, such as "MULTISET
 * UNION ALL".
 *
 * <p>Not to be confused with {@link SqlMultisetValueConstructor} or {@link
 * SqlMultisetQueryConstructor}.
 *
 * <p>todo: Represent the ALL keyword to MULTISET UNION ALL etc. as a hidden
 * operand. Then we can obsolete this class.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlMultisetSetOperator
    extends SqlBinaryOperator
{
    //~ Instance fields --------------------------------------------------------

    private final boolean all;

    //~ Constructors -----------------------------------------------------------

    public SqlMultisetSetOperator(String name, int prec, boolean all)
    {
        super(
            name,
            SqlKind.Other,
            prec,
            true,
            SqlTypeStrategies.rtiNullableMultiset,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcMultisetX2);
        this.all = all;
    }
}

// End SqlMultisetSetOperator.java
