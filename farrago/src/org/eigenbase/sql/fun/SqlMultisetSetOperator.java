/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package org.eigenbase.sql.fun;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import java.util.*;
/**
 * An operator which performs set operations on multisets, such as
 * "MULTISET UNION ALL". Not to be confused with {@link SqlMultisetOperator}.
 *
 * <p>todo: Represent the ALL keyword to MULTISET UNION ALL etc. as a
 * hidden operand. Then we can obsolete this class.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlMultisetSetOperator extends SqlBinaryOperator
{
    private final boolean all;

    public SqlMultisetSetOperator(String name, int prec, boolean all)
    {
        super(name, SqlKind.Other, prec, true,
            ReturnTypeInferenceImpl.useNullableMultiset,
            UnknownParamInference.useFirstKnown,
            OperandsTypeChecking.typeNullableMultisetMultiset);
        this.all = all;
    }
}

// End SqlMultisetSetOperator.java
