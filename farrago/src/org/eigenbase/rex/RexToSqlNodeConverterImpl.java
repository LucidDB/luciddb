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

import org.eigenbase.rel.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * Standard implementation of {@link RexToSqlNodeConverter}.
 *
 * @author
 * @version $Id$
 */
public class RexToSqlNodeConverterImpl
    implements RexToSqlNodeConverter
{

    //~ Instance fields --------------------------------------------------------

    private final RexSqlConvertletTable convertletTable;

    //~ Constructors -----------------------------------------------------------

    public RexToSqlNodeConverterImpl(RexSqlConvertletTable convertletTable)
    {
        this.convertletTable = convertletTable;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode convertCall(RelNode relNode, RexCall call)
    {
        final RexSqlConvertlet convertlet = convertletTable.get(call);
        if (convertlet != null) {
            return convertlet.convertCall(relNode, call);
        }
        // No convertlet was suitable.
        throw Util.needToImplement(call);
    }

    public SqlNode convertLiteral(
        RelNode relNode,
        RexLiteral literal)
    {
        throw literal.getTypeName().unexpected();
    }

}

// End RexToSqlNodeConverterImpl.java
