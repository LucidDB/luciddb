/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2009 The Eigenbase Project
// Copyright (C) 2007-2009 SQLstream, Inc.
// Copyright (C) 2007-2009 LucidEra, Inc.
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

import org.eigenbase.sql.*;


/**
 * Converts expressions from {@link RexNode} to {@link SqlNode}.
 */
public interface RexToSqlNodeConverter
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Converts a {@link RexCall} to a {@link SqlNode} expression.
     *
     * @param call RexCall to translate
     *
     * @return SqlNode
     */
    SqlNode convertCall(RexCall call);

    /**
     * Converts a {@link RexLiteral} to a {@link SqlLiteral}.
     *
     * @param literal RexLiteral to translate
     *
     * @return SqlNode
     */
    SqlNode convertLiteral(RexLiteral literal);

    /**
     * Converts a {@link RexInputRef} to a {@link SqlIdentifier}.
     *
     * @param ref RexInputRef to translate
     *
     * @return SqlNode
     */
    SqlNode convertInputRef(RexInputRef ref);
}

// End RexToSqlNodeConverter.java
