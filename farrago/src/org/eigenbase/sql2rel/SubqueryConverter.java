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

import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * SubqueryConverter provides the interface for classes that convert subqueries
 * into equivalent expressions.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public interface SubqueryConverter
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @return true if the subquery can be converted
     */
    public boolean canConvertSubquery();

    /**
     * Converts the subquery to an equivalent expression.
     *
     * @param subquery the SqlNode tree corresponding to a subquery
     * @param parentConverter sqlToRelConverter of the parent query
     * @param isExists whether the subquery is part of an EXISTS expression
     * @param isExplain whether the subquery is part of an EXPLAIN PLAN
     * statement
     *
     * @return the equivalent expression or null if the subquery couldn't be
     * converted
     */
    public RexNode convertSubquery(
        SqlCall subquery,
        SqlToRelConverter parentConverter,
        boolean isExists,
        boolean isExplain);
}

// End SubqueryConverter.java
