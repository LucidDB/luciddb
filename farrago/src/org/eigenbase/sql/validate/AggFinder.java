/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import org.eigenbase.sql.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.util.*;


/**
 * Visitor which looks for an aggregate function inside a tree of {@link
 * SqlNode} objects.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 28, 2004
 */
class AggFinder
    extends SqlBasicVisitor<Void>
{
    private final boolean over;
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AggFinder.
     *
     * @param over Whether to find windowed function calls {@code Agg(x) OVER
     * windowSpec}
     */
    AggFinder(boolean over)
    {
        this.over = over;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Finds an aggregate.
     *
     * @param node Parse tree to search
     * @return First aggregate function in parse tree, or null if not found
     */
    public SqlNode findAgg(SqlNode node)
    {
        try {
            node.accept(this);
            return null;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return (SqlNode) e.getNode();
        }
    }

    public Void visit(SqlCall call)
    {
        if (call.getOperator().isAggregator()) {
            throw new Util.FoundOne(call);
        }
        if (call.isA(SqlKind.Query)) {
            // don't traverse into queries
            return null;
        }
        if (call.isA(SqlKind.Over)) {
            if (over) {
                throw new Util.FoundOne(call);
            } else {
                // an aggregate function over a window is not an aggregate!
                return null;
            }
        }
        return super.visit(call);
    }
}

// End AggFinder.java
