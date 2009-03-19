/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
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
package org.eigenbase.sql.validate;

import org.eigenbase.sql.*;


/**
 * The name-resolution context for expression inside a multiset call. The
 * objects visible are multiset expressions, and those inherited from the parent
 * scope.
 *
 * @author wael
 * @version $Id$
 * @see CollectNamespace
 * @since Mar 25, 2003
 */
class CollectScope
    extends ListScope
{
    //~ Instance fields --------------------------------------------------------

    private final SqlValidatorScope usingScope;
    private final SqlCall child;

    //~ Constructors -----------------------------------------------------------

    CollectScope(
        SqlValidatorScope parent,
        SqlValidatorScope usingScope,
        SqlCall child)
    {
        super(parent);
        this.usingScope = usingScope;
        this.child = child;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getNode()
    {
        return child;
    }
}

// End CollectScope.java
