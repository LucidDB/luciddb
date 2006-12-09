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

import org.eigenbase.sql.*;


/**
 * The name-resolution context for expression inside a JOIN clause. The objects
 * visible are the joined table expressions, and those inherited from the parent
 * scope.
 *
 * <p>Consider "SELECT * FROM (A JOIN B ON {exp1}) JOIN C ON {exp2}". {exp1} is
 * resolved in the join scope for "A JOIN B", which contains A and B but not
 * C.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class JoinScope
    extends ListScope
{

    //~ Instance fields --------------------------------------------------------

    private final SqlValidatorScope usingScope;
    private final SqlJoin join;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>JoinScope</code>.
     * @param parent Parent scope
     * @param usingScope Scope for resolving USING clause
     * @param join Call to JOIN operator
     */
    JoinScope(
        SqlValidatorScope parent,
        SqlValidatorScope usingScope,
        SqlJoin join)
    {
        super(parent);
        this.usingScope = usingScope;
        this.join = join;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getNode()
    {
        return join;
    }

    public void addChild(SqlValidatorNamespace ns, String alias)
    {
        super.addChild(ns, alias);
        if ((usingScope != null) && (usingScope != parent)) {
            // We're looking at a join within a join. Recursively add this
            // child to its parent scope too. Example:
            //
            //   select *
            //   from (a join b on expr1)
            //   join c on expr2
            //   where expr3
            //
            // 'a' is a child namespace of 'a join b' and also of
            // 'a join b join c'.
            usingScope.addChild(ns, alias);
        }
    }

    public SqlWindow lookupWindow(String name)
    {
        // Lookup window in enclosing select.
        if (usingScope != null) {
            return usingScope.lookupWindow(name);
        } else {
            return null;
        }
    }

    /**
     * Returns the scope which is used for resolving USING clause.
     */
    public SqlValidatorScope getUsingScope()
    {
        return usingScope;
    }
}

// End JoinScope.java
