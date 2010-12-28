/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package org.eigenbase.lurql;

import java.io.*;

import org.eigenbase.util.*;


/**
 * LurqlPathBranch represents a parsed path branch (either FROM, FOLLOW, or
 * RECURSIVELY) in a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LurqlPathBranch
    extends LurqlQueryNode
{
    //~ Instance fields --------------------------------------------------------

    private final String aliasName;

    private final LurqlPathSpec thenSpec;

    //~ Constructors -----------------------------------------------------------

    protected LurqlPathBranch(String aliasName, LurqlPathSpec thenSpec)
    {
        this.aliasName = aliasName;
        this.thenSpec = thenSpec;
    }

    //~ Methods ----------------------------------------------------------------

    public String getAliasName()
    {
        return aliasName;
    }

    public LurqlPathSpec getThenSpec()
    {
        return thenSpec;
    }

    protected void unparseThenSpec(PrintWriter pw)
    {
        if (thenSpec != null) {
            pw.println();
            pw.print("then ");
            thenSpec.unparse(pw);
        }
    }

    protected void unparseAlias(PrintWriter pw)
    {
        if (aliasName != null) {
            pw.print(" as ");
            StackWriter.printSqlIdentifier(pw, aliasName);
        }
    }
}

// End LurqlPathBranch.java
