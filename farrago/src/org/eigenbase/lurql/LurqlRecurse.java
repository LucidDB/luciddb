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


/**
 * LurqlRecurse represents a parsed RECURSIVELY clause in a LURQL query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlRecurse
    extends LurqlPathBranch
{
    //~ Instance fields --------------------------------------------------------

    private final LurqlPathSpec pathSpec;

    //~ Constructors -----------------------------------------------------------

    public LurqlRecurse(LurqlPathSpec pathSpec, LurqlPathSpec thenSpec)
    {
        super(null, thenSpec);
        this.pathSpec = pathSpec;
    }

    //~ Methods ----------------------------------------------------------------

    public LurqlPathSpec getPathSpec()
    {
        return pathSpec;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.print("recursively ");
        pathSpec.unparse(pw);
        unparseThenSpec(pw);
    }
}

// End LurqlRecurse.java
