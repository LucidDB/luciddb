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

import java.util.*;

import org.eigenbase.util.*;


/**
 * LurqlPathSpec represents the parsed form of a path specification in a LURQL
 * query.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LurqlPathSpec
    extends LurqlQueryNode
{
    //~ Instance fields --------------------------------------------------------

    private final List<LurqlQueryNode> branches;

    private final LurqlPathSpec gatherThen;

    private final boolean gatherParent;

    //~ Constructors -----------------------------------------------------------

    public LurqlPathSpec(
        List<LurqlQueryNode> branches,
        LurqlPathSpec gatherThen,
        boolean gatherParent)
    {
        this.branches = Collections.unmodifiableList(branches);
        this.gatherThen = gatherThen;
        this.gatherParent = gatherParent;
    }

    //~ Methods ----------------------------------------------------------------

    public List<LurqlQueryNode> getBranches()
    {
        return branches;
    }

    public boolean isGather()
    {
        return gatherThen != null;
    }

    public LurqlPathSpec getGatherThenSpec()
    {
        return gatherThen;
    }

    public boolean isGatherParent()
    {
        return gatherParent;
    }

    // implement LurqlQueryNode
    public void unparse(PrintWriter pw)
    {
        pw.println("(");
        pw.write(StackWriter.INDENT);
        int k = 0;
        for (LurqlQueryNode branch : branches) {
            if (k++ > 0) {
                pw.println("union");
            }
            branch.unparse(pw);
            pw.println();
        }
        pw.write(StackWriter.OUTDENT);
        pw.print(")");
        if (isGather()) {
            pw.print(" gather ");
            if (isGatherParent()) {
                pw.print("with parent ");
            }
            pw.print("then ");
            gatherThen.unparse(pw);
        }
    }
}

// End LurqlPathSpec.java
