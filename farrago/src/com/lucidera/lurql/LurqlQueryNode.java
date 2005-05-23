/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lurql;

import org.eigenbase.util.*;

import java.io.*;
import java.util.*;

/**
 * LurqlQueryNode is an abstract base class representing a node
 * in a LURQL parse tree.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class LurqlQueryNode
{
    /**
     * Converts this node to text.
     *
     * @param pw the PrintWriter on which to unparse; must have
     * an underlying {@link StackWriter} to interpret indentation
     */
    public abstract void unparse(PrintWriter pw);

    // override Object
    public String toString()
    {
        StringWriter sw = new StringWriter();
        StackWriter stack = new StackWriter(sw, StackWriter.INDENT_SPACE4);
        PrintWriter pw = new PrintWriter(stack);
        unparse(pw);
        pw.close();
        return sw.toString();
    }

    protected void unparseFilterList(PrintWriter pw, List filterList)
    {
        if (!filterList.isEmpty()) {
            pw.println();
            pw.println("where");
            pw.write(StackWriter.INDENT);
            Iterator iter = filterList.iterator();
            while (iter.hasNext()) {
                LurqlFilter filter = (LurqlFilter) iter.next();
                filter.unparse(pw);
                if (iter.hasNext()) {
                    pw.println(" and");
                }
            }
            pw.write(StackWriter.OUTDENT);
        }
    }
}

// End LurqlQueryNode.java
