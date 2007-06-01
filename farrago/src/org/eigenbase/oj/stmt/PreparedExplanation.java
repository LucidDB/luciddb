/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.oj.stmt;

import java.io.*;

import java.sql.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * PreparedExplanation is a PreparedResult for an EXPLAIN PLAN statement. It's
 * always good to have an explanation prepared.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class PreparedExplanation
    implements PreparedResult
{
    //~ Instance fields --------------------------------------------------------

    private final RelDataType rowType;
    private final RelNode rel;
    private final boolean asXml;
    private final SqlExplainLevel detailLevel;

    //~ Constructors -----------------------------------------------------------

    public PreparedExplanation(
        RelDataType rowType,
        RelNode rel,
        boolean asXml,
        SqlExplainLevel detailLevel)
    {
        this.rowType = rowType;
        this.rel = rel;
        this.asXml = asXml;
        this.detailLevel = detailLevel;
    }

    //~ Methods ----------------------------------------------------------------

    public String getCode()
    {
        if (rel == null) {
            return RelOptUtil.dumpType(rowType);
        } else {
            return RelOptUtil.dumpPlan("", rel, asXml, detailLevel);
        }
    }

    public boolean isDml()
    {
        return false;
    }

    public TableModificationRel.Operation getTableModOp()
    {
        return null;
    }

    public RelNode getRel()
    {
        return rel;
    }

    public Object execute()
    {
        final String explanation = getCode();
        return executeStatic(explanation);
    }

    public static ResultSet executeStatic(final String explanation)
    {
        final LineNumberReader lineReader =
            new LineNumberReader(new StringReader(explanation));
        Iterator iter =
            new Iterator() {
                private String line;

                public boolean hasNext()
                {
                    if (line != null) {
                        return true;
                    }
                    try {
                        line = lineReader.readLine();
                    } catch (IOException ex) {
                        throw Util.newInternal(ex);
                    }
                    return (line != null);
                }

                public Object next()
                {
                    if (!hasNext()) {
                        return null;
                    }
                    String nextLine = line;
                    line = null;
                    return nextLine;
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            };
        return new IteratorResultSet(
            iter,
            new IteratorResultSet.SingletonColumnGetter());
    }
}

// End PreparedExplanation.java
