/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.oj.stmt;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


/**
 * PreparedExplanation is a PreparedResult for an EXPLAIN PLAN statement. It's
 * always good to have an explanation prepared.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class PreparedExplanation implements PreparedResult
{
    private final RelNode rel;

    PreparedExplanation(RelNode rel)
    {
        this.rel = rel;
    }

    public String getCode()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelOptPlanWriter planWriter = new RelOptPlanWriter(pw);
        planWriter.withIdPrefix = false;
        rel.explain(planWriter);
        pw.flush();
        return sw.toString();
    }

    public boolean isDml()
    {
        return false;
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
