/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
package org.eigenbase.rex;

import java.io.*;

import org.eigenbase.sql.*;


/**
 * Specification of the window of rows over which a {@link RexOver} windowed
 * aggregate is evaluated.
 *
 * <p>Treat it as immutable!
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 6, 2004
 */
public class RexWindow
{
    //~ Instance fields --------------------------------------------------------

    public final RexNode [] partitionKeys;
    public final RexNode [] orderKeys;
    private final SqlNode lowerBound;
    private final SqlNode upperBound;
    private final boolean physical;
    private final String digest;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a window.
     *
     * <p>If you need to create a window from outside this package, use {@link
     * RexBuilder#makeOver}.
     */
    RexWindow(
        RexNode [] partitionKeys,
        RexNode [] orderKeys,
        SqlNode lowerBound,
        SqlNode upperBound,
        boolean physical)
    {
        assert partitionKeys != null;
        assert orderKeys != null;
        this.partitionKeys = partitionKeys;
        this.orderKeys = orderKeys;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.physical = physical;
        this.digest = computeDigest();
        if (!physical) {
            assert orderKeys.length > 0 : "logical window requires sort key";
        }
    }

    //~ Methods ----------------------------------------------------------------

    public String toString()
    {
        return digest;
    }

    public int hashCode()
    {
        return digest.hashCode();
    }

    public boolean equals(Object that)
    {
        if (that instanceof RexWindow) {
            RexWindow window = (RexWindow) that;
            return digest.equals(window.digest);
        }
        return false;
    }

    private String computeDigest()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.print("(");
        int clauseCount = 0;
        if (partitionKeys.length > 0) {
            if (clauseCount++ > 0) {
                pw.print(' ');
            }
            pw.print("PARTITION BY ");
            for (int i = 0; i < partitionKeys.length; i++) {
                RexNode partitionKey = partitionKeys[i];
                pw.print(partitionKey.toString());
            }
        }
        if (orderKeys.length > 0) {
            if (clauseCount++ > 0) {
                pw.print(' ');
            }
            pw.print("ORDER BY ");
            for (int i = 0; i < orderKeys.length; i++) {
                RexNode orderKey = orderKeys[i];
                pw.print(orderKey.toString());
            }
        }
        if (lowerBound == null) {
            // No ROWS or RANGE clause
        } else if (upperBound == null) {
            if (clauseCount++ > 0) {
                pw.print(' ');
            }
            if (physical) {
                pw.print("ROWS ");
            } else {
                pw.print("RANGE ");
            }
            pw.print(lowerBound.toString());
        } else {
            if (clauseCount++ > 0) {
                pw.print(' ');
            }
            if (physical) {
                pw.print("ROWS BETWEEN ");
            } else {
                pw.print("RANGE BETWEEN ");
            }
            pw.print(lowerBound.toString());
            pw.print(" AND ");
            pw.print(upperBound.toString());
        }
        pw.print(")");
        return sw.toString();
    }

    public SqlNode getLowerBound()
    {
        return lowerBound;
    }

    public SqlNode getUpperBound()
    {
        return upperBound;
    }

    public boolean isRows()
    {
        return physical;
    }
}

// End RexWindow.java
