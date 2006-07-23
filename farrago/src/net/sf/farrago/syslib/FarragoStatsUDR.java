/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package net.sf.farrago.syslib;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.test.*;

import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * FarragoStatsUDR implements system procedures for manipulating Farrago
 * statistics stored in the repository. The procedures are intended to be used
 * for testing purposes.
 *
 * @author John Pham
 * @version $Id$
 */
public abstract class FarragoStatsUDR
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the row count for a table
     */
    public static void set_row_count(
        String catalog,
        String schema,
        String table,
        long rowCount)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoStatsUtil.setTableRowCount(
                sess,
                catalog,
                schema,
                table,
                rowCount);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * Sets the page count for an index
     */
    public static void set_page_count(
        String catalog,
        String schema,
        String index,
        long pageCount)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoStatsUtil.setIndexPageCount(
                sess,
                catalog,
                schema,
                index,
                pageCount);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * Creates a column histogram
     */
    public static void set_column_histogram(
        String catalog,
        String schema,
        String table,
        String column,
        long distinctValues,
        int samplePercent,
        long sampleDistinctValues,
        int distributionType,
        String valueDigits)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoStatsUtil.createColumnHistogram(
                sess,
                catalog,
                schema,
                table,
                column,
                distinctValues,
                samplePercent,
                sampleDistinctValues,
                distributionType,
                valueDigits);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }
}

// End FarragoStatsUDR.java
