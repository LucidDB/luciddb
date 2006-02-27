/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package org.eigenbase.runtime;

import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * A <code>ResultSetTupleIter</code> is an adapter which converts a {@link
 * ResultSet} to a {@link TupleIter}.
 */
public class ResultSetTupleIter implements TupleIter
{
    //~ Instance fields -------------------------------------------------------

    protected ResultSet resultSet;
    private Object row;
    private boolean endOfStream;

    //~ Constructors ----------------------------------------------------------

    public ResultSetTupleIter(ResultSet resultSet)
    {
        // NOTE jvs 4-Mar-2004:  I changed this to not call makeRow() from
        // this constructor, since subclasses aren't initialized yet.  Now
        // it follows the same pattern as CalcTupleIter.
        this.resultSet = resultSet;
        endOfStream = false;
    }

    //~ Methods ---------------------------------------------------------------

    public Object fetchNext()
    {
        // If restart() is called, row may be non-null upon entry to this
        // method.
        if (row == null) {
            moveToNext();
            if (endOfStream) {
                return NoDataReason.END_OF_DATA;
            }
        }

        Object result = row;
        row = null;
        return result;
    }

    public void restart()
    {
        try {
            if (resultSet.first()) {
                endOfStream = false;
                row = makeRow();
            } else {
                row = null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeAllocation()
    {
        // REVIEW: SWZ: 2/23/2006: Call close on resultSet?
        // resultSet.close();
    }

    /**
     * Creates an object representing the current row of the result set. The
     * default implementation of this method returns a {@link Row}, but
     * derived classes may override this.
     */
    protected Object makeRow()
        throws SQLException
    {
        return new Row(resultSet);
    }

    private void moveToNext()
    {
        try {
            if (endOfStream) {
                return;
            }
            if (resultSet.next()) {
                row = makeRow();
            } else {
                // record endOfStream since some ResultSet implementations don't
                // like extra calls to next() after it returns false
                endOfStream = true;
                row = null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}


// End ResultSetTupleIter.java
