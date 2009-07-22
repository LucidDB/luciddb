/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import java.sql.*;


/**
 * A <code>ResultSetTupleIter</code> is an adapter which converts a {@link
 * ResultSet} to a {@link TupleIter}.
 */
public class ResultSetTupleIter
    extends AbstractTupleIter
{
    //~ Instance fields --------------------------------------------------------

    protected ResultSet resultSet;
    protected boolean endOfStream;
    protected boolean underflow;
    protected Object row;

    //~ Constructors -----------------------------------------------------------

    public ResultSetTupleIter(ResultSet resultSet)
    {
        // NOTE jvs 4-Mar-2004:  I changed this to not call makeRow() from
        // this constructor, since subclasses aren't initialized yet.  Now
        // it follows the same pattern as CalcTupleIter.
        this.resultSet = resultSet;
        underflow = endOfStream = false;
    }

    //~ Methods ----------------------------------------------------------------

    public Object fetchNext()
    {
        underflow = false;              // trying again
        // here row may not be null, after restart()
        if (row == null && !endOfStream) {
            row = getNextRow();
        }
        if (endOfStream) {
            return NoDataReason.END_OF_DATA;
        } else if (underflow) {
            return NoDataReason.UNDERFLOW;
        }
        Object result = row;
        row = null;
        return result;
    }

    protected Object getNextRow() throws TimeoutException
    {
        try {
            if (resultSet.next()) {
                return makeRow();
            } else {
                // remember EOS, some ResultSet impls dislike an extra next()
                endOfStream = true;
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
     * default implementation of this method returns a {@link Row}, but derived
     * classes may override this.
     */
    protected Object makeRow()
        throws SQLException
    {
        return new Row(resultSet);
    }

}

// End ResultSetTupleIter.java
