/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.runtime;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * A <code>ResultSetIterator</code> is an adapter which converts a {@link
 * ResultSet} to a {@link Iterator}.
 */
public class ResultSetIterator implements Iterator
{
    //~ Instance fields -------------------------------------------------------

    protected ResultSet resultSet;
    private Object row;

    //~ Constructors ----------------------------------------------------------

    public ResultSetIterator(ResultSet resultSet)
    {
        // NOTE jvs 4-Mar-2004:  I changed this to not call makeRow() from
        // this constructor, since subclasses aren't initialized yet.  Now
        // it follows the same pattern as CalcIterator.
        this.resultSet = resultSet;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean hasNext()
    {
        if (row != null) {
            return true;
        }
        moveToNext();
        return row != null;
    }

    public Object next()
    {
        if (row == null) {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
        }
        Object result = row;
        row = null;
        return result;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates an object representing the current row of the result set. The
     * default implementation of this method returns a {@link Row}, but
     * derived classes may override this.
     */
    protected Object makeRow() throws SQLException
    {
        return new Row(resultSet);
    }

    private void moveToNext()
    {
        try {
            if (resultSet.next()) {
                row = makeRow();
            } else {
                row = null;
            }
        } catch (SQLException e) {
            throw new SaffronError(e);
        }
    }
}


// End ResultSetIterator.java
