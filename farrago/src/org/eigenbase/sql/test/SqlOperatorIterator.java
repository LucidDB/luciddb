/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.sql.test;

import java.util.*;

import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.SqlOperatorTable;


/**
 * A class that is capable of enumerating over all the SqlOperators defined in
 * {@link org.eigenbase.sql.SqlOperatorTable}.
 *
 * @author wael
 * @since May 25, 2004
 * @version $Id$
 **/
public class SqlOperatorIterator implements Iterator
{
    //~ Instance fields -------------------------------------------------------

    private List allSqlOperators =
        SqlStdOperatorTable.instance().getOperatorList();
    private Iterator allSqlOperatorsIt;

    //~ Constructors ----------------------------------------------------------

    public SqlOperatorIterator()
    {
        allSqlOperatorsIt = allSqlOperators.iterator();
    }

    //~ Methods ---------------------------------------------------------------

    public boolean hasNext()
    {
        return allSqlOperatorsIt.hasNext();
    }

    public Object next()
    {
        return allSqlOperatorsIt.next();
    }

    public void remove()
    {
        allSqlOperatorsIt.remove();
    }
}
