/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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

package net.sf.saffron.sql.test;

import net.sf.saffron.sql.SqlOperatorTable;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * A class that is capable of enumerating over all the SqlOperators defined in
 * {@link net.sf.saffron.sql.SqlOperatorTable}.
 *
 * @author wael
 * @since May 25, 2004
 * @version $Id$
 **/
public class SqlOperatorIterator implements Iterator
{

    private ArrayList allSqlOperators =
            SqlOperatorTable.instance().getOperatorList();
    private Iterator  allSqlOperatorsIt;

    public SqlOperatorIterator() {
        allSqlOperatorsIt = allSqlOperators.iterator();
    }

    public boolean hasNext() {
        return allSqlOperatorsIt.hasNext();
    }

    public Object next() {
        return allSqlOperatorsIt.next();
    }

    public void remove() {
        allSqlOperatorsIt.remove();
    }
}
