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

package org.eigenbase.sql;

import org.eigenbase.sql.test.SqlTester;


/**
 * SqlSetOperator represents a relational set theory operator
 * (UNION, INTERSECT, MINUS).  These are binary operators, but with
 * an extra boolean attribute tacked on for whether to remove duplicates
 * (e.g. UNION ALL does not remove duplicates).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlSetOperator extends SqlBinaryOperator
{
    //~ Instance fields -------------------------------------------------------

    public final boolean all;

    //~ Constructors ----------------------------------------------------------

    public SqlSetOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean all)
    {
        super(name, kind, prec, true, null, null, null);
        this.all = all;
    }

    //~ Methods ---------------------------------------------------------------

    public void test(SqlTester tester)
    {
        /* empty implementation */
    }
}


// End SqlSetOperator.java
