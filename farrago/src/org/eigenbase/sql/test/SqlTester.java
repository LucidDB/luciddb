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

import org.eigenbase.sql.type.SqlTypeName;


/**
 * A SqlTester is a tester of sql queries. The queries tested are specified in
 * {@link org.eigenbase.sql.SqlOperator#test}.
 * Operators of base class {@link org.eigenbase.sql.SqlOperator} added to the
 * singleton classes {@link org.eigenbase.sql.SqlOperatorTable}
 * are visited by calling their test functions.
 *
 * @author Wael Chatila
 * @since May 22, 2004
 * @version $Id$
 **/
public interface SqlTester
{
    //~ Methods ---------------------------------------------------------------

    void checkScalarExact(
        String expression,
        String result);

    void checkScalarApprox(
        String expression,
        String result);

    void checkBoolean(
        String expression,
        Boolean result);

    void checkString(
        String expression,
        String result);

    void checkNull(String expression);

    void check(
        String query,
        String result,
        SqlTypeName resultType);
}


// End SqlTester.java
