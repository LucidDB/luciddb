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

import net.sf.saffron.sql.type.SqlTypeName;

/**
 * A SqlTester is a tester of sql queries. The queries tested are specified in
 * {@link net.sf.saffron.sql.SqlOperator#test}.
 * Operators of base class {@link net.sf.saffron.sql.SqlOperator} added to the
 * singleton classes {@link net.sf.saffron.sql.SqlOperatorTable}
 * are visited by calling their test functions.
 *
 * @author wael
 * @since May 22, 2004
 * @version $Id$
 **/
public interface SqlTester {
    void checkScalarExact(String expression, String result);
    void checkScalarApprox(String expression, String result);
    void checkBoolean(String expression, String result);
    void checkString(String expression, String result);
    void checkNull(String expression);

    void check(String query, String result, SqlTypeName resultType);

}

// End SqlTester.java
