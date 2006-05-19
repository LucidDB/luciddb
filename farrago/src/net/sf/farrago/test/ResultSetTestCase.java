/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package net.sf.farrago.test;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

import org.eigenbase.test.DiffTestCase;

/**
 * ResultSetTestCase (refactroed from FarragoTestCase) is a abstract base
 * for JUnit tests (see FarragoJdbcTest) that uses result sets.
 *
 * @author Angel Chang
 * @version $Id$
 */
public abstract class ResultSetTestCase extends DiffTestCase
{
    //~ Static fields/initializers --------------------------------------------

    //~ Instance fields -------------------------------------------------------

    /** ResultSet for processing queries. */
    protected ResultSet resultSet;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoTestCase object.
     *
     * @param testName .
     *
     * @throws Exception .
     */
    protected ResultSetTestCase(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return the number of rows in resultSet (which is consumed as a side
     * effect)
     */
    protected int getResultSetCount()
        throws Exception
    {
        int n = 0;
        while (resultSet.next()) {
            ++n;
        }
        resultSet.close();
        return n;
    }


    /**
     * Compares the first column of a result set against a String-valued
     * reference set, disregarding order entirely.
     *
     * @param refSet expected results
     *
     * @throws Exception .
     */
    protected void compareResultSet(Set refSet)
        throws Exception
    {
        Set actualSet = new HashSet();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        assertEquals(refSet, actualSet);
    }

    /**
     * Compares the first column of a result set against a pattern. The result
     * set must return exactly one row.
     *
     * @param pattern Expected pattern
     */
    protected void compareResultSetWithPattern(Pattern pattern)
        throws Exception
    {
        if (!resultSet.next()) {
            fail("Query returned 0 rows, expected 1");
        }
        String actual = resultSet.getString(1);
        if (resultSet.next()) {
            fail("Query returned 2 or more rows, expected 1");
        }
        if (!pattern.matcher(actual).matches()) {
            fail("Query returned '" + actual + "', expected '"
                + pattern.pattern() + "'");
        }
    }

    /**
     * Compares the first column of a result set against a numeric result,
     * within a given tolerance. The result set must return exactly one row.
     *
     * @param expected Expected result
     * @param delta Tolerance
     */
    protected void compareResultSetWithDelta(
        double expected,
        double delta) throws Exception
    {
        if (!resultSet.next()) {
            fail("Query returned 0 rows, expected 1");
        }
        double actual = resultSet.getDouble(1);
        if (resultSet.next()) {
            fail("Query returned 2 or more rows, expected 1");
        }
        if (actual < expected - delta || actual > expected + delta) {
            fail("Query returned " + actual +
                ", expected " + expected +
                (delta == 0 ? "" : ("+/-" + delta)));
        }
    }

    /**
     * Compares the first column of a result set against a String-valued
     * reference set, taking order into account.
     *
     * @param refList expected results
     *
     * @throws Exception .
     */
    protected void compareResultList(List refList)
        throws Exception
    {
        List actualSet = new ArrayList();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        assertEquals(refList, actualSet);
    }
}


// End FarragoTestCase.java
