/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
import java.util.regex.*;

import org.eigenbase.test.*;


/**
 * ResultSetTestCase (refactroed from FarragoTestCase) is a abstract base for
 * JUnit tests (see FarragoJdbcTest) that uses result sets.
 *
 * @author Angel Chang
 * @version $Id$
 */
public abstract class ResultSetTestCase
    extends DiffTestCase
{
    //~ Instance fields --------------------------------------------------------

    /**
     * ResultSet for processing queries.
     */
    protected ResultSet resultSet;

    //~ Constructors -----------------------------------------------------------

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

    //~ Methods ----------------------------------------------------------------

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
    protected void compareResultSet(Set<String> refSet)
        throws Exception
    {
        Set<String> actualSet = new HashSet<String>();
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
            fail(
                "Query returned '" + actual + "', expected '"
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
        double delta)
        throws Exception
    {
        if (!resultSet.next()) {
            fail("Query returned 0 rows, expected 1");
        }
        double actual = resultSet.getDouble(1);
        if (resultSet.next()) {
            fail("Query returned 2 or more rows, expected 1");
        }
        if ((actual < (expected - delta)) || (actual > (expected + delta))) {
            fail(
                "Query returned " + actual
                + ", expected " + expected
                + ((delta == 0) ? "" : ("+/-" + delta)));
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
    protected void compareResultList(List<String> refList)
        throws Exception
    {
        List<String> actualSet = new ArrayList<String>();
        while (resultSet.next()) {
            String s = resultSet.getString(1);
            actualSet.add(s);
        }
        resultSet.close();
        assertEquals(refList, actualSet);
    }

    /**
     * Compares the columns of a result set against several String-valued
     * reference lists, taking order into account.
     *
     * @param refLists vararg of List<String>. The first list is compared to the
     * first column, the second list to the second column and so on
     */
    protected void compareResultLists(List<String> ... refLists)
        throws Exception
    {
        int numExpectedColumns = refLists.length;

        assertTrue(numExpectedColumns > 0);

        assertTrue(
            resultSet.getMetaData().getColumnCount() >= numExpectedColumns);

        int numExpectedRows = -1;

        List<List<String>> actualLists = new ArrayList<List<String>>();
        for (int i = 0; i < numExpectedColumns; i++) {
            actualLists.add(new ArrayList<String>());

            if (i == 0) {
                numExpectedRows = refLists[i].size();
            } else {
                assertEquals(
                    "num rows differ across ref lists",
                    numExpectedRows,
                    refLists[i].size());
            }
        }

        while (resultSet.next()) {
            for (int i = 0; i < numExpectedColumns; i++) {
                String s = resultSet.getString(i + 1);

                actualLists.get(i).add(s);
            }
        }
        resultSet.close();

        for (int i = 0; i < numExpectedColumns; i++) {
            assertEquals(
                "column mismatch in column " + (i + 1),
                refLists[i],
                actualLists.get(i));
        }
    }
}

// End ResultSetTestCase.java
