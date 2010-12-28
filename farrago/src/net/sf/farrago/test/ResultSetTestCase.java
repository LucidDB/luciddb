/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import org.eigenbase.sql.test.AbstractSqlTester;
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
        AbstractSqlTester.compareResultSet(resultSet, refSet);
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
        AbstractSqlTester.compareResultSetWithPattern(resultSet, pattern);
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
        AbstractSqlTester.compareResultSetWithDelta(resultSet, expected, delta);
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
        AbstractSqlTester.compareResultList(resultSet, refList);
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
        AbstractSqlTester.compareResultLists(resultSet, refLists);
    }
}

// End ResultSetTestCase.java
