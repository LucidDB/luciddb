/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
