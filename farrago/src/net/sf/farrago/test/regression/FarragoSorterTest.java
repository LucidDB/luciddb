/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.test.regression;

import java.io.*;
import java.sql.*;
import java.util.*;

import junit.extensions.*;
import junit.framework.*;

import net.sf.farrago.fem.config.*;
import net.sf.farrago.test.*;
import net.sf.farrago.util.*;


/**
 * FarragoSorterTest tests the sorter with various data sizes and distributions
 * (TODO: and types).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoSorterTest extends FarragoTestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static File testdataDir;
    private static long externalCount = -1;

    /**
     * Number of records to generate for in-memory sort.
     */
    private static final long IN_MEM_COUNT = 50;

    /**
     * Ratio of value range to record count for sparse sort.
     */
    private static final long SPARSE_FACTOR = 10;

    /**
     * Ratio of record count to value range for duplicate-heavy sort.
     */
    private static final long DUP_FACTOR = 10;

    /**
     * Ratio of data sort size to cache size for external sort.
     */
    private static final long EXTERNAL_SCALE_FACTOR = 20;

    //~ Constructors ----------------------------------------------------------

    public FarragoSorterTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        TestSuite suite = new TestSuite(FarragoSorterTest.class);
        TestSetup wrapper =
            new TestSetup(suite) {
                protected void setUp()
                    throws Exception
                {
                    staticSetUp();
                }

                protected void tearDown()
                    throws Exception
                {
                    FarragoTestCase.staticTearDown();
                }
            };
        return wrapper;
    }

    public static void staticSetUp()
        throws Exception
    {
        FarragoTestCase.staticSetUp();
        computeExternalCount();
        initializeDataDir();

        FarragoSqlTest setup =
            new FarragoSqlTest("testgen/FarragoSorterTest/setup.sql");
        setup.run();
    }

    private static void initializeDataDir()
    {
        // create a private directory for generated datafiles
        String homeDir = FarragoProperties.instance().homeDir.get();
        testdataDir = new File(homeDir, "testgen");
        testdataDir = new File(testdataDir, "FarragoSorterTest");
        testdataDir = new File(testdataDir, "data");

        // wipe out any existing contents
        FarragoFileAllocation dirAlloc =
            new FarragoFileAllocation(testdataDir);
        dirAlloc.closeAllocation();
        testdataDir.mkdir();
    }

    private void createForeignTable()
        throws Exception
    {
        stmt.executeUpdate("create foreign table sortertest.\"" + getName()
            + "\"(" + "pk bigint not null," + "val bigint not null) "
            + "server csv_server " + "options (table_name '" + getName()
            + "')");
    }

    private static void computeExternalCount()
    {
        // compute external count dynamically based on cache size
        FemFennelConfig fennelConfig =
            repos.getCurrentConfig().getFennelConfig();

        // first, compute number of bytes in cache
        long nBytes = fennelConfig.getCachePageSize();
        nBytes *= fennelConfig.getCachePagesInit();

        // next, scale up to desired sort size
        nBytes *= EXTERNAL_SCALE_FACTOR;

        // finally, convert from bytes to records, assuming 16 bytes per record
        externalCount = nBytes / 16;
    }

    private void testDistribution(DistributionGenerator gen)
        throws Exception
    {
        // create a file to contain the generated data
        File dataFile = new File(testdataDir, getName() + ".csv");
        FileWriter fileWriter = new FileWriter(dataFile);
        PrintWriter pw = new PrintWriter(fileWriter);

        // first line of file is column headings
        pw.println("PK,VAL");

        Random random = new Random(0);

        // TODO:  use an order-independent checksum instead
        long sum = 0;
        long pkSum = 0;

        for (long pk = 0; pk < gen.nRecords; ++pk) {
            long val = gen.generateValue(pk);
            sum += val;
            pkSum += pk;

            // write row
            pw.print(pk);
            pw.print(",");
            pw.print(val);
            pw.println();
        }

        pw.close();
        fileWriter.close();

        createForeignTable();

        resultSet =
            stmt.executeQuery("select pk,val from sortertest.\"" + getName()
                + "\" order by val");

        long sortSum = 0;
        long pkSortSum = 0;
        long lastVal = Long.MIN_VALUE;

        while (resultSet.next()) {
            long pk = resultSet.getLong(1);
            pkSortSum += pk;
            long val = resultSet.getLong(2);
            sortSum += val;
            assertTrue(val >= lastVal);
            lastVal = val;
        }

        resultSet.close();

        assertEquals(sum, sortSum);
        assertEquals(pkSum, pkSortSum);
    }

    /**
     * Tests an in-memory sort with mostly distinct values.
     */
    public void testInMemorySparse()
        throws Exception
    {
        testDistribution(
            new UniformDistributionGenerator(IN_MEM_COUNT,
                IN_MEM_COUNT * SPARSE_FACTOR));
    }

    /**
     * Tests an in-memory sort with many duplicate values.
     */
    public void testInMemoryDups()
        throws Exception
    {
        testDistribution(
            new UniformDistributionGenerator(IN_MEM_COUNT,
                IN_MEM_COUNT / DUP_FACTOR));
    }

    /**
     * Tests an in-memory sort with all values the same.
     */
    public void testInMemoryDegenerate()
        throws Exception
    {
        testDistribution(new UniformDistributionGenerator(IN_MEM_COUNT, 1));
    }

    /**
     * Tests an in-memory sort with sparse values already sorted.
     */
    public void testInMemoryPresortedSparse()
        throws Exception
    {
        testDistribution(
            new RampDistributionGenerator(IN_MEM_COUNT, 0, SPARSE_FACTOR));
    }

    /**
     * Tests an in-memory sort with duplicate values already sorted.
     */
    public void testInMemoryPresortedDups()
        throws Exception
    {
        testDistribution(
            new RampDistributionGenerator(IN_MEM_COUNT, 0, 1.0 / DUP_FACTOR));
    }

    /**
     * Tests an in-memory sort with sparse values already in reverse sort order.
     */
    public void testInMemoryPresortedSparseReverse()
        throws Exception
    {
        testDistribution(
            new RampDistributionGenerator(IN_MEM_COUNT, 0, -SPARSE_FACTOR));
    }

    /**
     * Tests an in-memory sort with duplicate values already in reverse sort
     * order.
     */
    public void testInMemoryPresortedDupsReverse()
        throws Exception
    {
        testDistribution(
            new RampDistributionGenerator(IN_MEM_COUNT, 0, -1.0 / DUP_FACTOR));
    }

    // TODO jvs 12-June-2004: add external sort tests once we have a real
    // sorter

    /**
     * Tests an external sort with mostly distinct values.
     */
    public void _testExternalSparse()
        throws Exception
    {
        testDistribution(
            new UniformDistributionGenerator(externalCount,
                externalCount * SPARSE_FACTOR));
    }

    //~ Inner Classes ---------------------------------------------------------

    private static abstract class DistributionGenerator
    {
        long nRecords;

        DistributionGenerator(long nRecords)
        {
            this.nRecords = nRecords;
        }

        abstract long generateValue(long pk);
    }

    private static class UniformDistributionGenerator
        extends DistributionGenerator
    {
        Random random;
        long maxValue;

        UniformDistributionGenerator(
            long nRecords,
            long maxValue)
        {
            super(nRecords);
            this.maxValue = maxValue;

            random = new Random(0);
        }

        long generateValue(long pk)
        {
            return Math.abs(random.nextLong()) % maxValue;
        }
    }

    private static class RampDistributionGenerator
        extends DistributionGenerator
    {
        long intercept;
        double slope;

        RampDistributionGenerator(
            long nRecords,
            long intercept,
            double slope)
        {
            super(nRecords);
            this.intercept = intercept;
            this.slope = slope;
        }

        long generateValue(long pk)
        {
            return (long) (intercept + (slope * pk));
        }
    }
}


// End FarragoSorterTest.java
