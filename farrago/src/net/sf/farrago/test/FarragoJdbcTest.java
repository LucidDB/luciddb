/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
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

import java.io.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.math.*;

import java.sql.*;
import java.sql.Date;

import java.util.*;
import java.util.logging.*;
import java.util.regex.*;

import junit.framework.*;

import net.sf.farrago.trace.*;

import org.eigenbase.util.*;
import org.eigenbase.util14.*;


// TODO jvs 17-Apr-2006:  break this monster up into a new package
// net.sf.farrago.test.jdbc.

/**
 * FarragoJdbcTest tests specifics of the Farrago implementation of the JDBC
 * API. See also unitsql/jdbc/*.sql. todo: test: 1. string too long for
 * char/varchar field 2. value which converted to char/varchar is too long 3.
 * various numeric values out of range, e.g. put 65537 in a tinyint 4. assign
 * boolean to integer columns (becomes 0/1) 5. assign numerics to boolean
 * 5a.small enough 5b out of range (not 0 or 1) 6. assign string to everything
 * 6a invalid string format to boolean, numerics 6b valid datetime string to
 * date, time, timestamp 7. casting betwen incompatible types 8. set null for
 * nonnullable columns 9. invalid parameter index
 *
 * @author Tim Leung
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcTest
    extends ResultSetTestCase
{

    //~ Static fields/initializers ---------------------------------------------

    /**
     * Logger to use for test tracing.
     */
    protected static final Logger tracer = FarragoTrace.getTestTracer();

    // Constants used by testDataTypes
    private static final byte minByte = Byte.MIN_VALUE;
    private static final byte maxByte = Byte.MAX_VALUE;
    private static final short minShort = Short.MIN_VALUE;
    private static final short maxShort = Short.MAX_VALUE;
    private static final int minInt = Integer.MIN_VALUE;
    private static final int maxInt = Integer.MAX_VALUE;
    private static final long minLong = Long.MIN_VALUE;
    private static final long maxLong = Long.MAX_VALUE;
    private static final float minFloat = Float.MIN_VALUE;
    private static final float maxFloat = Float.MAX_VALUE;
    private static final float floatValue1 = -1.5f;
    private static final float floatValue2 = 1.5f;

    // NOTE jvs 17-Apr-2006: We can't use Double.MIN_VALUE here because the
    // mingw32 build takes a larger number from MINDOUBLE in win32_values.h.
    // And we can't use Double.MAX_VALUE because something causes that to tip
    // over to POSITIVE_INFINITY when round-tripped via CHAR.  So we use
    // hard-coded approximations here.
    private static final double minDouble = 2.3e-308;
    private static final double maxDouble = 1.7e+308;
    private static final double doubleValue1 = -2.3;
    private static final double doubleValue2 = 2.3;
    private static final boolean boolValue = true;
    private static final BigDecimal bigDecimalValue =
        BigDecimal.valueOf(1035, 2);
    private static final String stringValue = "0";
    private static final byte [] bytes = { 127, -34, 56, 29, 56, 49 };

    /**
     * A point of time in Sydney, Australia. (The timezone is deliberately not
     * PST, where the test is likely to be run, or GMT, and should be in
     * daylight-savings time in December.) 4:22:33.456 PM on 21st December 2004
     * Japan (GMT+9) is 9 hours earlier in GMT, namely 7:22:33.456 AM on 21st
     * December 2004 GMT and is another 8 hours earlier in PST: 11:22:33.456 PM
     * on 20th December 2004 PST (GMT-8)
     */
    private static final Calendar sydneyCal =
        makeCalendar("JST", 2004, 11, 21, 16, 22, 33, 456);
    private static final Time time = new Time(sydneyCal.getTime().getTime());
    private static final Date date = new Date(sydneyCal.getTime().getTime());
    private static final Timestamp timestamp =
        new Timestamp(sydneyCal.getTime().getTime());

    private static final String dateStr = date.toString();
    private static final String timeStr = time.toString();
    private static final String timestampStr = timestamp.toString();

    private static final Time timeNoDate = Time.valueOf(timeStr);
    private static final Date dateNoTime = Date.valueOf(dateStr);
    private static final Timestamp timestampNoPrec =
        new Timestamp(timestamp.getTime() - 456);

    static {
        // Sanity check, assuming local time is PST
        String tzID = TimeZone.getDefault().getID();
        if (tzID.equals("America/Tijuana")) {
            String t = time.toString();
            assert t.equals("23:22:33") : t;
            String d = date.toString();
            assert d.equals("2004-12-20") : d;
        }
    }

    private static final Byte tinyIntObj = new Byte(minByte);
    private static final Short smallIntObj = new Short(maxShort);
    private static final Integer integerObj = new Integer(minInt);
    private static final Long bigIntObj = new Long(maxLong);
    private static final Float floatObj = new Float(maxFloat);
    private static final Double doubleObj = new Double(maxDouble);
    private static final Boolean boolObj = Boolean.FALSE;
    private static final BigDecimal decimalObj =
        new BigDecimal(13412342124143241D);
    private static final BigDecimal decimal73Obj = new BigDecimal(64.2341);
    private static final String charObj = "CHAR test string";
    private static final String varcharObj = "VARCHAR test string";
    private static final int TINYINT = 2;
    private static final int SMALLINT = 3;
    private static final int INTEGER = 4;
    private static final int BIGINT = 5;
    private static final int REAL = 6;
    private static final int FLOAT = 7;
    private static final int DOUBLE = 8;
    private static final int BOOLEAN = 9;
    private static final int CHAR = 10;
    private static final int VARCHAR = 11;
    private static final int BINARY = 12;
    private static final int VARBINARY = 13;
    private static final int TIME = 14;
    private static final int DATE = 15;
    private static final int TIMESTAMP = 16;
    private static final int DECIMAL = 17;
    private static final int DECIMAL73 = 18;
    private static boolean schemaExists = false;
    private static final String [] columnNames =
        new String[TestSqlType.all.length];
    protected static String columnTypeStr = "";
    protected static String columnStr = "";
    protected static String paramStr = "";

    static {
        for (int i = 0; i < TestSqlType.all.length; i++) {
            final TestSqlType sqlType = TestSqlType.all[i];
            assert sqlType.ordinal == (i + 2);
            columnNames[i] =
                "\"Column " + (i + 1) + ": " + sqlType.string + "\"";
            if (i > 0) {
                columnTypeStr += ", ";
                columnStr += ", ";
                paramStr += ", ";
            }
            columnTypeStr += (columnNames[i] + " " + sqlType.string);
            columnStr += columnNames[i];
            paramStr += "?";
        }
        columnTypeStr = "id integer primary key, " + columnTypeStr;
        columnStr = "id, " + columnStr;
        paramStr = "( ?, " + paramStr + ")";
    }

    // Flags indicating whether bugs have been fixed. (It's better to 'if'
    // out than to comment out code, because commented out code doesn't get
    // refactored.)
    private static final boolean todo = false;

    //~ Instance fields --------------------------------------------------------

    protected Object [] values;

    /**
     * Tester to use
     */
    private JdbcTester tester;

    /**
     * JDBC connection to Farrago database.
     */
    protected Connection connection;

    /**
     * PreparedStatement for processing queries.
     */
    protected PreparedStatement preparedStmt;

    /**
     * Statement for processing queries.
     */
    protected Statement stmt;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcTest object.
     */
    public FarragoJdbcTest(String testName)
        throws Exception
    {
        super(testName);
        tester = getTester(testName);
    }

    //~ Methods ----------------------------------------------------------------

    // Returns tester to use
    protected JdbcTester getTester(String name)
        throws Exception
    {
        return new FarragoJdbcTester(name);
    }

    // override DiffTestCase
    protected File getTestlogRoot()
        throws Exception
    {
        return FarragoTestCase.getTestlogRootStatic();
    }

    // implement TestCase
    public static Test suite()
    {
        return FarragoTestCase.wrappedSuite(FarragoJdbcTest.class);
    }

    /**
     * Creates a calendar.
     */
    private static Calendar makeCalendar(
        String tz,
        int year,
        int month,
        int date,
        int hour,
        int minute,
        int second,
        int millis)
    {
        final Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(year, month, date, hour, minute, second);
        cal.set(Calendar.MILLISECOND, millis);
        TimeZone timeZone = TimeZone.getTimeZone(tz);
        cal.setTimeZone(timeZone);
        return cal;
    }

    public void testJavaQuerySynchronousCancel()
        throws Exception
    {
        testQueryCancel(true, "JAVA");
    }

    public void testJavaQueryAsynchronousCancel()
        throws Exception
    {
        testQueryCancel(false, "JAVA");
    }

    public void testJavaQueryAsynchronousCancelRepeated()
        throws Exception
    {
        // use a seeded generator to make debugging more predictable
        final long seed = 1013L;
        Random rand = new Random(seed);

        // test nearly immediate cancellation
        for (int i = 0; i < 10; i++) {
            int millis = (int) (rand.nextDouble() * 5);
            testQueryCancel(millis, "JAVA");
        }

        // test more "reasonable" cancellation intervals
        rand.setSeed(seed);
        for (int i = 0; i < 10; i++) {
            int millis = (int) (rand.nextDouble() * 5000);
            testQueryCancel(millis, "JAVA");
        }
    }

    public void testFennelQuerySynchronousCancel()
        throws Exception
    {
        testQueryCancel(true, "FENNEL");
    }

    // FIXME jvs 11-Apr-2006:  re-enable once FNL-30 is fixed
    public void _testFennelQueryAsynchronousCancel()
        throws Exception
    {
        testQueryCancel(false, "FENNEL");
    }

    public void testUdxSynchronousCancel()
        throws Exception
    {
        testUdxCancel(true);
    }

    public void testUdxAsynchronousCancel()
        throws Exception
    {
        testUdxCancel(false);
    }

    private void testQueryCancel(boolean synchronous, String executorType)
        throws Exception
    {
        // Wait 2 seconds before cancel for asynchronous case
        testQueryCancel(synchronous ? 0 : 2000, executorType);
    }

    private void testQueryCancel(int waitMillis, String executorType)
        throws Exception
    {
        // cleanup
        String sql = "drop schema cancel_test cascade";
        try {
            stmt.execute(sql);
        } catch (SQLException ex) {
            // ignore
            Util.swallow(ex, tracer);
        }

        sql = "create schema cancel_test";
        stmt.execute(sql);
        sql =
            "create foreign table cancel_test.m(id int not null) "
            + "server sys_mock_foreign_data_server "
            + "options(executor_impl '"
            + executorType + "', row_count '1000000000')";
        stmt.execute(sql);
        if (executorType.equals("FENNEL")) {
            // For Fennel, we want to make sure it's down in
            // the ExecStreamGraph when the cancel request arrives
            sql = "select count(*) from cancel_test.m";
        } else {
            // But for Java, we want to test the checkCancel
            // in FarragoTupleIterResultSet, so don't count
            sql = "select * from cancel_test.m";
        }
        executeAndCancel(sql, waitMillis);
    }

    private void testUdxCancel(boolean synchronous)
        throws Exception
    {
        // Wait 2 seconds before cancel for asynchronous case
        testUdxCancel(synchronous ? 0 : 2000);
    }

    private void testUdxCancel(int waitMillis)
        throws Exception
    {
        // cleanup
        String sql = "drop schema cancel_test cascade";
        try {
            stmt.execute(sql);
        } catch (SQLException ex) {
            // ignore
        }

        sql = "create schema cancel_test";
        stmt.execute(sql);
        sql =
            "create function cancel_test.ramp(n int) returns table(i int) "
            + "language java parameter style system defined java "
            + "no sql external name "
            + "'class net.sf.farrago.test.FarragoTestUDR.ramp'";
        stmt.execute(sql);
        sql = "select * from table (cancel_test.ramp(1000000000))";
        executeAndCancel(sql, waitMillis);
    }

    protected boolean checkCancelException(SQLException ex)
    {
        return (ex.getMessage().indexOf("abort") > -1);
    }

    private void executeAndCancel(String sql, int waitMillis)
        throws SQLException
    {
        resultSet = stmt.executeQuery(sql);
        if (waitMillis == 0) {
            // cancel immediately
            stmt.cancel();
        } else {
            // Schedule timer to cancel after waitMillis
            Timer timer = new Timer(true);
            TimerTask task =
                new TimerTask() {
                    public void run()
                    {
                        Thread thread = Thread.currentThread();
                        thread.setName("FarragoJdbcCancelThread");
                        try {
                            tracer.fine("TimerTask "
                                + toStringThreadInfo(thread)
                                + " will cancel " + stmt);
                            stmt.cancel();
                        } catch (SQLException ex) {
                            Assert.fail(
                                "Cancel request failed:  "
                                + ex.getMessage());
                        }
                    }
                };
            tracer.fine("scheduling cancel task with delay=" +waitMillis);
            timer.schedule(task, waitMillis);
        }
        try {
            while (resultSet.next()) {
            }
        } catch (SQLException ex) {
            // expected
            Assert.assertTrue(
                "Expected statement cancelled message but got '"
                + ex.getMessage() + "'",
                checkCancelException(ex));
            return;
        }
        Assert.fail("Expected failure due to cancel request");
    }

    public void testCheckParametersSet()
        throws Exception
    {
        Throwable throwable = null;
        String query =
            "insert into datatypes_schema.dataTypes_table values " + paramStr;
        preparedStmt = connection.prepareStatement(query);
        preparedStmt.setInt(1, 100);
        try {
            preparedStmt.executeUpdate();
        } catch (SQLException ex) {
            throwable = ex;
        }

        checkThrowable("parameter not set",
            ".*Value is missing.*",
            throwable);

        values = new Object[2 + TestSqlType.all.length];
        checkSetNull();
        preparedStmt.clearParameters();
        try {
            preparedStmt.executeUpdate();
        } catch (SQLException ex) {
            throwable = ex;
        }

        checkThrowable("parameter not set",
            ".*Value is missing.*",
            throwable);
    }

    // NOTE jvs 26-July-2004:  some of the tests in this class modify fixture
    // tables such as SALES.EMPS, but that's OK, because transactions are
    // implicitly rolled back by FarragoTestCase.tearDown.
    public void testPreparedStmtDataTypes()
        throws Exception
    {
        String query =
            "insert into datatypes_schema.dataTypes_table values " + paramStr;
        checkPreparedStmtDataTypes(query);
    }

    public void checkPreparedStmtDataTypes(String query)
        throws Exception
    {
        preparedStmt = connection.prepareStatement(query);
        values = new Object[2 + TestSqlType.all.length];
        preparedStmt.setInt(1, 100);
        if (todo) {
            // TODO: Improve message for bad parameter index
            checkSetInvalidIndex(0);
        }
        checkSetNull();
        checkSetString();
        checkSetByteMin();
        checkSetByteMax();
        checkSetShortMin();
        checkSetShortMax();
        checkSetIntMin();
        checkSetIntMax();
        checkSetLongMin();
        checkSetLongMax();
        checkSetFloatMin();
        checkSetFloatMax();
        checkSetFloat();
        checkSetDoubleMin();
        checkSetDoubleMax();
        checkSetBooleanFalse();
        checkSetBigDecimal();
        checkSetBytes();
        checkSetDate();
        checkSetTime();
        checkSetTimestamp();
        checkSetObject();
    }

    protected void tearDown()
        throws Exception
    {
        tester.tearDown();
        super.tearDown();
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();
        tester.setUp();
        connection = tester.getConnection();
        stmt = tester.getStatement();

        synchronized (getClass()) {
            if (!schemaExists) {
                // cleanup
                String sql = "drop schema datatypes_schema cascade";
                try {
                    stmt.execute(sql);
                } catch (SQLException ex) {
                    // ignore
                    Util.swallow(ex, tracer);
                }
                stmt.executeUpdate("create schema datatypes_schema");
                stmt.executeUpdate(
                    "create table datatypes_schema.dataTypes_table ("
                    + columnTypeStr + ")");
            }
            schemaExists = true;
        }
    }

    private void checkSetObject()
        throws SQLException
    {
        // Test PreparedStatement.setObject(int,Object) for each kind
        // of object.
        preparedStmt.setObject(TINYINT, tinyIntObj);
        values[TINYINT] = tinyIntObj;
        preparedStmt.setObject(SMALLINT, smallIntObj);
        values[SMALLINT] = smallIntObj;
        preparedStmt.setObject(INTEGER, integerObj);
        values[INTEGER] = integerObj;
        preparedStmt.setObject(BIGINT, bigIntObj);
        values[BIGINT] = bigIntObj;
        preparedStmt.setObject(REAL, floatObj);
        values[REAL] = floatObj;
        preparedStmt.setObject(FLOAT, doubleObj);
        values[FLOAT] = doubleObj;
        preparedStmt.setObject(DOUBLE, doubleObj);
        values[DOUBLE] = doubleObj;
        preparedStmt.setObject(BOOLEAN, boolObj);
        values[BOOLEAN] = boolObj;
        preparedStmt.setObject(CHAR, charObj);
        values[CHAR] = charObj;
        preparedStmt.setObject(VARCHAR, varcharObj);
        values[VARCHAR] = varcharObj;
        preparedStmt.setObject(BINARY, bytes);
        values[BINARY] = bytes;
        preparedStmt.setObject(VARBINARY, bytes);
        values[VARBINARY] = bytes;
        preparedStmt.setObject(DATE, date);
        values[DATE] = date;
        preparedStmt.setObject(TIME, time);
        values[TIME] = time;
        preparedStmt.setObject(TIMESTAMP, timestamp);
        values[TIMESTAMP] = timestamp;
        preparedStmt.setObject(DECIMAL, decimalObj);
        values[DECIMAL] = decimalObj;
        preparedStmt.setObject(DECIMAL73, decimal73Obj);
        values[DECIMAL73] = decimal73Obj;
        checkResults(TestJavaType.Object);
    }

    private void checkSetTimestamp()
        throws Exception
    {
        checkSet(TestJavaType.Timestamp, TestSqlType.Char, timestamp);
        checkSet(TestJavaType.Timestamp, TestSqlType.Varchar, timestamp);
        checkSet(TestJavaType.Timestamp, TestSqlType.Date, timestamp);
        checkSet(TestJavaType.Timestamp, TestSqlType.Time, timestamp);
        checkSet(TestJavaType.Timestamp, TestSqlType.Timestamp, timestamp);
        checkResults(TestJavaType.Timestamp);
    }

    private void checkSetTime()
        throws Exception
    {
        checkSet(TestJavaType.Time, TestSqlType.Char, time);
        checkSet(TestJavaType.Time, TestSqlType.Varchar, time);
        checkSet(TestJavaType.Time, TestSqlType.Date, time);
        checkSet(TestJavaType.Time, TestSqlType.Time, time);
        checkSet(TestJavaType.Time, TestSqlType.Timestamp, time);
        checkResults(TestJavaType.Time);
    }

    private void checkSetDate()
        throws Exception
    {
        checkSet(TestJavaType.Date, TestSqlType.Char, date);
        checkSet(TestJavaType.Date, TestSqlType.Varchar, date);
        checkSet(TestJavaType.Date, TestSqlType.Date, date);
        checkSet(TestJavaType.Date, TestSqlType.Time, date);
        checkSet(TestJavaType.Date, TestSqlType.Timestamp, date);
        checkResults(TestJavaType.Date);
    }

    private void checkSetBytes()
        throws Exception
    {
        checkSet(TestJavaType.Bytes, TestSqlType.all, bytes);
        checkResults(TestJavaType.Bytes);
    }

    private void checkSetBigDecimal()
        throws Exception
    {
        checkSet(TestJavaType.BigDecimal, TestSqlType.all,
            bigDecimalValue);
        checkResults(TestJavaType.BigDecimal);
    }

    private void checkSetBooleanFalse()
        throws Exception
    {
        checkSet(
            TestJavaType.Boolean,
            TestSqlType.all,
            boolObj);
        checkResults(TestJavaType.Boolean);
    }

    private void checkSetDoubleMax()
        throws Exception
    {
        checkSet(
            TestJavaType.Double,
            TestSqlType.typesNumericAndChars,
            new Double(maxDouble));
        checkResults(TestJavaType.Double);
    }

    private void checkSetDoubleMin()
        throws Exception
    {
        checkSet(
            TestJavaType.Double,
            TestSqlType.typesNumericAndChars,
            new Double(minDouble));
        checkResults(TestJavaType.Double);
    }

    private void checkSetFloatMax()
        throws Exception
    {
        checkSet(
            TestJavaType.Float,
            TestSqlType.typesNumericAndChars,
            new Float(maxFloat));
        checkResults(TestJavaType.Float);
    }

    private void checkSetFloat()
        throws Exception
    {
        checkSet(
            TestJavaType.Float,
            TestSqlType.typesNumericAndChars,
            new Float(floatValue1));
        checkResults(TestJavaType.Float);
    }

    private void checkSetFloatMin()
        throws Exception
    {
        checkSet(
            TestJavaType.Float,
            TestSqlType.typesNumericAndChars,
            new Float(minFloat));
        checkResults(TestJavaType.Float);
    }

    private void checkSetLongMax()
        throws Exception
    {
        checkSet(
            TestJavaType.Long,
            TestSqlType.typesNumericAndChars,
            new Long(maxLong));
        checkResults(TestJavaType.Long);
    }

    private void checkSetLongMin()
        throws Exception
    {
        checkSet(
            TestJavaType.Long,
            TestSqlType.typesNumericAndChars,
            new Long(minLong));
        checkResults(TestJavaType.Long);
    }

    private void checkSetIntMax()
        throws Exception
    {
        checkSet(
            TestJavaType.Int,
            TestSqlType.typesNumericAndChars,
            new Integer(maxInt));
        checkResults(TestJavaType.Int);
    }

    private void checkSetIntMin()
        throws Exception
    {
        checkSet(
            TestJavaType.Int,
            TestSqlType.typesNumericAndChars,
            new Integer(minInt));
        checkResults(TestJavaType.Int);
    }

    private void checkSetShortMax()
        throws Exception
    {
        checkSet(
            TestJavaType.Short,
            TestSqlType.typesNumericAndChars,
            new Short(maxShort));
        checkResults(TestJavaType.Short);
    }

    private void checkSetShortMin()
        throws Exception
    {
        checkSet(
            TestJavaType.Short,
            TestSqlType.typesNumericAndChars,
            new Short(minShort));
        checkResults(TestJavaType.Short);
    }

    private void checkSetByteMax()
        throws Exception
    {
        checkSet(
            TestJavaType.Byte,
            TestSqlType.typesNumericAndChars,
            new Byte(maxByte));
        checkResults(TestJavaType.Byte);
        checkResults(TestJavaType.Bytes);
    }

    private void checkSetByteMin()
        throws Exception
    {
        checkSet(
            TestJavaType.Byte,
            TestSqlType.typesNumericAndChars,
            new Byte(minByte));
        checkResults(TestJavaType.Byte);
    }

    private void checkSetNull()
        throws Exception
    {
        checkSet(TestJavaType.String, TestSqlType.all, null);
        checkResults(TestJavaType.String);
    }

    private void checkSetString()
        throws Exception
    {
        checkSet(TestJavaType.String, TestSqlType.all, "0");
        checkResults(TestJavaType.String);
        checkSet(TestJavaType.String, TestSqlType.all, "1");
        checkResults(TestJavaType.String);

        // SetXxx should throw exception for numbers and booleans when
        // string cannot be converted
        checkSet(TestJavaType.String, TestSqlType.all, "string");
        checkResults(TestJavaType.String);
    }

    protected int checkResults(ResultSet resultSet, TestJavaType javaType)
        throws SQLException
    {
        final int columnCount = resultSet.getMetaData().getColumnCount();
        assert columnCount == (TestSqlType.all.length + 1);
        int rows = 0;
        while (resultSet.next()) {
            rows++;
            for (int k = 0; k < TestSqlType.all.length; k++) {
                // TestSqlType#2 (Tinyint) is held in column #2 (1-based).
                final TestSqlType sqlType = TestSqlType.all[k];
                final Object actual = resultSet.getObject(sqlType.ordinal);
                Object value = values[sqlType.ordinal];
                if (value == null) {
                    // value was not valid for this column -- so we don't
                    // expect a result
                    continue;
                }
                final Object expected = sqlType.getExpected(value);
                String message =
                    "sqltype [" + sqlType.string + "], javatype ["
                    + ((javaType == null) ? "?" : javaType.name)
                    + "], expected [" + toString(expected)
                    + ((expected == null) ? "" : (" :" + expected.getClass()))
                    + "], actual [" + toString(actual)
                    + ((actual == null) ? "" : (" :" + actual.getClass()))
                    + "]";
                assertEquals(message, expected, actual);
            }
        }
        return rows;
    }

    protected void checkResults(TestJavaType javaType)
        throws SQLException
    {
        int res = preparedStmt.executeUpdate();
        assertEquals(1, res);

        // Select the results back, to make sure the values we expected got
        // written.
        final Statement stmt = connection.createStatement();
        final ResultSet resultSet =
            stmt.executeQuery("select * from datatypes_schema.dataTypes_table");
        int rows = checkResults(resultSet, javaType);
        assertEquals(res, rows);
        stmt.close();
        connection.rollback();

        // wipe out the array for the next test
        Arrays.fill(values, null);
    }

    /**
     * Overrides {@link Assert#assertEquals(String,Object,Object)} to handle
     * byte arrays correctly.
     */
    public static void assertEquals(
        String message,
        Object expected,
        Object actual)
    {
        if ((expected instanceof byte []) && (actual instanceof byte [])
            && Arrays.equals((byte []) expected, (byte []) actual)) {
            return;
        }
        Assert.assertEquals(message, expected, actual);
    }

    private String toString(Object o)
    {
        if (o instanceof byte []) {
            StringBuffer buf = new StringBuffer("{");
            byte [] bytes = (byte []) o;
            for (int i = 0; i < bytes.length; i++) {
                if (i > 0) {
                    buf.append(", ");
                }
                byte b = bytes[i];
                buf.append(Integer.toHexString(b));
            }
            buf.append("}");
            return buf.toString();
        }
        return String.valueOf(o);
    }

    /** Returns string representation of thread info. */
    protected String toStringThreadInfo(Thread thread)
    {
        if (thread == null) {
            thread = Thread.currentThread();
        }
        StringBuffer buf = new StringBuffer();
        buf.append("thread[");
        buf.append(thread.isInterrupted()? "INT":"!int");
        buf.append(",").append(thread.getId());
        buf.append(",").append(thread.getName());
        buf.append("]");
        return buf.toString();
    }

    private void checkSet(
        TestJavaType javaType,
        TestSqlType [] types,
        Object value)
        throws Exception
    {
        for (int i = 0; i < types.length; i++) {
            TestSqlType type = types[i];
            checkSet(javaType, type, value);
        }
    }

    private boolean checkThrowable(String error,
        String expectedPattern,
        Throwable throwable)
    {
        Pattern expectedException = Pattern.compile(expectedPattern);

        boolean okay = false;
        if (throwable instanceof SQLException) {
            String errorString = throwable.toString();
            if (expectedException.matcher(errorString).matches()) {
                okay = true;
            }
        }

        if (!okay) {
            fail("Was expecting " + error + " error, throwable=" + throwable);
        }
        return okay;
    }

    private void checkSet(
        TestJavaType javaType,
        TestSqlType sqlType,
        Object value)
        throws Exception
    {
        int column = sqlType.ordinal;
        int validity = sqlType.checkIsValid(value);
        Throwable throwable;
        tracer.fine(
            "Call PreparedStmt.set" + javaType.name + "(" + column
            + ", " + value + "), validity is "
            + TestSqlType.validityName[validity]);
        try {
            javaType.setMethod.invoke(
                preparedStmt,
                new Object[] { new Integer(column), value });
            throwable = null;
        } catch (IllegalAccessException e) {
            throwable = e;
        } catch (IllegalArgumentException e) {
            throwable = e;
        } catch (InvocationTargetException e) {
            throwable = e.getCause();
        }

        Pattern expectedException = TestSqlType.exceptionPatterns[validity];
        if (expectedException == null) {
            assert (validity == TestSqlType.VALID);
            if (throwable != null) {
                fail(
                    "Error received when none expected, javaType="
                    + javaType.name + ", sqlType=" + sqlType.string
                    + ", value=" + value + ", throwable=" + throwable);
            }
            this.values[column] = value;
        } else {
            boolean okay = false;
            if (throwable instanceof SQLException) {
                String errorString = throwable.toString();
                if (expectedException.matcher(errorString).matches()) {
                    okay = true;
                }
            }
            if (!okay) {
                fail(
                    "Was expecting " + TestSqlType.validityName[validity]
                    + " error, javaType=" + javaType.name
                    + ", sqlType=" + sqlType.string + ", value=" + value
                    + ", throwable=" + throwable);
            }
        }
    }

    private void checkSetInvalidIndex(
        int column)
        throws Exception
    {
        Throwable throwable;
        try {
            preparedStmt.setString(column, null);
            throwable = null;
        } catch (SQLException e) {
            throwable = e;
        }

        checkThrowable("invalid column",
            ".*parameter index .* is out of bounds.*",
            throwable);
    }

    public void insertDataTypes()
        throws Exception
    {
        // Test insert (without dynamic parameters)
        final String ins_query =
            "insert into datatypes_schema.dataTypes_table values ";

        Statement statement = connection.createStatement();

        List numCharTypes = Arrays.asList(TestSqlType.typesNumericAndChars);
        List binTypes = Arrays.asList(TestSqlType.typesBinary);
        List approxCharTypes = new ArrayList();
        approxCharTypes.add(TestSqlType.Real);
        approxCharTypes.add(TestSqlType.Float);
        approxCharTypes.add(TestSqlType.Double);
        approxCharTypes.add(TestSqlType.Char);
        approxCharTypes.add(TestSqlType.Varchar);

        String hexBytes = ConversionUtil.toStringFromByteArray(bytes, 16);
        for (int i = 0; i <= 19; i++) {
            String columnValues = String.valueOf(i + 100);
            switch (i) {
            case 0:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];

                    // NOTE: conversion between varchars/binary is not
                    // permitted in SQL, but allowed in JDBC
                    if (sqlType.ordinal == BOOLEAN) {
                        columnValues += ", cast('false' as boolean)";
                    } else if (numCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast('" + stringValue + "' as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 1:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + minByte + " as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 2:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + maxByte + " as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 3:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        if (sqlType.checkIsValid(
                                Short.valueOf(minShort),
                                true)
                            == TestSqlType.VALID) {
                            columnValues +=
                                ", cast(" + minShort + " as "
                                + sqlType.string + ")";
                        } else {
                            columnValues += ", null";
                        }
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 4:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        if (sqlType.checkIsValid(
                                Short.valueOf(maxShort),
                                true)
                            == TestSqlType.VALID) {
                            columnValues +=
                                ", cast(" + maxShort + " as "
                                + sqlType.string + ")";
                        } else {
                            columnValues += ", null";
                        }
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 5:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        if (sqlType.checkIsValid(
                                Integer.valueOf(minInt),
                                true)
                            == TestSqlType.VALID) {
                            columnValues +=
                                ", cast(" + minInt + " as "
                                + sqlType.string + ")";
                        } else {
                            columnValues += ", null";
                        }
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 6:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        if (sqlType.checkIsValid(
                                Integer.valueOf(maxInt),
                                true)
                            == TestSqlType.VALID) {
                            columnValues +=
                                ", cast(" + maxInt + " as "
                                + sqlType.string + ")";
                        } else {
                            columnValues += ", null";
                        }
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 7:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        if (sqlType.checkIsValid(
                                Long.valueOf(minLong),
                                true)
                            == TestSqlType.VALID) {
                            // TODO: Fix to be literal minLong when minLong
                            // is accepted as literal in farrago
                            columnValues +=
                                ", cast('" + minLong + "' as "
                                + sqlType.string + ")";
                        } else {
                            columnValues += ", null";
                        }
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 8:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        if (sqlType.checkIsValid(
                                Long.valueOf(maxLong),
                                true)
                            == TestSqlType.VALID) {
                            columnValues +=
                                ", cast(" + maxLong + " as "
                                + sqlType.string + ")";
                        } else {
                            columnValues += ", null";
                        }
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 9:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (approxCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + minFloat + " as "
                            + sqlType.string + ")";
                    } else if (numCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + floatValue1 + " as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 10:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (approxCharTypes.contains(sqlType)) {
                        //columnValues += ", " + maxFloat;
                        columnValues +=
                            ", cast(3.4028234E38 as "
                            + sqlType.string + ")";
                    } else if (numCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + floatValue2 + " as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 11:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (approxCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + minDouble + " as "
                            + sqlType.string + ")";
                    } else if (numCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + doubleValue1 + " as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 12:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (approxCharTypes.contains(sqlType)
                        && (sqlType.ordinal != REAL)) {
                        columnValues +=
                            ", cast(" + maxDouble + " as "
                            + sqlType.string + ")";
                    } else if (numCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + doubleValue2 + " as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 13:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        columnValues += ", cast(1 as "
                            + sqlType.string + ")";
                    } else if (sqlType.ordinal == BOOLEAN) {
                        columnValues +=
                            ", cast(" + boolValue + " as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 14:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    if (numCharTypes.contains(sqlType)) {
                        columnValues +=
                            ", cast(" + bigDecimalValue + " as "
                            + sqlType.string + ")";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 15:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];

                    // TODO: Enable for BINARY once conversion from VARBINARY
                    //       to BINARY is supported
                    //if (binTypes.contains(sqlType)) {
                    if (sqlType.ordinal == VARBINARY) {
                        columnValues += ", x'" + hexBytes + "'";
                    } else {
                        columnValues += ", null";
                    }
                }
                break;
            case 16:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    switch (sqlType.ordinal) {
                    case CHAR:
                    case VARCHAR:
                    case TIMESTAMP:
                        columnValues +=
                            ", cast(DATE '" + dateStr
                            + "' as " + sqlType.string + ")";
                        break;
                    case DATE:
                        columnValues += ", DATE '" + dateStr + "'";
                        break;
                    default:
                        columnValues += ", null";
                        break;
                    }
                }
                break;
            case 17:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    switch (sqlType.ordinal) {
                    case CHAR:
                    case VARCHAR:
                    case TIMESTAMP:
                        columnValues +=
                            ", cast(TIME '" + timeStr
                            + "' as " + sqlType.string + ")";
                        break;
                    case TIME:
                        columnValues += ", TIME '" + timeStr + "'";
                        break;
                    default:
                        columnValues += ", null";
                        break;
                    }
                }
                break;
            case 18:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    TestSqlType sqlType = TestSqlType.all[j];
                    switch (sqlType.ordinal) {
                    case CHAR:
                    case VARCHAR:
                    case DATE:
                    case TIME:
                        columnValues +=
                            ", cast(TIMESTAMP '" + timestampStr
                            + "' as " + sqlType.string + ")";
                        break;
                    case TIMESTAMP:
                        columnValues += ", TIMESTAMP '" + timestampStr + "'";
                        break;
                    default:
                        columnValues += ", null";
                        break;
                    }
                }
                break;
            case 19:
                for (int j = 0; j < TestSqlType.all.length; j++) {
                    columnValues += ", ";
                    TestSqlType sqlType = TestSqlType.all[j];
                    switch (sqlType.ordinal) {
                    case TINYINT:
                        columnValues += tinyIntObj;
                        break;
                    case SMALLINT:
                        columnValues += smallIntObj;
                        break;
                    case INTEGER:
                        columnValues += integerObj;
                        break;
                    case BIGINT:
                        columnValues += bigIntObj;
                        break;
                    case REAL:
                        columnValues += "3.4028234E38";

                        //columnValues += floatObj;
                        break;
                    case FLOAT:
                    case DOUBLE:
                        columnValues += doubleObj;
                        break;
                    case BOOLEAN:
                        columnValues += boolObj;
                        break;
                    case CHAR:
                        columnValues += "'" + charObj + "'";
                        break;
                    case VARCHAR:
                        columnValues += "'" + varcharObj + "'";
                        break;
                    case BINARY:

                        // TODO: Use hexBytes for BINARY once conversion from
                        // VARBINARY       is supported.
                        columnValues += "null";
                        break;
                    case VARBINARY:
                        columnValues += "x'" + hexBytes + "'";
                        break;
                    case DATE:
                        columnValues += "DATE '" + dateStr + "'";
                        break;
                    case TIME:
                        columnValues += "TIME '" + timeStr + "'";
                        break;
                    case TIMESTAMP:
                        columnValues += "TIMESTAMP '" + timestampStr + "'";
                        break;
                    default:
                        columnValues += "null";
                        break;
                    }
                }
                break;

            default:
                assert false;
                break;
            }

            int res =
                statement.executeUpdate(ins_query + "(" + columnValues + ")");
            assertEquals(1, res);
        }
        statement.close();
    }

    public void testDataTypes()
        throws Exception
    {
        insertDataTypes();

        // Test select
        String query =
            "select " + columnStr + " from datatypes_schema.datatypes_table";

        preparedStmt = connection.prepareStatement(query);

        resultSet = preparedStmt.executeQuery();
        int id;
        while (resultSet.next()) {
            id = resultSet.getInt(1);
            switch (id) {
            case 100:
                assertEquals(
                    stringValue,
                    resultSet.getString(TINYINT));
                assertEquals(
                    stringValue,
                    resultSet.getString(SMALLINT));
                assertEquals(
                    stringValue,
                    resultSet.getString(INTEGER));
                assertEquals(
                    stringValue,
                    resultSet.getString(BIGINT));
                assertEquals(
                    

                    /*stringValue,*/
                "0.0",
                    resultSet.getString(REAL));
                assertEquals(
                    

                    /*stringValue,*/
                "0.0",
                    resultSet.getString(FLOAT));
                assertEquals(
                    

                    /*stringValue,*/
                "0.0",
                    resultSet.getString(DOUBLE));
                assertEquals(
                    stringValue,
                    resultSet.getString(DECIMAL));
                assertEquals(
                    "0.000",
                    resultSet.getString(DECIMAL73));
                assertEquals(
                    

                    /*stringValue,*/
                "false",
                    resultSet.getString(BOOLEAN));

                // Check CHAR - result String can be longer than the input
                // string Just check the first part
                assertEquals(
                    stringValue,
                    resultSet.getString(CHAR).substring(
                        0,
                        stringValue.length()));
                assertEquals(
                    stringValue,
                    resultSet.getString(VARCHAR));

                // What should BINARY/VARBINARY be?
                if (todo) {
                    assertEquals(
                        stringValue,
                        resultSet.getString(BINARY));
                    assertEquals(
                        stringValue,
                        resultSet.getString(VARBINARY));
                }

                /*assertEquals(
                    stringValue, resultSet.getString(DATE)); assertEquals(
                 stringValue, resultSet.getString(TIME)); assertEquals(
                 stringValue, resultSet.getString(TIMESTAMP));
                 */
                break;
            case 101:
                assertEquals(
                    minByte,
                    resultSet.getByte(TINYINT));
                assertEquals(
                    minByte,
                    resultSet.getByte(SMALLINT));
                assertEquals(
                    minByte,
                    resultSet.getByte(INTEGER));
                assertEquals(
                    minByte,
                    resultSet.getByte(BIGINT));
                assertEquals(
                    minByte,
                    resultSet.getByte(REAL));
                assertEquals(
                    minByte,
                    resultSet.getByte(FLOAT));
                assertEquals(
                    minByte,
                    resultSet.getByte(DOUBLE));
                assertEquals(
                    minByte,
                    resultSet.getByte(DECIMAL));
                assertEquals(
                    minByte,
                    resultSet.getByte(DECIMAL73));
                assertEquals(
                    0,
                    resultSet.getByte(BOOLEAN));
                assertEquals(
                    minByte,
                    resultSet.getByte(CHAR));
                assertEquals(
                    minByte,
                    resultSet.getByte(VARCHAR));
                break;
            case 102:
                assertEquals(
                    maxByte,
                    resultSet.getByte(TINYINT));
                assertEquals(
                    maxByte,
                    resultSet.getByte(SMALLINT));
                assertEquals(
                    maxByte,
                    resultSet.getByte(INTEGER));
                assertEquals(
                    maxByte,
                    resultSet.getByte(BIGINT));
                assertEquals(
                    maxByte,
                    resultSet.getByte(REAL));
                assertEquals(
                    maxByte,
                    resultSet.getByte(FLOAT));
                assertEquals(
                    maxByte,
                    resultSet.getByte(DOUBLE));
                assertEquals(
                    maxByte,
                    resultSet.getByte(DECIMAL));
                assertEquals(
                    maxByte,
                    resultSet.getByte(DECIMAL73));
                assertEquals(
                    0,
                    resultSet.getByte(BOOLEAN));
                assertEquals(
                    maxByte,
                    resultSet.getByte(CHAR));
                assertEquals(
                    maxByte,
                    resultSet.getByte(VARCHAR));
                break;
            case 103:
                assertEquals(
                    0,
                    resultSet.getShort(TINYINT));
                assertEquals(
                    minShort,
                    resultSet.getShort(SMALLINT));
                assertEquals(
                    minShort,
                    resultSet.getShort(INTEGER));
                assertEquals(
                    minShort,
                    resultSet.getShort(BIGINT));
                assertEquals(
                    minShort,
                    resultSet.getShort(REAL));
                assertEquals(
                    minShort,
                    resultSet.getShort(FLOAT));
                assertEquals(
                    minShort,
                    resultSet.getShort(DOUBLE));
                assertEquals(
                    minShort,
                    resultSet.getShort(DECIMAL));
                assertEquals(
                    0,
                    resultSet.getShort(DECIMAL73));
                assertEquals(
                    0,
                    resultSet.getShort(BOOLEAN));
                assertEquals(
                    minShort,
                    resultSet.getShort(CHAR));
                assertEquals(
                    minShort,
                    resultSet.getShort(VARCHAR));
                break;
            case 104:
                assertEquals(
                    0, /* null, not -1*/
                    resultSet.getShort(TINYINT));
                assertEquals(
                    maxShort,
                    resultSet.getShort(SMALLINT));
                assertEquals(
                    maxShort,
                    resultSet.getShort(INTEGER));
                assertEquals(
                    maxShort,
                    resultSet.getShort(BIGINT));
                assertEquals(
                    maxShort,
                    resultSet.getShort(REAL));
                assertEquals(
                    maxShort,
                    resultSet.getShort(FLOAT));
                assertEquals(
                    maxShort,
                    resultSet.getShort(DOUBLE));
                assertEquals(
                    maxShort,
                    resultSet.getShort(DECIMAL));
                assertEquals(
                    0,
                    resultSet.getShort(DECIMAL73));
                assertEquals(
                    0,
                    resultSet.getShort(BOOLEAN));
                assertEquals(
                    maxShort,
                    resultSet.getShort(CHAR));
                assertEquals(
                    maxShort,
                    resultSet.getShort(VARCHAR));
                break;
            case 105:
                assertEquals(
                    0,
                    resultSet.getInt(TINYINT));
                assertEquals(
                    0,
                    resultSet.getInt(SMALLINT));
                assertEquals(
                    minInt,
                    resultSet.getInt(INTEGER));
                assertEquals(
                    minInt,
                    resultSet.getInt(BIGINT));
                assertEquals(
                    minInt,
                    resultSet.getInt(REAL));
                assertEquals(
                    minInt,
                    resultSet.getInt(FLOAT));
                assertEquals(
                    minInt,
                    resultSet.getInt(DOUBLE));
                assertEquals(
                    minInt,
                    resultSet.getInt(DECIMAL));
                assertEquals(
                    0,
                    resultSet.getInt(DECIMAL73));
                assertEquals(
                    0,
                    resultSet.getInt(BOOLEAN));
                assertEquals(
                    minInt,
                    resultSet.getInt(CHAR));
                assertEquals(
                    minInt,
                    resultSet.getInt(VARCHAR));
                break;
            case 106:
                assertEquals(
                    0, /* null, not -1 */
                    resultSet.getInt(TINYINT));
                assertEquals(
                    0, /* null, not -1 */
                    resultSet.getInt(SMALLINT));
                assertEquals(
                    maxInt,
                    resultSet.getInt(INTEGER));
                assertEquals(
                    maxInt,
                    resultSet.getInt(BIGINT));
                assertEquals(
                    -2147483648,
                    resultSet.getInt(REAL));
                assertEquals(
                    maxInt,
                    resultSet.getInt(FLOAT));
                assertEquals(
                    maxInt,
                    resultSet.getInt(DOUBLE));
                assertEquals(
                    maxInt,
                    resultSet.getInt(DECIMAL));
                assertEquals(
                    0,
                    resultSet.getInt(DECIMAL73));
                assertEquals(
                    0,
                    resultSet.getInt(BOOLEAN));
                assertEquals(
                    maxInt,
                    resultSet.getInt(CHAR));
                assertEquals(
                    maxInt,
                    resultSet.getInt(VARCHAR));
                break;
            case 107:
                assertEquals(
                    0,
                    resultSet.getLong(TINYINT));
                assertEquals(
                    0,
                    resultSet.getLong(SMALLINT));
                assertEquals(
                    0,
                    resultSet.getLong(INTEGER));
                assertEquals(
                    minLong,
                    resultSet.getLong(BIGINT));
                assertEquals(
                    minLong,
                    resultSet.getLong(REAL));
                assertEquals(
                    minLong,
                    resultSet.getLong(FLOAT));
                assertEquals(
                    minLong,
                    resultSet.getLong(DOUBLE));
                assertEquals(
                    minLong,
                    resultSet.getLong(DECIMAL));
                assertEquals(
                    0,
                    resultSet.getLong(DECIMAL73));
                assertEquals(
                    0,
                    resultSet.getLong(BOOLEAN));
                assertEquals(
                    minLong,
                    resultSet.getLong(CHAR));
                assertEquals(
                    minLong,
                    resultSet.getLong(VARCHAR));
                break;
            case 108:
                assertEquals(
                    0, /* null, not -1 */
                    resultSet.getLong(TINYINT));
                assertEquals(
                    0, /* null, not -1 */
                    resultSet.getLong(SMALLINT));
                assertEquals(
                    0, /* null, not -1 */
                    resultSet.getLong(INTEGER));
                assertEquals(
                    maxLong,
                    resultSet.getLong(BIGINT));
                assertEquals(
                    maxLong,
                    resultSet.getLong(REAL));
                assertEquals(
                    maxLong,
                    resultSet.getLong(FLOAT));
                assertEquals(
                    maxLong,
                    resultSet.getLong(DOUBLE));
                assertEquals(
                    maxLong,
                    resultSet.getLong(DECIMAL));
                assertEquals(
                    0,
                    resultSet.getLong(DECIMAL73));
                assertEquals(
                    0,
                    resultSet.getLong(BOOLEAN));
                assertEquals(
                    maxLong,
                    resultSet.getLong(CHAR));
                assertEquals(
                    maxLong,
                    resultSet.getLong(VARCHAR));
                break;
            case 109:
                float expectedFloat1 = -2.0f;
                assertEquals(
                    expectedFloat1,
                    resultSet.getFloat(TINYINT),
                    0);
                assertEquals(
                    expectedFloat1,
                    resultSet.getFloat(SMALLINT),
                    0);
                assertEquals(
                    expectedFloat1,
                    resultSet.getFloat(INTEGER),
                    0);
                assertEquals(
                    expectedFloat1,
                    resultSet.getFloat(BIGINT),
                    0);
                assertEquals(
                    minFloat,
                    resultSet.getFloat(REAL),
                    0);
                assertEquals(
                    minFloat,
                    resultSet.getFloat(FLOAT),
                    0);
                assertEquals(
                    minFloat,
                    resultSet.getFloat(DOUBLE),
                    0);
                assertEquals(
                    expectedFloat1,
                    resultSet.getFloat(DECIMAL),
                    0);
                assertEquals(
                    floatValue1,
                    resultSet.getFloat(DECIMAL73),
                    0.001);
                assertEquals(
                    0,
                    resultSet.getFloat(BOOLEAN),
                    0);
                assertEquals(
                    minFloat,
                    resultSet.getFloat(CHAR),
                    0);
                assertEquals(
                    minFloat,
                    resultSet.getFloat(VARCHAR),
                    0);
                break;
            case 110:
                float expectedFloat2 = 2.0f;
                assertEquals(
                    expectedFloat2,
                    resultSet.getFloat(TINYINT),
                    0);
                assertEquals(
                    expectedFloat2,
                    resultSet.getFloat(SMALLINT),
                    0);
                assertEquals(
                    expectedFloat2,
                    resultSet.getFloat(INTEGER),
                    0);
                assertEquals(
                    expectedFloat2,
                    resultSet.getFloat(BIGINT),
                    0);
                assertEquals(
                    maxFloat,
                    resultSet.getFloat(REAL),
                    0);
                assertEquals(
                    maxFloat,
                    resultSet.getFloat(FLOAT),
                    0);
                assertEquals(
                    maxFloat,
                    resultSet.getFloat(DOUBLE),
                    0);
                assertEquals(
                    expectedFloat2,
                    resultSet.getFloat(DECIMAL),
                    0);
                assertEquals(
                    floatValue2,
                    resultSet.getFloat(DECIMAL73),
                    0.001);
                assertEquals(
                    0,
                    resultSet.getFloat(BOOLEAN),
                    0);
                assertEquals(
                    maxFloat,
                    resultSet.getFloat(CHAR),
                    0);
                assertEquals(
                    maxFloat,
                    resultSet.getFloat(VARCHAR),
                    0);
                break;
            case 111:
                double expectedDouble1 = -2;
                assertEquals(
                    expectedDouble1,
                    resultSet.getDouble(TINYINT),
                    0);
                assertEquals(
                    expectedDouble1,
                    resultSet.getDouble(SMALLINT),
                    0);
                assertEquals(
                    expectedDouble1,
                    resultSet.getDouble(INTEGER),
                    0);
                assertEquals(
                    expectedDouble1,
                    resultSet.getDouble(BIGINT),
                    0);
                assertEquals(
                    0,
                    resultSet.getDouble(REAL),
                    0);
                assertEquals(
                    minDouble,
                    resultSet.getDouble(FLOAT),
                    0);
                assertEquals(
                    minDouble,
                    resultSet.getDouble(DOUBLE),
                    0);
                assertEquals(
                    expectedDouble1,
                    resultSet.getDouble(DECIMAL),
                    0);
                assertEquals(
                    doubleValue1,
                    resultSet.getDouble(DECIMAL73),
                    0.001);
                assertEquals(
                    0,
                    resultSet.getDouble(BOOLEAN),
                    0);
                assertEquals(
                    minDouble,
                    resultSet.getDouble(CHAR),
                    0);
                assertEquals(
                    minDouble,
                    resultSet.getDouble(VARCHAR),
                    0);
                break;
            case 112:
                double expectedDouble2 = 2;
                assertEquals(
                    expectedDouble2,
                    resultSet.getDouble(TINYINT),
                    0);
                assertEquals(
                    expectedDouble2,
                    resultSet.getDouble(SMALLINT),
                    0);
                assertEquals(
                    expectedDouble2,
                    resultSet.getDouble(INTEGER),
                    0);
                assertEquals(
                    expectedDouble2,
                    resultSet.getDouble(BIGINT),
                    0);
                assertEquals(
                    doubleValue2,
                    resultSet.getDouble(REAL),
                    0.000001);
                assertEquals(
                    maxDouble,
                    resultSet.getDouble(FLOAT),
                    0);
                assertEquals(
                    maxDouble,
                    resultSet.getDouble(DOUBLE),
                    0);
                assertEquals(
                    expectedDouble2,
                    resultSet.getDouble(DECIMAL),
                    0);
                assertEquals(
                    doubleValue2,
                    resultSet.getDouble(DECIMAL73),
                    0.001);
                assertEquals(
                    0,
                    resultSet.getDouble(BOOLEAN),
                    0);
                assertEquals(
                    maxDouble,
                    resultSet.getDouble(CHAR),
                    0);
                assertEquals(
                    maxDouble,
                    resultSet.getDouble(VARCHAR),
                    0);
                break;
            case 113:
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(TINYINT));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(SMALLINT));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(INTEGER));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(BIGINT));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(REAL));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(FLOAT));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(DOUBLE));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(DECIMAL));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(DECIMAL73));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(BOOLEAN));

                assertEquals(
                    boolValue,
                    resultSet.getBoolean(CHAR));
                assertEquals(
                    boolValue,
                    resultSet.getBoolean(VARCHAR));
                break;
            case 114:
                BigDecimal expectedDecimal = new BigDecimal(10);
                assertEquals(
                    expectedDecimal,
                    resultSet.getBigDecimal(TINYINT));
                assertEquals(
                    expectedDecimal,
                    resultSet.getBigDecimal(SMALLINT));
                assertEquals(
                    expectedDecimal,
                    resultSet.getBigDecimal(INTEGER));
                assertEquals(
                    expectedDecimal,
                    resultSet.getBigDecimal(BIGINT));
                assertEquals(
                    bigDecimalValue.floatValue(),
                    resultSet.getBigDecimal(REAL).floatValue(),
                    0);
                assertEquals(
                    bigDecimalValue.doubleValue(),
                    resultSet.getBigDecimal(FLOAT).doubleValue(),
                    0);
                assertEquals(
                    bigDecimalValue.doubleValue(),
                    resultSet.getBigDecimal(DOUBLE).doubleValue(),
                    0);
                assertEquals(
                    expectedDecimal,
                    resultSet.getBigDecimal(DECIMAL));
                assertEquals(
                    bigDecimalValue.setScale(3, BigDecimal.ROUND_HALF_UP),
                    resultSet.getBigDecimal(DECIMAL73));
                assertEquals(
                    bigDecimalValue,
                    resultSet.getBigDecimal(CHAR));
                assertEquals(
                    bigDecimalValue,
                    resultSet.getBigDecimal(VARCHAR));
                break;
            case 115:

                // Check BINARY - resBytes can be longer than the input bytes
                byte [] resBytes = resultSet.getBytes(BINARY);

                // TODO: Enable once cast from VARBINARY to BINARY fixed
                if (todo) {
                    assertNotNull(resBytes);
                    for (int i = 0; i < bytes.length; i++) {
                        assertEquals(bytes[i], resBytes[i]);
                    }
                }

                // Check VARBINARY - should be same length
                resBytes = resultSet.getBytes(VARBINARY);

                assertNotNull(resBytes);
                assertEquals(bytes.length, resBytes.length);
                for (int i = 0; i < bytes.length; i++) {
                    assertEquals(bytes[i], resBytes[i]);
                }

                break;
            case 116:
                assertEquals(
                    dateNoTime.getTime(),
                    resultSet.getDate(CHAR).getTime());
                assertEquals(
                    dateNoTime.getTime(),
                    resultSet.getDate(VARCHAR).getTime());
                assertEquals(
                    dateNoTime.getTime(),
                    resultSet.getDate(DATE).getTime());
                assertEquals(
                    dateNoTime.getTime(),
                    resultSet.getDate(TIMESTAMP).getTime());

                break;
            case 117:
                assertEquals(
                    timeNoDate.getTime(),
                    resultSet.getTime(CHAR).getTime());
                assertEquals(
                    timeNoDate.getTime(),
                    resultSet.getTime(VARCHAR).getTime());
                assertEquals(
                    timeNoDate.getTime(),
                    resultSet.getTime(TIME).getTime());

                // FIXME: FNL-54
                // SQL Spec Part 2 Section 4.6.2 Table 3 requires 
                // Time to Timestamp cast to set the date to current_date
                // (currently stored in FarragoRuntimeContext)
                assertEquals(
                    timeNoDate.getTime(),
                    resultSet.getTime(TIMESTAMP).getTime());

                break;
            case 118:
                // TODO: Should these be timestamp with or without precision?
                assertEquals(
                    timestampNoPrec.getTime(),
                    resultSet.getTimestamp(CHAR).getTime());
                assertEquals(
                    timestampNoPrec.getTime(),
                    resultSet.getTimestamp(VARCHAR).getTime());
                assertEquals(
                    dateNoTime.getTime(),
                    resultSet.getTimestamp(DATE).getTime());
                // FIXME: See Time to Timestamp note above
                if (todo) {
                    assertEquals(
                        timestamp.getTime(),
                        resultSet.getTimestamp(TIME).getTime());
                }
                assertEquals(
                    timestamp.getTime(),
                    resultSet.getTimestamp(TIMESTAMP).getTime());
                break;
            case 119:
                assertEquals(
                    tinyIntObj,
                    resultSet.getObject(TINYINT));
                assertEquals(
                    smallIntObj,
                    resultSet.getObject(SMALLINT));
                assertEquals(
                    integerObj,
                    resultSet.getObject(INTEGER));
                assertEquals(
                    bigIntObj,
                    resultSet.getObject(BIGINT));
                assertEquals(
                    floatObj,
                    resultSet.getObject(REAL));
                assertEquals(
                    doubleObj,
                    resultSet.getObject(FLOAT));
                assertEquals(
                    doubleObj,
                    resultSet.getObject(DOUBLE));
                assertEquals(
                    boolObj,
                    resultSet.getObject(BOOLEAN));

                // Check CHAR - result String can be longer than the input
                // string Just check the first part
                assertEquals(
                    charObj,
                    resultSet.getString(CHAR).substring(
                        0,
                        charObj.length()));
                assertEquals(
                    varcharObj,
                    resultSet.getObject(VARCHAR));

                // Check BINARY - resBytes can be longer than the input bytes
                resBytes = (byte []) resultSet.getObject(BINARY);

                // TODO: fix only cast from BINARY to VARBINARY
                if (todo) {
                    assertNotNull(resBytes);
                    for (int i = 0; i < bytes.length; i++) {
                        assertEquals(bytes[i], resBytes[i]);
                    }
                }

                // Check VARBINARY - should be same length
                resBytes = (byte []) resultSet.getObject(VARBINARY);
                assertNotNull(resBytes);
                assertEquals(bytes.length, resBytes.length);
                for (int i = 0; i < bytes.length; i++) {
                    assertEquals(bytes[i], resBytes[i]);
                }

                assertEquals(
                    dateNoTime,
                    resultSet.getObject(DATE));
                assertEquals(
                    timeNoDate,
                    resultSet.getObject(TIME));
                assertEquals(
                    timestamp,
                    resultSet.getObject(TIMESTAMP));
                break;
            default:

                // Unexpected id
                assert false;
                break;
            }
        }

        resultSet.close();
        resultSet = null;

        connection.rollback();
    }

    /**
     * Tests sql Date/Time to java.sql Date/Time translations.
     */
    public void testDateTimeSql()
        throws Exception
    {
        String dateSql =
            "values (DATE '2004-12-21', TIME '12:22:33'," + ""
            + " TIMESTAMP '2004-12-21 12:22:33')";

        preparedStmt = connection.prepareStatement(dateSql);
        resultSet = preparedStmt.executeQuery();

        if (resultSet.next()) {
            Date date = resultSet.getDate(1);
            Timestamp tstamp = resultSet.getTimestamp(3);

            Calendar cal = Calendar.getInstance();
            cal.setTimeZone(TimeZone.getDefault());
            cal.clear();
            cal.set(2004, 11, 21); //month is zero based.  idiots ...
            assertEquals(
                cal.getTime().getTime(),
                date.getTime());

            cal.set(2004, 11, 21, 12, 22, 33);
            assertEquals(
                cal.getTime().getTime(),
                tstamp.getTime());
        } else {
            assert false : "Static query returned no rows?";
        }

        resultSet.close();

        if (false) {
            resultSet = stmt.executeQuery(dateSql);

            while (resultSet.next()) {
                Calendar cal = Calendar.getInstance();
                cal.setTimeZone(TimeZone.getTimeZone("GMT-6"));

                //  not supported by IteratorResultSet yet.
                Date date = resultSet.getDate(1, sydneyCal);
                Timestamp tstamp = resultSet.getTimestamp(3, sydneyCal);

                cal.setTimeZone(TimeZone.getTimeZone("GMT-8"));
                cal.clear();
                cal.set(2004, 11, 21); //month is zero based.  idiots ...
                assertEquals(
                    cal.getTime().getTime(),
                    date.getTime());

                cal.set(2004, 11, 21, 12, 22, 33);
                assertEquals(
                    cal.getTime().getTime(),
                    tstamp.getTime());
            }
        }
    }

    /**
     * Tests setQueryTimeout.
     *
     * @throws Exception .
     */
    public void testTimeout()
        throws Exception
    {
        String sql = "select * from sales.emps order by name";
        preparedStmt = connection.prepareStatement(sql);
        for (int i = 10; i >= -2; i--) {
            preparedStmt.setQueryTimeout(i);
            resultSet = preparedStmt.executeQuery();

            compareResultLists(
                Arrays.asList("110", "100", "110", "120"),
                Arrays.asList("Eric", "Fred", "John", "Wilma"),
                Arrays.asList("20", "10", "40", "20"),
                Arrays.asList("M", null, "M", "F"),
                Arrays.asList("San Francisco", null, "Vancouver", null));
        }

        sql = "select empid from sales.emps where name=?";
        preparedStmt = connection.prepareStatement(sql);
        ParameterMetaData pmd = preparedStmt.getParameterMetaData();
        assertEquals(
            1,
            pmd.getParameterCount());
        assertEquals(
            128,
            pmd.getPrecision(1));
        assertEquals(
            Types.VARCHAR,
            pmd.getParameterType(1));
        assertEquals(
            "VARCHAR",
            pmd.getParameterTypeName(1));

        preparedStmt.setString(1, "Wilma");
        preparedStmt.setQueryTimeout(5);
        resultSet = preparedStmt.executeQuery();
        compareResultSet(Collections.singleton("1"));
        preparedStmt.setString(1, "Eric");
        preparedStmt.setQueryTimeout(3);
        resultSet = preparedStmt.executeQuery();
        compareResultSet(Collections.singleton("3"));
    }

    /**
     * Tests char and varchar Data Type in JDBC.
     *
     * @throws Exception .
     */
    public void testChar()
        throws Exception
    {
        String name = "JDBC Test Char";

        preparedStmt =
            connection.prepareStatement(
                "insert into \"SALES\".\"EMPS\" values "
                + "(?, ?, 10, ?, ?, ?, 28, NULL, NULL, false)");

        preparedStmt.setByte(3, (byte) 1);
        preparedStmt.setByte(4, (byte) -128);
        doEmpInsert(name, 1);

        preparedStmt.setShort(3, (short) 2);
        preparedStmt.setShort(4, (short) 32767);
        doEmpInsert(name, 2);

        preparedStmt.setInt(3, 3);
        preparedStmt.setInt(4, -234234);
        doEmpInsert(name, 3);

        preparedStmt.setLong(3, 4L);
        preparedStmt.setLong(4, 123432432432545455L);
        doEmpInsert(name, 4);

        Throwable throwable;
        try {
            preparedStmt.setFloat(3, 5.0F);
            preparedStmt.setFloat(4, 6.02e+23F);
            doEmpInsert(name, 5);
            throwable = null;
            fail("Expected an error");
        } catch (SQLException e) {
            throwable = e;
        }
        assertExceptionMatches(throwable,
            ".*Value '5.0' is too long for parameter of type CHAR.1.");

        try {
            preparedStmt.setDouble(3, 6.2);
            preparedStmt.setDouble(4, 3.14);
            doEmpInsert(name, 6);
        } catch (SQLException e) {
            throwable = e;
        }
        assertExceptionMatches(throwable,
            ".*Value '6.2' is too long for parameter of type CHAR.1.");

        preparedStmt.setBigDecimal(
            3,
            new BigDecimal(2.0));
        preparedStmt.setBigDecimal(
            4,
            new BigDecimal(88.23432432));
        doEmpInsert(name, 7);

        try {
            preparedStmt.setBoolean(3, false);
            preparedStmt.setBoolean(4, true);
            doEmpInsert(name, 8);
        } catch (SQLException e) {
            throwable = e;
        }
        assertExceptionMatches(throwable,
            ".*Value 'false' is too long for parameter of type CHAR.1.");

        preparedStmt.setString(3, "x");
        preparedStmt.setBoolean(4, true);
        doEmpInsert(name, 8);

        preparedStmt.setString(3, "F");
        preparedStmt.setString(4, "San Jose");
        doEmpInsert(name, 9);

        preparedStmt.setObject(
            3,
            new Character('M'));
        preparedStmt.setObject(
            4,
            new StringBuffer("New York"));
        doEmpInsert(name, 10);

        // only query what we insert above
        String query;
        query = "select gender, city, empid from sales.emps where name like '";
        query += name + "%'";

        preparedStmt = connection.prepareStatement(query);

        resultSet = preparedStmt.executeQuery();
        int empid;
        while (resultSet.next()) {
            empid = resultSet.getInt(3);
            switch (empid) {
            case 101:
                assertEquals(
                    1,
                    resultSet.getByte(1));
                assertEquals(
                    -128,
                    resultSet.getByte(2));
                break;
            case 102:
                assertEquals(
                    2,
                    resultSet.getShort(1));
                assertEquals(
                    32767,
                    resultSet.getShort(2));
                break;
            case 103:
                assertEquals(
                    3,
                    resultSet.getInt(1));
                assertEquals(
                    -234234,
                    resultSet.getInt(2));
                break;
            case 104:
                assertEquals(
                    4L,
                    resultSet.getLong(1));
                assertEquals(
                    123432432432545455L,
                    resultSet.getLong(2));
                break;
            case 105:
                assertEquals(
                    5.0F,
                    resultSet.getFloat(1),
                    0.0001);
                assertEquals(
                    6.02e+23F,
                    resultSet.getFloat(2),
                    0.0001);
                break;
            case 106:
                assertEquals(
                    6.2,
                    resultSet.getDouble(1),
                    0.0001);
                assertEquals(
                    3.14,
                    resultSet.getDouble(2),
                    0.0001);
                break;
            case 107:
                assertEquals(
                    new BigDecimal(2.0),
                    resultSet.getBigDecimal(1));
                assertEquals(
                    new BigDecimal(88.23432432),
                    resultSet.getBigDecimal(2));
                break;
            case 108:
                assertEquals(
                    "x",
                    resultSet.getString(1));
                assertEquals(
                    true,
                    resultSet.getBoolean(2));
                break;
            case 109:
                assertEquals(
                    "F",
                    resultSet.getString(1));
                assertEquals(
                    "San Jose",
                    resultSet.getString(2));
                break;
            case 110:

                // Okay, since the underlying types are VARCHAR and CHAR,
                // getObject should return Strings
                assertEquals(
                    "M",
                    resultSet.getObject(1));
                assertEquals(
                    "New York",
                    resultSet.getObject(2));
                break;
            default:
                assertEquals(1, 2);
                break;
            }
        }

        resultSet.close();
        resultSet = null;
    }

    public static void assertExceptionMatches(
        Throwable e,
        String expectedPattern)
    {
        if (e == null) {
            fail(
                "Expected an error which matches pattern '" + expectedPattern
                + "'");
        }
        String msg = e.toString();

        // NOTE jvs 4-June-2006:  Let regex dot match newlines
        // since sometimes error stacks include them.
        Pattern pattern = Pattern.compile(expectedPattern, Pattern.DOTALL);
        if (!pattern.matcher(msg).matches()) {
            fail(
                "Got a different error '" + msg + "' than expected '"
                + expectedPattern + "'");
        }
    }

    private void doEmpInsert(String name, int i)
        throws SQLException
    {
        preparedStmt.setInt(1, i + 1000);
        preparedStmt.setString(2, name + i);
        preparedStmt.setInt(5, i + 100);
        int res = preparedStmt.executeUpdate();
        assertEquals(1, res);
    }

    /**
     * Tests integer Data Type in JDBC.
     *
     * @throws Exception .
     */
    public void testInteger()
        throws Exception
    {
        short empno = 555;
        byte deptno = 10;
        int empid = -99;
        long age = 28L;

        float empno2 = 666.6F;
        double deptno2 = 10.0;
        BigDecimal empid2 = new BigDecimal(88.20334342);
        boolean age2 = true;

        String empno3 = "777";
        Object deptno3 = new Integer(10);

        String name = "JDBC Test Int";
        String name2 = "JDBC Test Int2";
        String name3 = "JDBC Test Int3";
        int res;

        String query = "insert into \"SALES\".\"EMPS\" values ";
        query += "(?, ?, ?, 'M', 'Oakland', ?, ?, NULL, NULL, false)";

        preparedStmt = connection.prepareStatement(query);

        preparedStmt.setShort(1, empno);
        preparedStmt.setString(2, name);
        preparedStmt.setByte(3, deptno);
        preparedStmt.setInt(4, empid);
        preparedStmt.setLong(5, age);
        res = preparedStmt.executeUpdate();
        assertEquals(1, res);

        preparedStmt.setFloat(1, empno2);
        preparedStmt.setString(2, name2);
        preparedStmt.setDouble(3, deptno2);
        preparedStmt.setBigDecimal(4, empid2);
        preparedStmt.setBoolean(5, age2);
        res = preparedStmt.executeUpdate();
        assertEquals(1, res);

        preparedStmt.setString(1, empno3);
        preparedStmt.setString(2, name3);
        preparedStmt.setObject(3, deptno3);
        preparedStmt.setInt(4, 28);
        preparedStmt.setLong(5, 28);
        res = preparedStmt.executeUpdate();
        assertEquals(1, res);

        query =
            "select empno, deptno, empid, age from sales.emps where name = ?";
        preparedStmt = connection.prepareStatement(query);
        preparedStmt.setString(1, name);

        resultSet = preparedStmt.executeQuery();
        while (resultSet.next()) {
            assertEquals(
                empno,
                resultSet.getShort(1));
            assertEquals(
                deptno,
                resultSet.getByte(2));
            assertEquals(
                empid,
                resultSet.getInt(3));
            assertEquals(
                age,
                resultSet.getLong(4));
        }

        resultSet.close();

        preparedStmt.setString(1, name2);
        resultSet = preparedStmt.executeQuery();
        while (resultSet.next()) {
            assertEquals(
                667, /* 666.6 rounded up */
                resultSet.getInt(1));
            assertEquals(
                667, /* 666.6 rounded up */
                resultSet.getFloat(1),
                0.0001);
            assertEquals(
                deptno2,
                resultSet.getDouble(2),
                0.0001);

            assertEquals(
                BigDecimal.valueOf(empid2.longValue()),
                resultSet.getBigDecimal(3));
            assertEquals(
                age2,
                resultSet.getBoolean(4));
        }

        resultSet.close();

        preparedStmt.setString(1, name3);
        resultSet = preparedStmt.executeQuery();
        while (resultSet.next()) {
            assertEquals(
                empno3,
                resultSet.getString(1));
            assertEquals(
                deptno3,
                resultSet.getObject(2));
        }

        resultSet.close();

        resultSet = null;
    }

    /**
     * Tests re-execution of a prepared query.
     *
     * @throws Exception .
     */
    public void testPreparedQuery()
        throws Exception
    {
        String sql = "select * from sales.emps";
        preparedStmt = connection.prepareStatement(sql);
        for (int i = 0; i < 5; ++i) {
            resultSet = preparedStmt.executeQuery();
            assertEquals(
                4,
                getResultSetCount());
            resultSet.close();
            resultSet = null;
        }
    }

    /**
     * Tests re-execution of an unprepared query. There's no black-box way to
     * verify that caching is working, but if it is, this will at least exercise
     * it.
     *
     * @throws Exception .
     */
    public void testCachedQuery()
        throws Exception
    {
        repeatQuery(false);
    }

    /**
     * Tests re-execution of an unprepared query with statement caching
     * disabled.
     *
     * @throws Exception .
     */
    public void testUncachedQuery()
        throws Exception
    {
        // disable caching
        repeatQuery(true);
    }

    private void repeatQuery(boolean flushCache)
        throws Exception
    {
        String sql = "select * from sales.emps";
        for (int i = 0; i < 3; ++i) {
            resultSet = stmt.executeQuery(sql);
            assertEquals(
                4,
                getResultSetCount());
            resultSet.close();
            resultSet = null;
            if (flushCache) {
                stmt.execute("call sys_boot.mgmt.flush_code_cache()");
            }
        }
    }

    /**
     * Tests retrieval of ResultSetMetaData without actually executing query.
     */
    public void testPreparedMetaData()
        throws Exception
    {
        String sql = "select name from sales.emps";
        preparedStmt = connection.prepareStatement(sql);
        ResultSetMetaData metaData = preparedStmt.getMetaData();
        assertEquals(
            1,
            metaData.getColumnCount());
        assertTrue(metaData.isSearchable(1));
        assertEquals(
            ResultSetMetaData.columnNoNulls,
            metaData.isNullable(1));
        assertEquals(
            "NAME",
            metaData.getColumnName(1));
        assertEquals(
            128,
            metaData.getPrecision(1));
        assertEquals(
            0,
            metaData.getScale(1));
        assertEquals(
            Types.VARCHAR,
            metaData.getColumnType(1));
        assertEquals(
            "VARCHAR",
            metaData.getColumnTypeName(1));
        if (todo) {
            // TODO: Test getColumnClassName
            assertEquals(
                "String",
                metaData.getColumnClassName(1));
        }
        assertEquals(
            ResultSetMetaData.columnNoNulls,
            metaData.isNullable(1));
        assertEquals(
            false,
            metaData.isSigned(1));
    }

    /**
     * Verifies that DDL statements are validated at prepare time, not just
     * execution time.
     */
    public void testDdlPreparation()
    {
        // test error:  exceed timestamp precision
        String ddl =
            "create table sales.bad_tbl("
            + "ts timestamp(100) not null primary key)";
        try {
            preparedStmt = connection.prepareStatement(ddl);
        } catch (SQLException ex) {
            // expected; verify that the message refers to precision
            Assert.assertTrue(
                "Expected message about precision but got '"
                + ex.getMessage() + "'",
                ex.getMessage().indexOf("Precision") > -1);
            return;
        }
        fail("Expected failure due to invalid DDL");
    }

    // TODO:  re-execute DDL, DML

    /**
     * Tests valid usage of a dynamic parameter and retrieval of associated
     * metadata.
     */
    public void testDynamicParameter()
        throws Exception
    {
        String sql = "select empid from sales.emps where name=?";
        preparedStmt = connection.prepareStatement(sql);
        ParameterMetaData pmd = preparedStmt.getParameterMetaData();
        assertEquals(
            1,
            pmd.getParameterCount());
        assertEquals(
            128,
            pmd.getPrecision(1));
        assertEquals(
            0,
            pmd.getScale(1));
        assertEquals(
            Types.VARCHAR,
            pmd.getParameterType(1));
        assertEquals(
            "VARCHAR",
            pmd.getParameterTypeName(1));
        if (todo) {
            // TODO: Test getParameterClassName
            assertEquals(
                "String",
                pmd.getParameterClassName(1));
        }
        assertEquals(
            ParameterMetaData.parameterNullable,
            pmd.isNullable(1));
        assertEquals(
            ParameterMetaData.parameterModeIn,
            pmd.getParameterMode(1));
        assertEquals(
            false,
            pmd.isSigned(1));

        preparedStmt.setString(1, "Wilma");
        resultSet = preparedStmt.executeQuery();
        compareResultSet(Collections.singleton("1"));
        preparedStmt.setString(1, "Eric");
        resultSet = preparedStmt.executeQuery();
        compareResultSet(Collections.singleton("3"));
        preparedStmt.setString(1, "George");
        resultSet = preparedStmt.executeQuery();
        assertEquals(
            0,
            getResultSetCount());
        preparedStmt.setString(1, null);
        resultSet = preparedStmt.executeQuery();
        assertEquals(
            0,
            getResultSetCount());
    }

    /**
     * Tests valid usage of multiple dynamic parameters.
     */
    public void testMultipleDynamicParameters1()
        throws Exception
    {
        // NOTE: This query tests FennelRelUtil.convertIntervalTupleToRel()'s
        // createNullFilter call.
        String sql =
            "select empid from sales.emps where deptno >= ? AND age < ?";
        preparedStmt = connection.prepareStatement(sql);

        preparedStmt.setInt(1, 20);
        preparedStmt.setInt(2, 75);
        resultSet = preparedStmt.executeQuery();
        compareResultSet(Collections.singleton("1"));

        preparedStmt.setInt(1, 10);
        preparedStmt.setInt(2, 75);
        resultSet = preparedStmt.executeQuery();
        compareResultSet(
            new HashSet<String>(Arrays.asList(new String[] { "30", "1" })));

        preparedStmt.setInt(1, 100);
        preparedStmt.setInt(2, 100);
        resultSet = preparedStmt.executeQuery();
        assertEquals(
            0,
            getResultSetCount());

        preparedStmt.setInt(1, 0);
        preparedStmt.setInt(2, 18);
        resultSet = preparedStmt.executeQuery();
        assertEquals(
            0,
            getResultSetCount());
    }

    /**
     * Tests valid usage of multiple dynamic parameters, including the fix for
     * FRG-72.
     */
    public void testMultipleDynamicParameters2()
        throws Exception
    {
        // NOTE: This query tests FennelRelUtil.convertIntervalTupleToRel()'s
        // createNullFilter call.
        String sql =
            "select empid from sales.emps where deptno >= ? and deptno <= ?";
        preparedStmt = connection.prepareStatement(sql);

        preparedStmt.setInt(1, 20);
        preparedStmt.setInt(2, 30);
        resultSet = preparedStmt.executeQuery();
        compareResultSet(
            new HashSet<String>(Arrays.asList(new String[] { "3", "1" })));

        preparedStmt.setInt(1, 30);
        preparedStmt.setInt(2, 40);
        resultSet = preparedStmt.executeQuery();
        compareResultSet(Collections.singleton("2"));

        preparedStmt.setObject(1, null);
        preparedStmt.setInt(2, 100);
        resultSet = preparedStmt.executeQuery();
        assertEquals(
            0,
            getResultSetCount());

        preparedStmt.setInt(1, 0);
        preparedStmt.setObject(2, null);
        resultSet = preparedStmt.executeQuery();
        assertEquals(
            0,
            getResultSetCount());
    }

    /**
     * Tests metadata for dynamic parameter in an UPDATE statement.
     */
    public void testDynamicParameterInUpdate()
        throws Exception
    {
        String sql = "update sales.emps set age = ?";
        preparedStmt = connection.prepareStatement(sql);
        ParameterMetaData pmd = preparedStmt.getParameterMetaData();
        assertEquals(
            1,
            pmd.getParameterCount());
        assertEquals(
            Types.INTEGER,
            pmd.getParameterType(1));
        assertEquals(
            "INTEGER",
            pmd.getParameterTypeName(1));
    }

    /**
     * Tests invalid usage of a dynamic parameter.
     */
    public void testInvalidDynamicParameter()
        throws Exception
    {
        String sql = "select ? from sales.emps";
        try {
            preparedStmt = connection.prepareStatement(sql);
        } catch (SQLException ex) {
            // expected
            return;
        }
        Assert.fail("Expected failure due to invalid dynamic param");
    }

    /**
     * Tests invalid attempt to execute a statement with a dynamic parameter
     * without preparation.
     */
    public void testDynamicParameterExecuteImmediate()
        throws Exception
    {
        final String msg =
            "Expected failure due to immediate execution with dynamic param";

        String sql = "select empid from sales.emps where name=?";
        try {
            resultSet = stmt.executeQuery(sql);
            Assert.fail(msg);
        } catch (SQLException ex) {
            // expected
        }

        try {
            boolean hasResult = stmt.execute(sql);
            Util.discard(hasResult);
            Assert.fail(msg);
        } catch (SQLException ex) {
            // expected
        }

        sql = "update sales.emps set age = ?";
        try {
            int cnt = stmt.executeUpdate(sql);
            Util.discard(cnt);
            Assert.fail(msg);
        } catch (SQLException ex) {
            // expected
        }
    }

    /**
     * Tests {@link Statement.setMaxRows}.
     */
    public void testMaxRows()
        throws Exception
    {
        assertEquals(0, stmt.getMaxRows());
        stmt.setMaxRows(1);
        assertEquals(1, stmt.getMaxRows());
        String sql = "select name from sales.depts order by 1";
        resultSet = stmt.executeQuery(sql);
        Set refSet = new HashSet();
        refSet.add("Accounts");
        compareResultSet(refSet);
        stmt.setMaxRows(0);
        assertEquals(0, stmt.getMaxRows());
        resultSet = stmt.executeQuery(sql);
        refSet = new HashSet();
        refSet.add("Accounts");
        refSet.add("Marketing");
        refSet.add("Sales");
        compareResultSet(refSet);
    }

    //~ Inner Interfaces -------------------------------------------------------

    public static interface JdbcTester
    {
        public void setUp()
            throws Exception;

        public void tearDown()
            throws Exception;

        public Connection getConnection();

        public Statement getStatement();
    }

    //~ Inner Classes ----------------------------------------------------------

    public static class FarragoJdbcTester
        implements JdbcTester
    {
        FarragoTestCase testCase;

        protected FarragoJdbcTester(String name)
            throws Exception
        {
            testCase = new FarragoTestCase(name) {
                };
        }

        public void setUp()
            throws Exception
        {
            testCase.setUp();
        }

        public void tearDown()
            throws Exception
        {
            testCase.tearDown();
        }

        public Connection getConnection()
        {
            return testCase.connection;
        }

        public Statement getStatement()
        {
            return testCase.stmt;
        }
    }

    // TODO 17-Apr-2006: don't use typenames which class with those
    // in java.lang; that's just confusing!

    /**
     * Defines a SQL type, and a corresponding column in the datatypes table,
     * and some operations particular to each type.
     */
    private static class TestSqlType
    {
        /**
         * Definition of the <code>TINYINT</code> SQL type.
         */
        private static final TestSqlType Tinyint =
            new TestSqlIntegralType(TINYINT,
                "tinyint",
                Byte.MIN_VALUE,
                Byte.MAX_VALUE) {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return
                            new Byte(
                                (byte) NumberUtil.round(
                                    ((Number) value).doubleValue()));
                    }
                    if (value instanceof Boolean) {
                        return
                            new Byte(
                                ((Boolean) value).booleanValue() ? (byte) 1
                                : (byte) 0);
                    }
                    if (value instanceof String) {
                        return Byte.valueOf((String) value);
                    }
                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>SMALLINT</code> SQL type.
         */
        private static final TestSqlType Smallint =
            new TestSqlIntegralType(SMALLINT,
                "smallint",
                Short.MIN_VALUE,
                Short.MAX_VALUE) {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return
                            new Short(
                                (short) NumberUtil.round(
                                    ((Number) value).doubleValue()));
                    }
                    if (value instanceof Boolean) {
                        return
                            new Short(
                                ((Boolean) value).booleanValue() ? (short) 1
                                : (short) 0);
                    }
                    if (value instanceof String) {
                        return Short.valueOf(((String) value).trim());
                    }
                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>INTEGER</code> SQL type.
         */
        private static final TestSqlType Integer =
            new TestSqlIntegralType(INTEGER,
                "integer",
                java.lang.Integer.MIN_VALUE,
                java.lang.Integer.MAX_VALUE) {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return
                            new Integer(
                                (int) NumberUtil.round(
                                    ((Number) value).doubleValue()));
                    }
                    if (value instanceof Boolean) {
                        return
                            new Integer(
                                ((Boolean) value).booleanValue() ? 1 : 0);
                    }
                    if (value instanceof String) {
                        return
                            java.lang.Integer.valueOf(((String) value).trim());
                    }
                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>BIGINT</code> SQL type.
         */
        private static final TestSqlType Bigint =
            new TestSqlIntegralType(BIGINT,
                "bigint",
                Long.MIN_VALUE,
                Long.MAX_VALUE) {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return
                            new Long(
                                (long) NumberUtil.round(
                                    ((Number) value).doubleValue()));
                    }
                    if (value instanceof Boolean) {
                        return
                            new Long(((Boolean) value).booleanValue() ? 1 : 0);
                    }
                    if (value instanceof String) {
                        return Long.valueOf(((String) value).trim());
                    }
                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>REAL</code> SQL type.
         */
        private static final TestSqlType Real =
            new TestSqlApproxType(REAL,
                "real",
                -java.lang.Float.MAX_VALUE,
                java.lang.Float.MAX_VALUE) {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        // SQL real yields Java float
                        return new Float(((Number) value).floatValue());
                    }
                    if (value instanceof Boolean) {
                        return
                            new Float(((Boolean) value).booleanValue() ? 1 : 0);
                    }
                    if (value instanceof String) {
                        return java.lang.Float.valueOf(((String) value).trim());
                    }
                    return super.getExpected(value);
                }
            };

        // REVIEW jvs 17-Apr-2006:  shouldn't it be -maxFloat, maxFloat
        // for the limits here?

        /**
         * Definition of the <code>FLOAT</code> SQL type.
         */
        private static final TestSqlType Float =
            new TestSqlApproxType(FLOAT, "float", -maxDouble, maxDouble) {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        // SQL float yields Java double
                        return new Double(((Number) value).doubleValue());
                    }
                    if (value instanceof Boolean) {
                        return
                            new Double(
                                ((Boolean) value).booleanValue() ? 1 : 0);
                    }
                    if (value instanceof String) {
                        return
                            java.lang.Double.valueOf(((String) value).trim());
                    }
                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>DOUBLE</code> SQL type.
         */
        private static final TestSqlType Double =
            new TestSqlApproxType(DOUBLE, "double", -maxDouble, maxDouble) {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        // SQL double yields Java double
                        return new Double(((Number) value).doubleValue());
                    }
                    if (value instanceof Boolean) {
                        return
                            new Double(
                                ((Boolean) value).booleanValue() ? 1 : 0);
                    }
                    if (value instanceof String) {
                        return
                            java.lang.Double.valueOf(((String) value).trim());
                    }
                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>BOOLEAN</code> SQL type.
         */
        private static final TestSqlType Boolean =
            new TestSqlType(BOOLEAN, "boolean") {
                public int checkIsValid(Object value, boolean strict)
                {
                    if ((value == null) || (value instanceof Boolean)) {
                        return VALID;
                    }
                    if (value instanceof Number) {
                        return VALID;
                    }
                    if (value instanceof String) {
                        String str = ((String) value).trim();
                        if (str.equalsIgnoreCase("TRUE")
                            || str.equalsIgnoreCase("FALSE")
                            || str.equalsIgnoreCase("UNKNOWN")) {
                            return VALID;
                        }
                        try {
                            java.lang.Double.parseDouble(str);
                            return VALID;
                        } catch (NumberFormatException e) {
                            return BADFORMAT;
                        }
                    }
                    return INVALID;
                }

                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return new Boolean(((Number) value).longValue() != 0);
                    }
                    if (value instanceof String) {
                        String str = ((String) value).trim();
                        if (str.equalsIgnoreCase("TRUE")) {
                            return new Boolean(true);
                        } else if (str.equalsIgnoreCase("FALSE")) {
                            return new Boolean(false);
                        } else if (str.equalsIgnoreCase("UNKNOWN")) {
                            return null;
                        }

                        double n = java.lang.Double.parseDouble(str);
                        return new Boolean(n != 0);
                    }
                    return super.getExpected(value);
                }
            };

        /* Are we supporting bit/decimal/numeric? see dtbug175 */
        /*"bit", */
        /*"decimal(12,2)", */
        /* If strings too small - will get:
        java: TupleAccessor.cpp:416: void
         fennel::TupleAccessor::unmarshal(fennel::TupleData&, unsigned int)
         const: Assertion `value.cbData <= accessor.cbStorage' failed.
         */

        /**
         * Definition of the <code>CHAR(100)</code> SQL type.
         */
        private static final TestSqlType Char =
            new TestSqlType(CHAR, "char(100)") {
                public int checkIsValid(Object value, boolean strict)
                {
                    if (value == null) {
                        return VALID;
                    } else if (value instanceof byte []) {
                        return INVALID;
                    } else {
                        String str = String.valueOf(value);
                        if (str.length() <= 100) {
                            return VALID;
                        } else {
                            return TOOLONG;
                        }
                    }
                }

                public Object getExpected(Object value)
                {
                    String s = String.valueOf(value);
                    if (s.length() < 100) {
                        // Pad to 100 characters.
                        StringBuffer buf = new StringBuffer(s);
                        while (buf.length() < 100) {
                            buf.append(' ');
                        }
                        s = buf.toString();
                    }
                    return s;
                }
            };

        /**
         * Definition of the <code>VARCHAR(200)</code> SQL type.
         */
        private static final TestSqlType Varchar =
            new TestSqlType(VARCHAR, "varchar(200)") {
                public int checkIsValid(Object value, boolean strict)
                {
                    if (value == null) {
                        return VALID;
                    } else if (value instanceof byte []) {
                        return INVALID;
                    } else {
                        String str = String.valueOf(value);
                        if (str.length() <= 200) {
                            return VALID;
                        } else {
                            return TOOLONG;
                        }
                    }
                }

                public Object getExpected(Object value)
                {
                    return String.valueOf(value);
                }
            };

        /**
         * Definition of the <code>BINARY(10)</code> SQL type.
         */
        private static final TestSqlType Binary =
            new TestSqlType(BINARY, "binary(10)") {
                public int checkIsValid(Object value, boolean strict)
                {
                    if (value == null) {
                        return VALID;
                    } else if (value instanceof byte []) {
                        byte [] bytes = (byte []) value;
                        if (bytes.length <= 10) {
                            return VALID;
                        } else {
                            return TOOLONG;
                        }
                    } else {
                        return INVALID;
                    }
                }

                public Object getExpected(Object value)
                {
                    if (value instanceof byte []) {
                        byte [] b = (byte []) value;
                        if (b.length == 10) {
                            return b;
                        }

                        // Pad to 10 bytes.
                        byte [] b2 = new byte[10];
                        System.arraycopy(
                            b,
                            0,
                            b2,
                            0,
                            Math.min(b.length, 10));
                        return b2;
                    }

                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>VARBINARY(20)</code> SQL type.
         */
        private static final TestSqlType Varbinary =
            new TestSqlType(VARBINARY, "varbinary(20)") {
                public int checkIsValid(Object value, boolean strict)
                {
                    if (value == null) {
                        return VALID;
                    } else if (value instanceof byte []) {
                        byte [] bytes = (byte []) value;
                        if (bytes.length <= 20) {
                            return VALID;
                        } else {
                            return TOOLONG;
                        }
                    } else {
                        return INVALID;
                    }
                }
            };

        /**
         * Definition of the <code>TIME(0)</code> SQL type.
         */
        private static final TestSqlType Time =
            new TestSqlType(TIME, "Time(0)") {
                public int checkIsValid(Object value, boolean strict)
                {
                    if (value == null) {
                        return VALID;
                    } else if ((value instanceof java.sql.Time)
                        || (value instanceof java.sql.Timestamp)) {
                        return VALID;
                    } else if (value instanceof String) {
                        try {
                            java.sql.Time.valueOf(((String) value).trim());
                            return VALID;
                        } catch (Exception e) {
                            return BADFORMAT;
                        }
                    } else {
                        return INVALID;
                    }
                }

                public Object getExpected(Object value)
                {
                    if (value instanceof java.util.Date) {
                        // Lop off the date, leaving only the time of day.
                        java.util.Date d = (java.util.Date) value;
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(d);
                        cal.set(Calendar.YEAR, 1970);
                        cal.set(Calendar.MONTH, 0);
                        cal.set(Calendar.DAY_OF_MONTH, 1);
                        return new Time(cal.getTimeInMillis());
                    } else if (value instanceof String) {
                        return java.sql.Time.valueOf(((String) value).trim());
                    }

                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>DATE</code> SQL type.
         */
        private static final TestSqlType Date =
            new TestSqlType(DATE, "Date") {
                public int checkIsValid(Object value, boolean strict)
                {
                    if (value == null) {
                        return VALID;
                    } else if ((value instanceof java.sql.Date)
                        || (value instanceof java.sql.Timestamp)) {
                        return VALID;
                    } else if (value instanceof String) {
                        try {
                            java.sql.Date.valueOf(((String) value).trim());
                            return VALID;
                        } catch (Exception e) {
                            return BADFORMAT;
                        }
                    } else {
                        return INVALID;
                    }
                }

                public Object getExpected(Object value)
                {
                    if (value instanceof java.util.Date) {
                        // Truncate the date to 12:00AM
                        java.util.Date d = (java.util.Date) value;
                        Calendar cal = Calendar.getInstance();
                        cal.setTime(d);
                        cal.set(Calendar.HOUR_OF_DAY, 0);
                        cal.set(Calendar.MINUTE, 0);
                        cal.set(Calendar.SECOND, 0);
                        cal.set(Calendar.MILLISECOND, 0);
                        return new Date(cal.getTimeInMillis());
                    } else if (value instanceof String) {
                        return java.sql.Date.valueOf(((String) value).trim());
                    }

                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>TIMESTAMP</code> SQL type.
         */
        private static final TestSqlType Timestamp =
            new TestSqlType(TIMESTAMP, "timestamp(0)") {
                public int checkIsValid(Object value, boolean strict)
                {
                    if (value == null) {
                        return VALID;
                    } else if ((value instanceof java.sql.Date)
                        || (value instanceof java.sql.Timestamp)) {
                        return VALID;
                    } else if (value instanceof String) {
                        try {
                            java.sql.Timestamp.valueOf(((String) value).trim());
                            return VALID;
                        } catch (Exception e) {
                            return BADFORMAT;
                        }
                    } else {
                        return INVALID;
                    }
                }

                public Object getExpected(Object value)
                {
                    if (value instanceof Timestamp) {
                        return value;
                    } else if (value instanceof java.util.Date) {
                        return
                            new Timestamp(
                                ((java.util.Date) value).getTime());
                    } else if (value instanceof String) {
                        return
                            java.sql.Timestamp.valueOf(((String) value).trim());
                    }
                    return super.getExpected(value);
                }
            };

        /**
         * Definition of the <code>DECIMAL</code> SQL type.
         */
        private static final TestSqlType Decimal =
            new TestSqlDecimalType(DECIMAL);

        private static final TestSqlType Decimal73 =
            new TestSqlDecimalType(DECIMAL73, 7, 3);

        private static final TestSqlType [] all =
            {
                Tinyint, Smallint, Integer, Bigint, Real, Float, Double, Boolean,
                Char, Varchar, Binary, Varbinary, Time, Date, Timestamp,
                Decimal, Decimal73
            };
        private static final TestSqlType [] typesNumericAndChars =
            {
                Tinyint, Smallint, Integer, Bigint, Real, Float, Double, Char,
                Varchar, Decimal, Decimal73
            };
        private static final TestSqlType [] typesNumeric =
            {
                Tinyint, Smallint, Integer, Bigint, Real, Float, Double,
                Decimal, Decimal73
            };
        private static final TestSqlType [] typesChar = {
                Char, Varchar
            };
        private static final TestSqlType [] typesBinary =
            { Binary, Varbinary, };
        private static final TestSqlType [] typesDateTime =
            { Time, Date, Timestamp };
        public static final int VALID = 0;
        public static final int INVALID = 1;
        public static final int OUTOFRANGE = 2;
        public static final int TOOLONG = 3;
        public static final int BADFORMAT = 4;
        public static final int NOTNULLABLE = 5;
        public static final String [] validityName =
            {
                "valid", "invalid", "out of range", "too long", "bad format", "not nullable"
            };
        public static final Pattern [] exceptionPatterns =
            new Pattern[] {
                null,
                Pattern.compile(
                    ".*Cannot assign a value of Java class .* to .*"),
                Pattern.compile(".*out of range.*"),
                Pattern.compile(".*too long.*"),
                Pattern.compile(".*cannot be converted.*"),
                Pattern.compile(".*non-nullable.*")
            };
        private final int ordinal;
        private final String string;

        TestSqlType(
            int ordinal,
            String example)
        {
            this.ordinal = ordinal;
            this.string = example;
        }

        protected static boolean isBetween(Number number, long min, long max)
        {
            long x = number.longValue();
            return (min <= x) && (x <= max);
        }

        protected static boolean isBetween(Number number,
            double min,
            double max)
        {
            double x = number.doubleValue();
            return (min <= x) && (x <= max);
        }

        public Object getExpected(Object value)
        {
            return value;
        }

        public int checkIsValid(Object value)
        {
            return checkIsValid(value, true);
        }

        public int checkIsValid(Object value, boolean strict)
        {
            return VALID;
        }
    }

    /**
     * Defines class for testing integral sql type
     */
    private static class TestSqlIntegralType
        extends TestSqlType
    {
        long min;
        long max;

        TestSqlIntegralType(int ordinal, String example, long min, long max)
        {
            super(ordinal, example);
            this.min = min;
            this.max = max;
        }

        public int checkIsValid(Object value, boolean strict)
        {
            if ((value == null) || (value instanceof Boolean)) {
                return VALID;
            } else if (value instanceof Number) {
                if (strict) {
                    return
                        isBetween((Number) value, min, max) ? VALID
                        : OUTOFRANGE;
                } else {
                    return VALID;
                }
            } else if (value instanceof String) {
                String str = ((String) value).trim();
                try {
                    Long n = Long.valueOf(str);
                    if (strict) {
                        return isBetween(n, min, max) ? VALID : OUTOFRANGE;
                    } else {
                        return VALID;
                    }
                } catch (NumberFormatException e) {
                    return BADFORMAT;
                }
            }

            return INVALID;
        }
    }

    /**
     * Defines class for testing approximate sql type
     */
    private static class TestSqlApproxType
        extends TestSqlType
    {
        double min;
        double max;

        TestSqlApproxType(int ordinal, String example, double min, double max)
        {
            super(ordinal, example);
            this.min = min;
            this.max = max;
        }

        public int checkIsValid(Object value, boolean strict)
        {
            if ((value == null) || (value instanceof Boolean)) {
                return VALID;
            } else if (value instanceof Number) {
                if (strict) {
                    return
                        isBetween((Number) value, min, max) ? VALID
                        : OUTOFRANGE;
                } else {
                    return VALID;
                }
            } else if (value instanceof String) {
                String str = ((String) value).trim();
                try {
                    Long n = Long.valueOf(str);
                    if (strict) {
                        return isBetween(n, min, max) ? VALID : OUTOFRANGE;
                    } else {
                        return VALID;
                    }
                } catch (NumberFormatException e) {
                    return BADFORMAT;
                }
            }

            return INVALID;
        }
    }

    /**
     * Defines class for testing decimal sql type
     */
    private static class TestSqlDecimalType
        extends TestSqlType
    {
        final static int MAX_PRECISION = 19;
        int precision;
        int scale;
        BigInteger maxUnscaled;
        BigInteger minUnscaled;

        TestSqlDecimalType(int ordinal)
        {
            super(ordinal, "DECIMAL");
            this.precision = MAX_PRECISION;
            this.scale = 0;
        }

        TestSqlDecimalType(
            int ordinal,
            int precision)
        {
            super(ordinal, "DECIMAL(" + precision + ")");
            this.precision = precision;
            this.scale = 0;
        }

        TestSqlDecimalType(
            int ordinal,
            int precision,
            int scale)
        {
            super(ordinal, "DECIMAL(" + precision + "," + scale + ")");
            this.precision = precision;
            this.scale = scale;
        }

        private BigInteger getMaxUnscaled()
        {
            if (maxUnscaled == null) {
                maxUnscaled = NumberUtil.getMaxUnscaled(precision);
            }
            return maxUnscaled;
        }

        private BigInteger getMinUnscaled()
        {
            if (minUnscaled == null) {
                minUnscaled = NumberUtil.getMinUnscaled(precision);
            }
            return minUnscaled;
        }

        public int checkIsValid(Object value, boolean strict)
        {
            if ((value == null) || (value instanceof Boolean)) {
                return VALID;
            }
            try {
                if ((value instanceof Number) || (value instanceof String)) {
                    BigDecimal expected = (BigDecimal) getExpected(value);
                    BigInteger usv = expected.unscaledValue();
                    if (strict) {
                        if (usv.compareTo(getMaxUnscaled()) > 0) {
                            return OUTOFRANGE;
                        } else if (usv.compareTo(getMinUnscaled()) < 0) {
                            return OUTOFRANGE;
                        }
                    }
                } else {
                    return INVALID;
                }
            } catch (NumberFormatException ex) {
                return BADFORMAT;
            }
            return VALID;
        }

        public Object getExpected(Object value)
        {
            BigDecimal n;
            if (value instanceof Number) {
                n = NumberUtil.toBigDecimal((Number) value);
            } else if (value instanceof String) {
                n = new BigDecimal(((String) value).trim());
            } else if (value instanceof Boolean) {
                n = new BigDecimal(((Boolean) value).booleanValue() ? 1 : 0);
            } else {
                return super.getExpected(value);
            }
            n = NumberUtil.rescaleBigDecimal(n, scale);
            return n;
        }
    }

    /**
     * Defines a Java type.
     *
     * <p>Each type has a correponding set of get/set methods, for example
     * "Boolean" has {@link ResultSet#getBoolean(int)} and {@link
     * PreparedStatement#setBoolean(int,boolean)}.
     */
    protected static class TestJavaType
    {
        private static final TestJavaType Boolean =
            new TestJavaType("Boolean", boolean.class, true);
        private static final TestJavaType Byte =
            new TestJavaType("Byte", byte.class, true);
        private static final TestJavaType Short =
            new TestJavaType("Short", short.class, true);
        private static final TestJavaType Int =
            new TestJavaType("Int", int.class, true);
        private static final TestJavaType Long =
            new TestJavaType("Long", long.class, true);
        private static final TestJavaType Float =
            new TestJavaType("Float", float.class, true);
        private static final TestJavaType Double =
            new TestJavaType("Double", double.class, true);
        private static final TestJavaType BigDecimal =
            new TestJavaType("BigDecimal", BigDecimal.class, true);
        private static final TestJavaType String =
            new TestJavaType("String", String.class, true);
        private static final TestJavaType Bytes =
            new TestJavaType("Bytes", byte [].class, true);

        // Date, Time, Timestamp each have an additional set method, e.g.
        //   setXxx(int,Date,Calendar)
        // TODO: test this
        private static final TestJavaType Date =
            new TestJavaType("Date", Date.class, true);
        private static final TestJavaType Time =
            new TestJavaType("Time", Time.class, true);
        private static final TestJavaType Timestamp =
            new TestJavaType("Timestamp", Timestamp.class, true);

        // Object has 2 extra 'setObject' methods:
        //   setObject(int,Object,int targetTestSqlType)
        //   setObject(int,Object,int targetTestSqlType,int scale)
        // TODO: test this
        private static final TestJavaType Object =
            new TestJavaType("Object", Object.class, true);

        // next 4 are not regular, because their 'set' method has an extra
        // parameter, e.g. setAsciiStream(int,InputStream,int length)
        private static final TestJavaType AsciiStream =
            new TestJavaType("AsciiStream", InputStream.class, false);
        private static final TestJavaType UnicodeStream =
            new TestJavaType("UnicodeStream", InputStream.class, false);
        private static final TestJavaType BinaryStream =
            new TestJavaType("BinaryStream", InputStream.class, false);
        private static final TestJavaType CharacterStream =
            new TestJavaType("CharacterStream", Reader.class, false);
        private static final TestJavaType Ref =
            new TestJavaType("Ref", Ref.class, true);
        private static final TestJavaType Blob =
            new TestJavaType("Blob", Blob.class, true);
        private static final TestJavaType Clob =
            new TestJavaType("Clob", Clob.class, true);
        private static final TestJavaType Array =
            new TestJavaType("Array", Array.class, true);
        private final String name;
        private final Class clazz;

        /**
         * whether it has a setXxxx(int,xxx) method
         */
        private final boolean regular;
        private final Method setMethod;
        TestJavaType [] all =
            {
                Boolean, Byte, Short, Int, Long, Float, Double, BigDecimal, String,
                Bytes, Date, Time, Timestamp, Object, AsciiStream, UnicodeStream,
                BinaryStream, CharacterStream, Ref, Blob, Clob, Array,
            };

        private TestJavaType(
            String name,
            Class clazz,
            boolean regular)
        {
            this.name = name;
            this.clazz = clazz;

            this.regular = regular;

            // e.g. PreparedStatement.setBoolean(int,boolean)
            Method method = null;
            try {
                method =
                    PreparedStatement.class.getMethod(
                        "set" + name,
                        new Class[] { int.class, clazz });
            } catch (NoSuchMethodException e) {
            } catch (SecurityException e) {
            }
            this.setMethod = method;
        }
    }
}

// End FarragoJdbcTest.java
