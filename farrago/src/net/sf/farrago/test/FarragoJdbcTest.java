/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

import junit.framework.Assert;
import junit.framework.Test;

import java.sql.Date;

/**
 * FarragoJdbcTest tests specifics of the Farrago implementation of the JDBC
 * API.  See also unitsql/jdbc/*.sql.
 *
 * todo: test:
 * 1. string too long for char/varchar field
 * 2. value which converted to char/varchar is too long
 * 3. various numeric values out of range, e.g. put 65537 in a tinyint
 * 4. assign boolean to integer columns (becomes 0/1)
 * 5. assign numerics to boolean
 *    5a. small enough
 *    5b out of range (not 0 or 1)
 *
 * @author Tim Leung
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcTest extends FarragoTestCase
{
    //~ Static fields/initializers --------------------------------------------

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
    private static final double minDouble = Double.MIN_VALUE;
    private static final double maxDouble = Double.MAX_VALUE;
    private static final boolean boolValue = true;
    private static final BigDecimal bigDecimalValue =
        new BigDecimal(maxDouble);
    private static final String stringValue = "0";
    private static final byte [] bytes = { 127, -34, 56, 29, 56, 49 };

    /**
     * A point of time in Sydney, Australia. (The timezone is deliberately not
     * PST, where the test is likely to be run, or GMT, and should be in
     * daylight-savings time in December.)
     *
     * 4:22:33.456 PM on 21st December 2004 Japan (GMT+9)
     * is 9 hours earlier in GMT, namely
     * 7:22:33.456 AM on 21st December 2004 GMT
     * and is another 8 hours earlier in PST:
     * 11:22:33.456 PM on 20th December 2004 PST (GMT-8)
     */
    private static final Calendar sydneyCal =
        makeCalendar("JST", 2004, 11, 21, 16, 22, 33, 456);
    private static final Time time = new Time(sydneyCal.getTime().getTime());
    private static final Date date = new Date(sydneyCal.getTime().getTime());

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

    private static final Timestamp timestamp =
        new Timestamp(sydneyCal.getTime().getTime());
    private static final Byte tinyIntObj = new Byte(minByte);
    private static final Short smallIntObj = new Short(maxShort);
    private static final Integer integerObj = new Integer(minInt);
    private static final Long bigIntObj = new Long(maxLong);
    private static final Float floatObj = new Float(maxFloat);
    private static final Double doubleObj = new Double(maxDouble);
    private static final Boolean boolObj = Boolean.FALSE;
    private static final BigDecimal decimalObj =
        new BigDecimal(13412342124143241D);
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
    private static boolean schemaExists = false;
    private static final String [] columnNames =
        new String[TestSqlType.all.length];
    private static String columnTypeStr = "";
    private static String columnStr = "";
    private static String paramStr = "";

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
    private static final boolean dtbug119_fixed = false;
    private static final boolean todo = false;

    //~ Instance fields -------------------------------------------------------

    private Object [] values;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcTest object.
     */
    public FarragoJdbcTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoJdbcTest.class);
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

    public void testSynchronousCancel()
        throws Exception
    {
        testCancel(true);
    }
    
    public void testAsynchronousCancel()
        throws Exception
    {
        testCancel(false);
    }
    
    private void testCancel(boolean synchronous)
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
        sql = "create foreign table cancel_test.m(id int not null) "
            + "server sys_mock_foreign_data_server "
            + "options(executor_impl 'FENNEL', row_count '1000000000')";
        stmt.execute(sql);
        sql = "select * from cancel_test.m";
        resultSet = stmt.executeQuery(sql);
        boolean found;
        found = resultSet.next();
        assertTrue(found);
        found = resultSet.next();
        assertTrue(found);
        if (synchronous) {
            // cancel immediately
            stmt.cancel();
        } else {
            Timer timer = new Timer(true);
            // cancel after 2 seconds
            TimerTask task = new TimerTask() 
                {
                    public void run()
                    {
                        try {
                            stmt.cancel();
                        } catch (SQLException ex) {
                            Assert.fail(
                                "Cancel request failed:  " 
                                + ex.getMessage());
                        }
                    }
                };
            timer.schedule(task, 2000);
        }
        try {
            while (resultSet.next()) {
            }
        } catch (SQLException ex) {
            // expected
            Assert.assertTrue(
                "Expected abort message but got '" + ex.getMessage() + "'", 
                ex.getMessage().indexOf("abort") > -1);
            return;
        }
        Assert.fail("Expected failure due to cancel request");
    }

    // NOTE jvs 26-July-2004:  some of the tests in this class modify fixture
    // tables such as SALES.EMPS, but that's OK, because transactions are
    // implicitly rolled back by FarragoTestCase.tearDown.
    public void testPreparedStmtDataTypes()
        throws Exception
    {
        String query =
            "insert into datatypes_schema.dataTypes_table values " + paramStr;
        preparedStmt = connection.prepareStatement(query);
        values = new Object[2 + TestSqlType.all.length];
        preparedStmt.setInt(1, 100);
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
        checkSetDoubleMin();
        checkSetDoubleMax();
        checkSetBooleanFalse();
        if (todo) {
            // BigDecimal values seem to go in but don't come out!
            checkSetBigDecimal();
        }
        checkSetBytes();
        checkSetDate();
        checkSetTime();
        checkSetTimestamp();
        checkSetObject();
    }

    protected void setUp()
        throws Exception
    {
        super.setUp();

        synchronized (getClass()) {
            if (!schemaExists) {
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
        checkSet(TestJavaType.Date, TestSqlType.Timestamp, date);
        checkResults(TestJavaType.Date);
    }

    private void checkSetBytes()
        throws Exception
    {
        checkSet(TestJavaType.Bytes, TestSqlType.typesBinary, bytes);
        checkResults(TestJavaType.Bytes);
    }

    private void checkSetBigDecimal()
        throws Exception
    {
        checkSet(TestJavaType.BigDecimal, TestSqlType.typesNumericAndChars,
            bigDecimalValue);
        checkResults(TestJavaType.BigDecimal);
    }

    private void checkSetBooleanFalse()
        throws Exception
    {
        checkSet(
            TestJavaType.Boolean, TestSqlType.typesNumericAndChars, boolObj);
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

    private void checkSetString()
        throws Exception
    {
        // Skipped: dtbug220
        // for (int j=2; j<=TestSqlType.length; j++)
        checkSet(TestJavaType.String, TestSqlType.Char, stringValue);
        checkSet(TestJavaType.String, TestSqlType.Varchar, stringValue);
        if (true) {
            //todo: setString on VARBINARY column should fail
            checkSet(TestJavaType.String, TestSqlType.Binary, stringValue);
            checkSet(TestJavaType.String, TestSqlType.Varbinary, stringValue);
        }
        checkResults(TestJavaType.String);
    }

    private void checkResults(TestJavaType javaType)
        throws SQLException
    {
        int res = preparedStmt.executeUpdate();
        assertEquals(1, res);

        // Select the results back, to make sure the values we expected got
        // written.
        final Statement stmt = connection.createStatement();
        final ResultSet resultSet =
            stmt.executeQuery("select * from datatypes_schema.dataTypes_table");
        final int columnCount = resultSet.getMetaData().getColumnCount();
        assert columnCount == (TestSqlType.all.length + 1);
        while (resultSet.next()) {
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
        if (expected instanceof byte [] && actual instanceof byte []
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

    private void checkSet(
        TestJavaType javaType,
        TestSqlType sqlType,
        Object value)
        throws Exception
    {
        int column = sqlType.ordinal;
        int validity = sqlType.checkIsValid(value);
        Throwable throwable;
        tracer.fine("Call PreparedStmt.set" + javaType.name + "(" + column
            + ", " + value + "), value is " + TestSqlType.validityName[validity]);
        try {
            javaType.setMethod.invoke(
                preparedStmt,
                new Object [] { new Integer(column), value });
            throwable = null;
        } catch (IllegalAccessException e) {
            throwable = e;
        } catch (IllegalArgumentException e) {
            throwable = e;
        } catch (InvocationTargetException e) {
            throwable = e.getCause();
        }
        switch (validity) {
        case TestSqlType.VALID:
            if (throwable != null) {
                fail("Error received when none expected, javaType="
                    + javaType.name + ", sqlType=" + sqlType.string
                    + ", value=" + value + ", throwable=" + throwable);
            }
            this.values[column] = value;
            break;
        case TestSqlType.INVALID:
            if (throwable instanceof SQLException) {
                String errorString = throwable.toString();
                if (errorString.matches(
                            ".*Cannot assign a value of Java class .* to .*")) {
                    break;
                }
            }
            fail("Was expecting error, javaType=" + javaType.name
                + ", sqlType=" + sqlType.string + ", value=" + value);
            break;
        case TestSqlType.OUTOFRANGE:
            Pattern outOfRangePattern = Pattern.compile("out of range");
            if (throwable instanceof SQLException) {
                String errorString = throwable.toString();
                if (outOfRangePattern.matcher(errorString).matches()) {
                    break;
                }
            }
            fail("Was expecting out-of-range error, javaType=" + javaType.name
                + ", sqlType=" + sqlType.string + ", value=" + value);
            break;
        }
    }

    public void testDataTypes()
        throws Exception
    {
        final String ins_query =
            "insert into datatypes_schema.dataTypes_table values ";
        String query = ins_query + paramStr;

        query =
            "select " + columnStr + " from datatypes_schema.datatypes_table";

        preparedStmt = connection.prepareStatement(query);

        resultSet = preparedStmt.executeQuery();
        int id;
        while (resultSet.next()) {
            id = resultSet.getInt(1);
            switch (id) {
            case 100:
                /* Numerics and timestamps skipped due to dtbug220 */
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
                    stringValue,
                    resultSet.getString(REAL));
                assertEquals(
                    stringValue,
                    resultSet.getString(FLOAT));
                assertEquals(
                    stringValue,
                    resultSet.getString(DOUBLE));
                assertEquals(
                    stringValue,
                    resultSet.getString(BOOLEAN));

                // Check CHAR - result String can be longer than the input string
                // Just check the first part
                assertEquals(
                    stringValue,
                    resultSet.getString(CHAR).substring(
                        0,
                        stringValue.length()));
                assertEquals(
                    stringValue,
                    resultSet.getString(VARCHAR));

                // What should BINARY/VARBINARY be?
                //assertEquals(stringValue, resultSet.getString(BINARY));
                //assertEquals(stringValue, resultSet.getString(VARBINARY));
                // dtbug110, 199
                assertEquals(
                    stringValue,
                    resultSet.getString(DATE));
                assertEquals(
                    stringValue,
                    resultSet.getString(TIME));
                assertEquals(
                    stringValue,
                    resultSet.getString(TIMESTAMP));
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
                    1,
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
                    1,
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
                    1,
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
                    -1,
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
                    1,
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
                    1,
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
                    -1,
                    resultSet.getInt(TINYINT));
                assertEquals(
                    -1,
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
                    1,
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
                    1,
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
                    -1,
                    resultSet.getLong(TINYINT));
                assertEquals(
                    -1,
                    resultSet.getLong(SMALLINT));
                assertEquals(
                    -1,
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
                    1,
                    resultSet.getLong(BOOLEAN));
                assertEquals(
                    maxLong,
                    resultSet.getLong(CHAR));
                assertEquals(
                    maxLong,
                    resultSet.getLong(VARCHAR));
                break;
            case 109:
                assertEquals(
                    0,
                    resultSet.getFloat(TINYINT),
                    0);
                assertEquals(
                    0,
                    resultSet.getFloat(SMALLINT),
                    0);
                assertEquals(
                    0,
                    resultSet.getFloat(INTEGER),
                    0);
                assertEquals(
                    0,
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
                    1,
                    resultSet.getFloat(BOOLEAN),
                    1);
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
                assertEquals(
                    -1,
                    resultSet.getFloat(TINYINT),
                    0);
                assertEquals(
                    -1,
                    resultSet.getFloat(SMALLINT),
                    0);
                assertEquals(
                    maxInt,
                    resultSet.getFloat(INTEGER),
                    0);
                assertEquals(
                    maxLong,
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
                    1,
                    resultSet.getFloat(BOOLEAN),
                    1);
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
                assertEquals(
                    0,
                    resultSet.getDouble(TINYINT),
                    0);
                assertEquals(
                    0,
                    resultSet.getDouble(SMALLINT),
                    0);
                assertEquals(
                    0,
                    resultSet.getDouble(INTEGER),
                    0);
                assertEquals(
                    0,
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
                    1,
                    resultSet.getDouble(BOOLEAN),
                    1);
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
                assertEquals(
                    -1,
                    resultSet.getDouble(TINYINT),
                    0);
                assertEquals(
                    -1,
                    resultSet.getDouble(SMALLINT),
                    0);
                assertEquals(
                    maxInt,
                    resultSet.getDouble(INTEGER),
                    0);
                assertEquals(
                    maxLong,
                    resultSet.getDouble(BIGINT),
                    0);
                assertEquals(
                    Float.POSITIVE_INFINITY,
                    resultSet.getDouble(REAL),
                    0);
                assertEquals(
                    maxDouble,
                    resultSet.getDouble(FLOAT),
                    0);
                assertEquals(
                    maxDouble,
                    resultSet.getDouble(DOUBLE),
                    0);
                assertEquals(
                    1,
                    resultSet.getDouble(BOOLEAN),
                    1);
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
                    resultSet.getBoolean(BOOLEAN));

                //assertEquals(boolValue, resultSet.getBoolean(CHAR));
                //can not convert String (true) to long exception
                //assertEquals(boolValue, resultSet.getBoolean(VARCHAR));
                break;
            case 114:

                // dtbug119
                if (dtbug119_fixed) {
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(TINYINT));
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(SMALLINT));
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(INTEGER));
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(BIGINT));
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(REAL));
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(FLOAT));
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(DOUBLE));

                    //todo:
                    //assertEquals(bigDecimalValue, resultSet.getBigDecimal(BigDecimal));
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(CHAR));
                    assertEquals(
                        bigDecimalValue,
                        resultSet.getBigDecimal(VARCHAR));
                }
                break;
            case 115:

                // Check BINARY - resBytes can be longer than the input bytes
                byte [] resBytes = resultSet.getBytes(BINARY);
                assertNotNull(resBytes);
                for (int i = 0; i < bytes.length; i++) {
                    assertEquals(bytes[i], resBytes[i]);
                }

                // Check VARBINARY - should be same length
                resBytes = resultSet.getBytes(VARBINARY);

                // resBytes null for some reason

                /*assertNotNull(resBytes);
                  assertEquals(bytes.length, resBytes.length);
                  for (int i=0; i<bytes.length; i++)
                  assertEquals(bytes[i], resBytes[i]); */
                break;
            case 116:
                assertEquals(
                    date,
                    resultSet.getDate(CHAR));
                assertEquals(
                    date,
                    resultSet.getDate(VARCHAR));
                assertEquals(
                    date,
                    resultSet.getDate(DATE));
                assertEquals(
                    date,
                    resultSet.getDate(TIMESTAMP));
                break;
            case 117:
                assertEquals(
                    time,
                    resultSet.getTime(CHAR));
                assertEquals(
                    time,
                    resultSet.getTime(VARCHAR));
                assertEquals(
                    time,
                    resultSet.getTime(TIME));
                assertEquals(
                    time,
                    resultSet.getTime(TIMESTAMP));
                break;
            case 118:
                assertEquals(
                    timestamp,
                    resultSet.getTimestamp(CHAR));
                assertEquals(
                    timestamp,
                    resultSet.getTimestamp(VARCHAR));
                assertEquals(
                    timestamp,
                    resultSet.getTimestamp(DATE));
                assertEquals(
                    timestamp,
                    resultSet.getTimestamp(TIME));
                assertEquals(
                    timestamp,
                    resultSet.getTimestamp(TIMESTAMP));
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

                // Check CHAR - result String can be longer than the input string
                // Just check the first part
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
                assertNotNull(resBytes);
                for (int i = 0; i < bytes.length; i++) {
                    assertEquals(bytes[i], resBytes[i]);
                }

                // Check VARBINARY - should be same length
                resBytes = (byte []) resultSet.getObject(VARBINARY);
                assertNotNull(resBytes);
                assertEquals(bytes.length, resBytes.length);
                for (int i = 0; i < bytes.length; i++) {
                    assertEquals(bytes[i], resBytes[i]);
                }

                // dtbug199
                //assertEquals(date, resultSet.getObject(DATE));
                //assertEquals(time, resultSet.getObject(TIME));
                //assertEquals(timestamp, resultSet.getObject(TIMESTAMP));
                break;
            default:

                // Unexpected id
                assert false;
                break;
            }
        }

        resultSet.close();
        resultSet = null;
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
            assertEquals(cal.getTime().getTime(), date.getTime());

            cal.set(2004, 11, 21, 12, 22, 33);

            assertEquals(cal.getTime().getTime(), tstamp.getTime());
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
                assertEquals(cal.getTime().getTime(), date.getTime());

                cal.set(2004, 11, 21, 12, 22, 33);

                assertEquals(cal.getTime().getTime(), tstamp.getTime());
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
        String sql = "select * from sales.emps";
        preparedStmt = connection.prepareStatement(sql);
        for (int i = 10; i >= -2; i--) {
            preparedStmt.setQueryTimeout(i);
            resultSet = preparedStmt.executeQuery();
            assertEquals(
                4,
                getResultSetCount());
            resultSet.close();
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
        preparedStmt =
            connection.prepareStatement(
                "insert into \"SALES\".\"EMPS\" values "
                + "(?, ?, 10, ?, ?, ?, 28, NULL, NULL, false)");

        preparedStmt.setByte(3, (byte) 1);
        preparedStmt.setByte(4, (byte) -128);
        doEmpInsert(1);

        preparedStmt.setShort(3, (short) 2);
        preparedStmt.setShort(4, (short) 32767);
        doEmpInsert(2);

        preparedStmt.setInt(3, 3);
        preparedStmt.setInt(4, -234234);
        doEmpInsert(3);

        preparedStmt.setLong(3, 4L);
        preparedStmt.setLong(4, 123432432432545455L);
        doEmpInsert(4);

        Throwable throwable;
        try {
            preparedStmt.setFloat(3, 5.0F);
            preparedStmt.setFloat(4, 6.02e+23F);
            doEmpInsert(5);
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
            doEmpInsert(6);
        } catch (SQLException e) {
            throwable = e;
        }
        assertExceptionMatches(throwable,
            ".*Value '6.2' is too long for parameter of type CHAR.1.");

        preparedStmt.setBigDecimal(
            3,
            new BigDecimal(2));
        preparedStmt.setBigDecimal(
            4,
            new BigDecimal(88.23432432));
        doEmpInsert(7);

        try {
            preparedStmt.setBoolean(3, false);
            preparedStmt.setBoolean(4, true);
            doEmpInsert(8);
        } catch (SQLException e) {
            throwable = e;
        }
        assertExceptionMatches(throwable,
            ".*Value 'false' is too long for parameter of type CHAR.1.");

        preparedStmt.setString(3, "x");
        preparedStmt.setBoolean(4, true);
        doEmpInsert(8);

        preparedStmt.setString(3, "F");
        preparedStmt.setString(4, "San Jose");
        doEmpInsert(9);

        preparedStmt.setObject(
            3,
            new Character('M'));
        preparedStmt.setObject(
            4,
            new StringBuffer("New York"));
        doEmpInsert(10);

        // this query won't find everything we insert above because of bugs in
        // executeUpdate when there's a setFloat/setDouble called on a varchar
        // column.  See comments in bug#117
        //query = "select gender, city, empid from sales.emps where name like '";
        //query += name + "%'";
        // for now, use this query instead
        String query =
            "select gender, city, empid from sales.emps where empno>1000";

        preparedStmt = connection.prepareStatement(query);

        resultSet = preparedStmt.executeQuery();
        int empid;
        while (resultSet.next()) {
            empid = resultSet.getInt(3);
            switch (empid) {
            case 101:

                // ERROR  - bug #117 logged for errors given in the following commented lines
                // assertEquals(gender, resultSet.getByte(1));
                // assertEquals(city, resultSet.getByte(2));
                break;
            case 102:

                // assertEquals(gender2, resultSet.getShort(1));
                // assertEquals(city2, resultSet.getShort(2));
                break;
            case 103:

                // assertEquals(gender3, resultSet.getInt(1));
                // assertEquals(city3, resultSet.getInt(2));
                break;
            case 104:

                // assertEquals(gender4, resultSet.getLong(1));
                // assertEquals(city4, resultSet.getLong(2));
                break;
            case 105:

                // assertEquals(gender5, resultSet.getFloat(1), 0.0001);
                // assertEquals(city5, resultSet.getFloat(2), 0.0001);
                break;
            case 106:

                // assertEquals(gender6, resultSet.getDouble(1), 0.0001);
                // assertEquals(city6, resultSet.getDouble(2), 0.0001);
                break;
            case 107:

                // ERROR
                // assertEquals(gender7, resultSet.getBigDecimal(1));
                // assertEquals(city7, resultSet.getBigDecimal(2));
                break;
            case 108:
                assertEquals(
                    "x",
                    resultSet.getString(1));

                // ERROR:  same as bug #117
                // assertEquals(city8, resultSet.getBoolean(2));
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

                // ERROR - the actual result looks correct, probably merely object type not matching
                // assertEquals(genderx, resultSet.getObject(1));
                // assertEquals(cityx, resultSet.getObject(2));
                break;
            default:

                // uncomment this when I can do a like sql query
                assertEquals(1, 2);
                break;
            }
        }

        resultSet.close();
        resultSet = null;
    }

    static void assertExceptionMatches(
        Throwable e,
        String expectedPattern)
    {
        if (e == null) {
            fail("Expected an error which matches pattern '" + expectedPattern
                + "'");
        }
        String msg = e.toString();
        if (!msg.matches(expectedPattern)) {
            fail("Got a different error '" + msg + "' than expected '"
                + expectedPattern + "'");
        }
    }

    private void doEmpInsert(int i)
        throws SQLException
    {
        String name = "JDBC Test Char";
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
                666,
                resultSet.getFloat(1),
                0.0001);
            assertEquals(
                deptno2,
                resultSet.getDouble(2),
                0.0001);

            // ERROR: Bug#119: getBigDecimal not supported
            // assertEquals(empid2, resultSet.getBigDecimal(3));
            // ERROR: Bug#117: getBoolean returns SQLException like getXXX on char data types (where XXX is numeric type)
            // assertEquals(age2, resultSet.getBoolean(4));
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
     * Tests re-execution of an unprepared query.  There's no black-box way to
     * verify that caching is working, but if it is, this will at least
     * exercise it.
     *
     * @throws Exception .
     */
    public void testCachedQuery()
        throws Exception
    {
        repeatQuery();
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
        stmt.execute("alter system set \"codeCacheMaxBytes\" = min");
        repeatQuery();

        // re-enable caching
        stmt.execute("alter system set \"codeCacheMaxBytes\" = max");
    }

    private void repeatQuery()
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
            Types.VARCHAR,
            metaData.getColumnType(1));
        assertEquals(
            "VARCHAR",
            metaData.getColumnTypeName(1));
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
            Types.VARCHAR,
            pmd.getParameterType(1));
        assertEquals(
            "VARCHAR",
            pmd.getParameterTypeName(1));

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
        String sql = "select empid from sales.emps where name=?";
        try {
            resultSet = stmt.executeQuery(sql);
        } catch (SQLException ex) {
            // expected
            return;
        }
        Assert.fail(
            "Expected failure due to immediate execution with dynamic param");
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Defines a SQL type, and a corresponding column in the datatypes table,
     * and some operations particular to each type.
     */
    private static class TestSqlType
    {
        /** Definition of the <code>TINYINT</code> SQL type. */
        private static final TestSqlType Tinyint =
            new TestSqlType(TINYINT, "tinyint") {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return new Byte(((Number) value).byteValue());
                    }
                    if (value instanceof Boolean) {
                        return new Byte(((Boolean) value).booleanValue()
                            ? (byte) 1 : (byte) 0);
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>SMALLINT</code> SQL type. */
        private static final TestSqlType Smallint =
            new TestSqlType(SMALLINT, "smallint") {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return new Short(((Number) value).shortValue());
                    }
                    if (value instanceof Boolean) {
                        return new Short(((Boolean) value).booleanValue()
                            ? (short) 1 : (short) 0);
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>INTEGER</code> SQL type. */
        private static final TestSqlType Integer =
            new TestSqlType(INTEGER, "integer") {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return new Integer(((Number) value).intValue());
                    }
                    if (value instanceof Boolean) {
                        return new Integer(((Boolean) value).booleanValue()
                            ? 1 : 0);
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>BIGINT</code> SQL type. */
        private static final TestSqlType Bigint =
            new TestSqlType(BIGINT, "bigint") {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        return new Long(((Number) value).longValue());
                    }
                    if (value instanceof Boolean) {
                        return new Long(((Boolean) value).booleanValue() ? 1 : 0);
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>REAL</code> SQL type. */
        private static final TestSqlType Real =
            new TestSqlType(REAL, "real") {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        // SQL real yields Java float
                        return new Float(((Number) value).floatValue());
                    }
                    if (value instanceof Boolean) {
                        return new Float(((Boolean) value).booleanValue() ? 1 : 0);
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>FLOAT</code> SQL type. */
        private static final TestSqlType Float =
            new TestSqlType(FLOAT, "float") {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        // SQL float yields Java double
                        return new Double(((Number) value).doubleValue());
                    }
                    if (value instanceof Boolean) {
                        return new Double(((Boolean) value).booleanValue() ? 1
                            : 0);
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>DOUBLE</code> SQL type. */
        private static final TestSqlType Double =
            new TestSqlType(DOUBLE, "double") {
                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        // SQL double yields Java double
                        return new Double(((Number) value).doubleValue());
                    }
                    if (value instanceof Boolean) {
                        return new Double(((Boolean) value).booleanValue() ? 1
                            : 0);
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>BOOLEAN</code> SQL type. */
        private static final TestSqlType Boolean =
            new TestSqlType(BOOLEAN, "boolean") {
                public int checkIsValid(Object value)
                {
                    if ((value == null) || value instanceof Boolean) {
                        return VALID;
                    }
                    if (value instanceof Number) {
                        return isBetween((Number) value, 0, 1) ? VALID
                        : OUTOFRANGE;
                    }
                    return INVALID;
                }

                public Object getExpected(Object value)
                {
                    if (value instanceof Number) {
                        // SQL double yields Java double
                        return new Boolean(((Number) value).intValue() != 0);
                    }
                    return super.getExpected(value);
                }
            };

        /* Are we supporting bit/decimal/numeric? see dtbug175 */
        /*"bit", */
        /*"decimal(12,2)", */
        /* If strings too small - will get:
        java: TupleAccessor.cpp:416: void fennel::TupleAccessor::unmarshal(fennel::TupleData&, unsigned int) const: Assertion `value.cbData <= accessor.cbStorage' failed.
        */

        /** Definition of the <code>CHAR(100)</code> SQL type. */
        private static final TestSqlType Char =
            new TestSqlType(CHAR, "char(100)") {
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

        /** Definition of the <code>VARCHAR(200)</code> SQL type. */
        private static final TestSqlType Varchar =
            new TestSqlType(VARCHAR, "varchar(200)") {
                public Object getExpected(Object value)
                {
                    return String.valueOf(value);
                }
            };

        /** Definition of the <code>BINARY(10)</code> SQL type. */
        private static final TestSqlType Binary =
            new TestSqlType(BINARY, "binary(10)") {
                public int checkIsValid(Object value)
                {
                    if (value == null) {
                        return VALID;
                    } else if (value instanceof byte []) {
                        byte [] bytes = (byte []) value;
                        if (bytes.length < 10) {
                            return VALID;
                        } else {
                            return OUTOFRANGE;
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

        /** Definition of the <code>VARBINARY(20)</code> SQL type. */
        private static final TestSqlType Varbinary =
            new TestSqlType(VARBINARY, "varbinary(20)") {
                public int checkIsValid(Object value)
                {
                    if (value == null) {
                        return VALID;
                    } else if (value instanceof byte []) {
                        byte [] bytes = (byte []) value;
                        if (bytes.length < 20) {
                            return VALID;
                        } else {
                            return OUTOFRANGE;
                        }
                    } else {
                        return INVALID;
                    }
                }
            };

        /** Definition of the <code>TIME(0)</code> SQL type. */
        private static final TestSqlType Time =
            new TestSqlType(TIME, "Time(0)") {
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
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>DATE</code> SQL type. */
        private static final TestSqlType Date =
            new TestSqlType(DATE, "Date") {
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
                    }
                    return super.getExpected(value);
                }
            };

        /** Definition of the <code>TIMESTAMP</code> SQL type. */
        private static final TestSqlType Timestamp =
            new TestSqlType(TIMESTAMP, "timestamp(0)") {
                public Object getExpected(Object value)
                {
                    if (value instanceof Timestamp) {
                        return value;
                    } else if (value instanceof java.util.Date) {
                        return new Timestamp(
                            ((java.util.Date) value).getTime());
                    }
                    return super.getExpected(value);
                }
            };
        private static final TestSqlType [] all =
        {
            Tinyint, Smallint, Integer, Bigint, Real, Float, Double, Boolean,
            Char, Varchar, Binary, Varbinary, Time, Date, Timestamp,
        };
        private static final TestSqlType [] typesNumericAndChars =
        {
            Tinyint, Smallint, Integer, Bigint, Real, Float, Double, Char,
            Varchar,
        };
        private static final TestSqlType [] typesBinary = { Binary, Varbinary, };
        public static final int VALID = 0;
        public static final int INVALID = 1;
        public static final int OUTOFRANGE = 2;
        public static String [] validityName =
        { "valid", "invalid", "outofrange" };
        private final int ordinal;
        private final String string;

        TestSqlType(
            int ordinal,
            String example)
        {
            this.ordinal = ordinal;
            this.string = example;
        }

        private static boolean isBetween(
            Number number,
            long min,
            long max)
        {
            long x = number.longValue();
            return (min <= x) && (x <= max);
        }

        public Object getExpected(Object value)
        {
            return value;
        }

        public int checkIsValid(Object value)
        {
            return VALID;
        }
    }

    /**
     * Defines a Java type.
     *
     * <p>Each type has a correponding set of get/set methods, for example
     * "Boolean" has {@link ResultSet#getBoolean(int)} and
     * {@link PreparedStatement#setBoolean(int,boolean)}.
     */
    private static class TestJavaType
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
        // parmaeter, e.g. setAsciiStream(int,InputStream,int length)
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
        /** whether it has a setXxxx(int,xxx) method */
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
                        new Class [] { int.class, clazz });
            } catch (NoSuchMethodException e) {
            } catch (SecurityException e) {
            }
            this.setMethod = method;
        }
    }
}


// End FarragoJdbcTest.java
