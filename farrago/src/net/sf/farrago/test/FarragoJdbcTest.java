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

package net.sf.farrago.test;

import junit.framework.*;

import java.sql.*;
import java.sql.Date;

import java.util.*;

import java.math.BigDecimal;

/**
 * FarragoJdbcTest tests specifics of the Farrago implementation of the JDBC
 * API.  See also unitsql/jdbc/*.sql.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcTest extends FarragoTestCase
{
    private static final boolean dtbug220_fixed = false;
    private static final boolean dtbug110_199_fixed = false;
    private static final boolean dtbug119_fixed = false;
    private static final boolean dtbug199_fixed = false;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoDJbcTest object.
     *
     * @param testName JUnit test name
     *
     * @throws Exception .
     */
    public FarragoJdbcTest(String testName) throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    // implement TestCase
    public static Test suite()
    {
        return wrappedSuite(FarragoJdbcTest.class);
    }

    // NOTE jvs 26-July-2004:  some of the tests in this class modify fixture
    // tables such as SALES.EMPS, but that's OK, because transactions are
    // implicitly rolled back by FarragoTestCase.tearDown.

    public void testDataTypes() throws Exception
    {
        byte minByte = Byte.MIN_VALUE, maxByte = Byte.MAX_VALUE;
        short minShort = Short.MIN_VALUE, maxShort = Short.MAX_VALUE;
        int  minInt = Integer.MIN_VALUE, maxInt = Integer.MAX_VALUE;
        long minLong = Long.MIN_VALUE, maxLong = Long.MAX_VALUE;
        float minFloat = Float.MIN_VALUE, maxFloat = Float.MAX_VALUE;
        double minDouble = Double.MIN_VALUE, maxDouble = Double.MAX_VALUE;
        boolean boolValue = true;
        BigDecimal bigDecimalValue = new BigDecimal(maxDouble);
        String stringValue = "0";
        byte[] bytes = { 127, -34, 56, 29, 56, 49 };

        Calendar cal = Calendar.getInstance() ;
        cal.setTimeZone(TimeZone.getTimeZone("GMT-8"));
        cal.clear();
        cal.set(2004,11,21,12,22,33);
        Time time = new Time(cal.getTime().getTime());
        Date date = new Date(cal.getTime().getTime());
        Timestamp timestamp = new Timestamp(cal.getTime().getTime());

        Byte tinyIntObj = new Byte(minByte);
        Short smallIntObj = new Short(maxShort);
        Integer integerObj = new Integer(minInt);
        Long bigIntObj = new Long(maxLong);
        Float floatObj = new Float(maxFloat);
        Double doubleObj = new Double(maxDouble);
        Boolean boolObj = new Boolean(false);
        BigDecimal decimalObj = new BigDecimal(13412342124143241D);
        String charObj = "CHAR test string";
        String varcharObj = "VARCHAR test string";

        int TINYINT = 2;
        int SMALLINT = 3;
        int INTEGER = 4;
        int BIGINT = 5;
        int REAL = 6;
        int FLOAT = 7;
        int DOUBLE = 8;
        int BOOLEAN = 9;
        int CHAR = 10;
        int VARCHAR = 11;
        int BINARY = 12;
        int VARBINARY = 13;
        int TIME = 14;
        int DATE = 15;
        int TIMESTAMP = 16;

        String dataTypes[] = { "tinyint", "smallint", "integer",  "bigint",
                               "real",    "float",    "double",
                               "boolean",
                               /* Are we supporting bit/decimal/numeric? see dtbug175 */
                               /*"bit", */
                               /*"decimal(12,2)", */

                               /* If strings too small - will get:
                                  java: TupleAccessor.cpp:416: void fennel::TupleAccessor::unmarshal(fennel::TupleData&, unsigned int) const: Assertion `value.cbData <= accessor.cbStorage' failed.
                               */
                               "char(100)", "varchar(200)",

                               "binary(10)", "varbinary(20)",
                               "date", "time(0)", "timestamp(0)"};
        String columnNames[] = new String[dataTypes.length];
        String columnTypeStr = new String();
        String columnStr = new String();
        String paramStr = new String();

        for (int i=0; i < dataTypes.length; i++) {
            columnNames[i] = "\"Column " + (i+1) + ": " + dataTypes[i] + "\"";
            columnTypeStr += columnNames[i] + " " + dataTypes[i];
            columnStr += columnNames[i];
            paramStr += "?";
            if (i < dataTypes.length-1) {
                columnTypeStr += ", ";
                columnStr += ", ";
                paramStr += ", ";
            }
        }
        columnTypeStr = "id integer primary key, " + columnTypeStr;
        columnStr = "id, " + columnStr;
        paramStr = "( ?, " + paramStr + ")";
        stmt.executeUpdate("create schema datatypes_schema");
        stmt.executeUpdate("create table datatypes_schema.dataTypes_table ("
                           + columnTypeStr + ")");

        String ins_query = "insert into datatypes_schema.dataTypes_table values ";
        String query = ins_query + paramStr;

        preparedStmt = connection.prepareStatement(query);
        int res;
        for (int i = 0; i <= 19; i++)
        {
            preparedStmt.setInt(1, i+100);

            switch (i)
            {
            case 0:
                // Skipped: dtbug220
                // for (int j=2; j<=javaSqlTypes.length; j++)
                for (int j=CHAR; j<=VARBINARY; j++)
                    preparedStmt.setString(j, stringValue);
                break;
            case 1:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setByte(j, minByte);
                break;
            case 2:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setByte(j, maxByte);
                break;
            case 3:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setShort(j, minShort);
                break;
            case 4:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setShort(j, maxShort);
                break;
            case 5:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setInt(j, minInt);
                break;
            case 6:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setInt(j, maxInt);
                break;
            case 7:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setLong(j, minLong);
                break;
            case 8:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setLong(j, maxLong);
                break;
            case 9:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setFloat(j, minFloat);
                break;
            case 10:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setFloat(j, maxFloat);
                break;
            case 11:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setDouble(j, minDouble);
                break;
            case 12:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setDouble(j, maxDouble);
                break;
            case 13:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setBoolean(j, boolValue);
                break;
            case 14:
                for (int j = TINYINT; j<=VARCHAR; j++)
                    preparedStmt.setBigDecimal(j, bigDecimalValue);
                break;
            case 15:
                preparedStmt.setBytes(BINARY, bytes);
                preparedStmt.setBytes(VARBINARY, bytes);
                break;
            case 16:
                for (int j = CHAR; j<=VARCHAR; j++)
                    preparedStmt.setDate(j, date);
                // dtbug199
                //preparedStmt.setDate(DATE, date)   ;
                //preparedStmt.setDate(TIMESTAMP, date);
                break;
            case 17:
                for (int j = CHAR; j<=VARCHAR; j++)
                    preparedStmt.setTime(j, time);
                // dtbug199
                //preparedStmt.setTime(TIME, time);
                //preparedStmt.setTime(TIMESTAMP, time);
                break;
            case 18:
                for (int j = CHAR; j<=VARCHAR; j++)
                    preparedStmt.setTimestamp(j, timestamp);
                // dtbug199
                //for (int j = DATE; j<=TIMESTAMP; j++)
                //    preparedStmt.setTimestamp(j, timestamp);
                break;
            case 19:
                preparedStmt.setObject(TINYINT, tinyIntObj);
                preparedStmt.setObject(SMALLINT, smallIntObj);
                preparedStmt.setObject(INTEGER, integerObj);
                preparedStmt.setObject(BIGINT, bigIntObj);
                preparedStmt.setObject(REAL, floatObj);
                preparedStmt.setObject(FLOAT, doubleObj);
                preparedStmt.setObject(DOUBLE, doubleObj);
                preparedStmt.setObject(BOOLEAN, boolObj);
                preparedStmt.setObject(CHAR, charObj);
                preparedStmt.setObject(VARCHAR, varcharObj);
                preparedStmt.setObject(BINARY, bytes);
                preparedStmt.setObject(VARBINARY, bytes);
                //preparedStmt.setObject(DATE, date);
                //preparedStmt.setObject(TIME, time);
                //preparedStmt.setObject(TIMESTAMP, timestamp);
                break;

            default:
                assert false;
                break;
            }

            res = preparedStmt.executeUpdate();
            assertEquals(1, res);
        }

        query = "select " + columnStr + " from datatypes_schema.datatypes_table";

        preparedStmt = connection.prepareStatement(query);

        resultSet = preparedStmt.executeQuery();
        int id;
        while (resultSet.next())
        {
            id = resultSet.getInt(1);
            switch (id)
            {
            case 100:
                if (dtbug220_fixed) {
                    /* Numerics and timestamps skipped due to dtbug220 */
                    assertEquals(stringValue, resultSet.getString(TINYINT));
                    assertEquals(stringValue, resultSet.getString(SMALLINT));
                    assertEquals(stringValue, resultSet.getString(INTEGER));
                    assertEquals(stringValue, resultSet.getString(BIGINT));
                    assertEquals(stringValue, resultSet.getString(REAL));
                    assertEquals(stringValue, resultSet.getString(FLOAT));
                    assertEquals(stringValue, resultSet.getString(DOUBLE));
                    assertEquals(stringValue, resultSet.getString(BOOLEAN));
                }

                // Check CHAR - result String can be longer than the input string
                // Just check the first part
                assertEquals(stringValue,
                             resultSet.getString(CHAR).substring(0, stringValue.length()));
                assertEquals(stringValue, resultSet.getString(VARCHAR));

                // What should BINARY/VARBINARY be?
                //assertEquals(stringValue, resultSet.getString(BINARY));
                //assertEquals(stringValue, resultSet.getString(VARBINARY));

                // dtbug110, 199
                if (dtbug110_199_fixed) {
                    assertEquals(stringValue, resultSet.getString(DATE));
                    assertEquals(stringValue, resultSet.getString(TIME));
                    assertEquals(stringValue, resultSet.getString(TIMESTAMP));
                }
                break;

            case 101:
                assertEquals(minByte, resultSet.getByte(TINYINT));
                assertEquals(minByte, resultSet.getByte(SMALLINT));
                assertEquals(minByte, resultSet.getByte(INTEGER));
                assertEquals(minByte, resultSet.getByte(BIGINT));
                assertEquals(minByte, resultSet.getByte(REAL));
                assertEquals(minByte, resultSet.getByte(FLOAT));
                assertEquals(minByte, resultSet.getByte(DOUBLE));
                assertEquals(1, resultSet.getByte(BOOLEAN));
                assertEquals(minByte, resultSet.getByte(CHAR));
                assertEquals(minByte, resultSet.getByte(VARCHAR));
                break;

            case 102:
                assertEquals(maxByte, resultSet.getByte(TINYINT));
                assertEquals(maxByte, resultSet.getByte(SMALLINT));
                assertEquals(maxByte, resultSet.getByte(INTEGER));
                assertEquals(maxByte, resultSet.getByte(BIGINT));
                assertEquals(maxByte, resultSet.getByte(REAL));
                assertEquals(maxByte, resultSet.getByte(FLOAT));
                assertEquals(maxByte, resultSet.getByte(DOUBLE));
                assertEquals(1, resultSet.getByte(BOOLEAN));
                assertEquals(maxByte, resultSet.getByte(CHAR));
                assertEquals(maxByte, resultSet.getByte(VARCHAR));
                break;

            case 103:
                assertEquals(0, resultSet.getShort(TINYINT));
                assertEquals(minShort, resultSet.getShort(SMALLINT));
                assertEquals(minShort, resultSet.getShort(INTEGER));
                assertEquals(minShort, resultSet.getShort(BIGINT));
                assertEquals(minShort, resultSet.getShort(REAL));
                assertEquals(minShort, resultSet.getShort(FLOAT));
                assertEquals(minShort, resultSet.getShort(DOUBLE));
                assertEquals(1, resultSet.getShort(BOOLEAN));
                assertEquals(minShort, resultSet.getShort(CHAR));
                assertEquals(minShort, resultSet.getShort(VARCHAR));
                break;

            case 104:
                assertEquals(-1, resultSet.getShort(TINYINT));
                assertEquals(maxShort, resultSet.getShort(SMALLINT));
                assertEquals(maxShort, resultSet.getShort(INTEGER));
                assertEquals(maxShort, resultSet.getShort(BIGINT));
                assertEquals(maxShort, resultSet.getShort(REAL));
                assertEquals(maxShort, resultSet.getShort(FLOAT));
                assertEquals(maxShort, resultSet.getShort(DOUBLE));
                assertEquals(1, resultSet.getShort(BOOLEAN));
                assertEquals(maxShort, resultSet.getShort(CHAR));
                assertEquals(maxShort, resultSet.getShort(VARCHAR));
                break;

            case 105:
                assertEquals(0, resultSet.getInt(TINYINT));
                assertEquals(0, resultSet.getInt(SMALLINT));
                assertEquals(minInt, resultSet.getInt(INTEGER));
                assertEquals(minInt, resultSet.getInt(BIGINT));
                assertEquals(minInt, resultSet.getInt(REAL));
                assertEquals(minInt, resultSet.getInt(FLOAT));
                assertEquals(minInt, resultSet.getInt(DOUBLE));
                assertEquals(1, resultSet.getInt(BOOLEAN));
                assertEquals(minInt, resultSet.getInt(CHAR));
                assertEquals(minInt, resultSet.getInt(VARCHAR));
                break;

            case 106:
                assertEquals(-1, resultSet.getInt(TINYINT));
                assertEquals(-1, resultSet.getInt(SMALLINT));
                assertEquals(maxInt, resultSet.getInt(INTEGER));
                assertEquals(maxInt, resultSet.getInt(BIGINT));
                assertEquals(-2147483648, resultSet.getInt(REAL));
                assertEquals(maxInt, resultSet.getInt(FLOAT));
                assertEquals(maxInt, resultSet.getInt(DOUBLE));
                assertEquals(1, resultSet.getInt(BOOLEAN));
                assertEquals(maxInt, resultSet.getInt(CHAR));
                assertEquals(maxInt, resultSet.getInt(VARCHAR));
                break;

            case 107:
                assertEquals(0, resultSet.getLong(TINYINT));
                assertEquals(0, resultSet.getLong(SMALLINT));
                assertEquals(0, resultSet.getLong(INTEGER));
                assertEquals(minLong, resultSet.getLong(BIGINT));
                assertEquals(minLong, resultSet.getLong(REAL));
                assertEquals(minLong, resultSet.getLong(FLOAT));
                assertEquals(minLong, resultSet.getLong(DOUBLE));
                assertEquals(1, resultSet.getLong(BOOLEAN));
                assertEquals(minLong, resultSet.getLong(CHAR));
                assertEquals(minLong, resultSet.getLong(VARCHAR));
                break;

            case 108:
                assertEquals(-1, resultSet.getLong(TINYINT));
                assertEquals(-1, resultSet.getLong(SMALLINT));
                assertEquals(-1, resultSet.getLong(INTEGER));
                assertEquals(maxLong, resultSet.getLong(BIGINT));
                assertEquals(maxLong, resultSet.getLong(REAL));
                assertEquals(maxLong, resultSet.getLong(FLOAT));
                assertEquals(maxLong, resultSet.getLong(DOUBLE));
                assertEquals(1, resultSet.getLong(BOOLEAN));
                assertEquals(maxLong, resultSet.getLong(CHAR));
                assertEquals(maxLong, resultSet.getLong(VARCHAR));
                break;

            case 109:
                assertEquals(0, resultSet.getFloat(TINYINT), 0);
                assertEquals(0, resultSet.getFloat(SMALLINT), 0);
                assertEquals(0, resultSet.getFloat(INTEGER), 0);
                assertEquals(0, resultSet.getFloat(BIGINT), 0);
                assertEquals(minFloat, resultSet.getFloat(REAL), 0);
                assertEquals(minFloat, resultSet.getFloat(FLOAT), 0);
                assertEquals(minFloat, resultSet.getFloat(DOUBLE), 0);
                assertEquals(1, resultSet.getFloat(BOOLEAN),1);
                assertEquals(minFloat, resultSet.getFloat(CHAR), 0);
                assertEquals(minFloat, resultSet.getFloat(VARCHAR), 0);
                break;

            case 110:
                assertEquals(-1, resultSet.getFloat(TINYINT), 0);
                assertEquals(-1, resultSet.getFloat(SMALLINT), 0);
                assertEquals(maxInt, resultSet.getFloat(INTEGER), 0);
                assertEquals(maxLong, resultSet.getFloat(BIGINT), 0);
                assertEquals(maxFloat, resultSet.getFloat(REAL), 0);
                assertEquals(maxFloat, resultSet.getFloat(FLOAT), 0);
                assertEquals(maxFloat, resultSet.getFloat(DOUBLE), 0);
                assertEquals(1, resultSet.getFloat(BOOLEAN),1);
                assertEquals(maxFloat, resultSet.getFloat(CHAR), 0);
                assertEquals(maxFloat, resultSet.getFloat(VARCHAR), 0);
                break;

            case 111:
                assertEquals(0, resultSet.getDouble(TINYINT), 0);
                assertEquals(0, resultSet.getDouble(SMALLINT), 0);
                assertEquals(0, resultSet.getDouble(INTEGER), 0);
                assertEquals(0, resultSet.getDouble(BIGINT), 0);
                assertEquals(0, resultSet.getDouble(REAL), 0);
                assertEquals(minDouble, resultSet.getDouble(FLOAT), 0);
                assertEquals(minDouble, resultSet.getDouble(DOUBLE), 0);
                assertEquals(1, resultSet.getDouble(BOOLEAN),1);
                assertEquals(minDouble, resultSet.getDouble(CHAR), 0);
                assertEquals(minDouble, resultSet.getDouble(VARCHAR), 0);
                break;

            case 112:
                assertEquals(-1, resultSet.getDouble(TINYINT), 0);
                assertEquals(-1, resultSet.getDouble(SMALLINT), 0);
                assertEquals(maxInt, resultSet.getDouble(INTEGER), 0);
                assertEquals(maxLong, resultSet.getDouble(BIGINT), 0);
                assertEquals(Float.POSITIVE_INFINITY, resultSet.getDouble(REAL), 0);
                assertEquals(maxDouble, resultSet.getDouble(FLOAT), 0);
                assertEquals(maxDouble, resultSet.getDouble(DOUBLE), 0);
                assertEquals(1, resultSet.getDouble(BOOLEAN),1);
                assertEquals(maxDouble, resultSet.getDouble(CHAR), 0);
                assertEquals(maxDouble, resultSet.getDouble(VARCHAR), 0);
                break;

            case 113:
                assertEquals(boolValue, resultSet.getBoolean(TINYINT));
                assertEquals(boolValue, resultSet.getBoolean(SMALLINT));
                assertEquals(boolValue, resultSet.getBoolean(INTEGER));
                assertEquals(boolValue, resultSet.getBoolean(BIGINT));
                assertEquals(boolValue, resultSet.getBoolean(REAL));
                assertEquals(boolValue, resultSet.getBoolean(FLOAT));
                assertEquals(boolValue, resultSet.getBoolean(DOUBLE));
                assertEquals(boolValue, resultSet.getBoolean(BOOLEAN));
                    //assertEquals(boolValue, resultSet.getBoolean(CHAR));
        		    //can not convert String (true) to long exception
                    //assertEquals(boolValue, resultSet.getBoolean(VARCHAR));
                break;

            case 114:
                // dtbug119
                if (dtbug119_fixed) {
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(TINYINT));
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(SMALLINT));
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(INTEGER));
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(BIGINT));
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(REAL));
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(FLOAT));
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(DOUBLE));
                    //todo:
                    //assertEquals(bigDecimalValue, resultSet.getBigDecimal(BigDecimal));
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(CHAR));
                    assertEquals(bigDecimalValue, resultSet.getBigDecimal(VARCHAR));
                }
                break;

            case 115:
                // Check BINARY - resBytes can be longer than the input bytes
                byte[] resBytes = resultSet.getBytes(BINARY);
                assertNotNull(resBytes);
                for (int i=0; i<bytes.length; i++)
                    assertEquals(bytes[i], resBytes[i]);

                // Check VARBINARY - should be same length
                resBytes = resultSet.getBytes(VARBINARY);
                // resBytes null for some reason
                /*assertNotNull(resBytes);
                  assertEquals(bytes.length, resBytes.length);
                  for (int i=0; i<bytes.length; i++)
                  assertEquals(bytes[i], resBytes[i]); */
                break;

            case 116:
                if (dtbug199_fixed) {
                    assertEquals(date, resultSet.getDate(CHAR));
                    assertEquals(date, resultSet.getDate(VARCHAR));
                    assertEquals(date, resultSet.getDate(DATE));
                    assertEquals(date, resultSet.getDate(TIMESTAMP));
                }
                break;

            case 117:
                if (dtbug199_fixed) {
                    assertEquals(time, resultSet.getTime(CHAR));
                    assertEquals(time, resultSet.getTime(VARCHAR));
                    assertEquals(time, resultSet.getTime(TIME));
                    assertEquals(time, resultSet.getTime(TIMESTAMP));
                }
                break;

            case 118:
                if (dtbug199_fixed) {
                    assertEquals(timestamp, resultSet.getTimestamp(CHAR));
                    assertEquals(timestamp, resultSet.getTimestamp(VARCHAR));
                    assertEquals(timestamp, resultSet.getTimestamp(DATE));
                    assertEquals(timestamp, resultSet.getTimestamp(TIME));
                    assertEquals(timestamp, resultSet.getTimestamp(TIMESTAMP));
                }
                break;

            case 119:
                assertEquals(tinyIntObj, resultSet.getObject(TINYINT));
                assertEquals(smallIntObj, resultSet.getObject(SMALLINT));
                assertEquals(integerObj, resultSet.getObject(INTEGER));
                assertEquals(bigIntObj, resultSet.getObject(BIGINT));
                assertEquals(floatObj, resultSet.getObject(REAL));
                assertEquals(doubleObj, resultSet.getObject(FLOAT));
                assertEquals(doubleObj, resultSet.getObject(DOUBLE));
                assertEquals(boolObj, resultSet.getObject(BOOLEAN));

                // Check CHAR - result String can be longer than the input string
                // Just check the first part
                assertEquals(charObj,
                             resultSet.getString(CHAR).substring(0, charObj.length()));
                assertEquals(varcharObj, resultSet.getObject(VARCHAR));

                // Check BINARY - resBytes can be longer than the input bytes
                resBytes = (byte[]) resultSet.getObject(BINARY);
                assertNotNull(resBytes);
                for (int i=0; i<bytes.length; i++)
                    assertEquals(bytes[i], resBytes[i]);

                // Check VARBINARY - should be same length
                resBytes = (byte[]) resultSet.getObject(VARBINARY);
                assertNotNull(resBytes);
                assertEquals(bytes.length, resBytes.length);
                for (int i=0; i<bytes.length; i++)
                    assertEquals(bytes[i], resBytes[i]);

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
     * Test sql date/time to java.sql date/time translations.
     */
    public void testDateTimeSql() throws Exception
    {
        String dateSql = "select DATE '2004-12-21', TIME '12:22:33'," + "" +
            " TIMESTAMP '2004-12-21 12:22:33' from values('true')";
        preparedStmt = connection.prepareStatement(dateSql);
        resultSet  = preparedStmt.executeQuery();

        if (resultSet.next()) {

            Date date = resultSet.getDate(1);
            Timestamp tstamp = resultSet.getTimestamp(3);

            Calendar cal = Calendar.getInstance() ;
            cal.setTimeZone(TimeZone.getTimeZone("GMT-8"));
            cal.clear();
            cal.set(2004,11,21); //month is zero based.  idiots ...
            assert date.getTime() == cal.getTime().getTime() ;

            cal.set(2004,11,21,12,22,33);

            assert tstamp.getTime() == cal.getTime().getTime();
        } else assert false : "Static query returned no rows?";

        resultSet.close();

        /*
        resultSet = stmt.executeQuery(dateSql);

        while (resultSet.next()) {
            Calendar cal = Calendar.getInstance() ;
            cal.setTimeZone(TimeZone.getTimeZone("GMT-6"));
            //  not supported by IteratorResultSet yet.
            Date date = resultSet.getDate(1, cal);
            Timestamp tstamp = resultSet.getTimestamp(3, cal);


            cal.setTimeZone(TimeZone.getTimeZone("GMT-8"));
            cal.clear();
            cal.set(2004,11,21); //month is zero based.  idiots ...
            assert date.getTime() == cal.getTime().getTime() ;

            cal.set(2004,11,21,12,22,33);

            assert tstamp.getTime() == cal.getTime().getTime();
        }
        */

    }


    /**
     * Test setQueryTimeout
     *
     * @throws Exception .
     */
    public void testTimeout() throws Exception
    {
        String sql = "select * from sales.emps";
        preparedStmt = connection.prepareStatement(sql);
        for (int i = 10; i >= -2; i--) {
            preparedStmt.setQueryTimeout(i);
            resultSet = preparedStmt.executeQuery();
            if (catalog.isFennelEnabled()) {
                assertEquals(4,getResultSetCount());
            } else {
                assertEquals(0,getResultSetCount());
            }
            resultSet.close();
        }

        sql = "select empid from sales.emps where name=?";
        preparedStmt = connection.prepareStatement(sql);
        ParameterMetaData pmd = preparedStmt.getParameterMetaData();
        assertEquals(
            1,pmd.getParameterCount());
        assertEquals(
            128,pmd.getPrecision(1));
        assertEquals(
            Types.VARCHAR,pmd.getParameterType(1));
        assertEquals(
            "VARCHAR",pmd.getParameterTypeName(1));

        preparedStmt.setString(1,"Wilma");
        preparedStmt.setQueryTimeout(5);
        resultSet = preparedStmt.executeQuery();
        if (catalog.isFennelEnabled()) {
            compareResultSet(Collections.singleton("1"));
        }
        preparedStmt.setString(1,"Eric");
        preparedStmt.setQueryTimeout(3);
        resultSet = preparedStmt.executeQuery();
        if (catalog.isFennelEnabled()) {
            compareResultSet(Collections.singleton("3"));
        }
    }


    /**
     * Test char and varchar Data Type in JDBC
     *
     * @throws Exception .
     */
    public void testChar() throws Exception
    {
        byte gender = 1;
        byte city = -128;
        short gender2 = 2;
        short city2 = 32767;
        int gender3 = 3;
        int city3 = -234234;
        long gender4 = 4L;
        long city4 = 123432432432545455L;
        float gender5 = 5.0F;
        float city5 = 6.02e+23F;
        double gender6 = 6.2;
        double city6 = 3.14;
        BigDecimal gender7 = new BigDecimal(2);
        BigDecimal city7 = new BigDecimal(88.23432432);
        boolean gender8 = false;
        boolean city8 = true;
        String gender9 = "F";
        String city9 = "San Jose";

        Object genderx = new Character('M');
        // alternative:
        //Object genderx = new Integer(1);
        Object cityx = new StringBuffer("New York");

        String name = "JDBC Test Char";

        String query = "insert into \"SALES\".\"EMPS\" values ";
        query += "(?, ?, 10, ?, ?, ?, 28, NULL, NULL, false)";

        preparedStmt = connection.prepareStatement(query);
        int res;
        for (int i = 1; i <= 10; i++)
        {
            preparedStmt.setInt(1, i+1000);
            preparedStmt.setString(2, name + i);
            switch (i)
            {
            case 1:
                preparedStmt.setByte(3, gender);
                preparedStmt.setByte(4, city);
                break;
            case 2:
                preparedStmt.setShort(3, gender2);
                preparedStmt.setShort(4, city2);
                break;
            case 3:
                preparedStmt.setInt(3, gender3);
                preparedStmt.setInt(4, city3);
                break;
            case 4:
                preparedStmt.setLong(3, gender4);
                preparedStmt.setLong(4, city4);
                break;
            case 5:
                preparedStmt.setFloat(3, gender5);
                preparedStmt.setFloat(4, city5);
                break;
            case 6:
                preparedStmt.setDouble(3, gender6);
                preparedStmt.setDouble(4, city6);
                break;
            case 7:
                preparedStmt.setBigDecimal(3, gender7);
                preparedStmt.setBigDecimal(4, city7);
                break;
            case 8:
                preparedStmt.setBoolean(3, gender8);
                preparedStmt.setBoolean(4, city8);
                break;
            case 9:
                preparedStmt.setString(3, gender9);
                preparedStmt.setString(4, city9);
                break;
            case 10:
                preparedStmt.setObject(3, genderx);
                preparedStmt.setObject(4, cityx);
                break;
            default:
                assertEquals(1,2);
                break;
            }
            preparedStmt.setInt(5, i+100);
            res = preparedStmt.executeUpdate();
            assertEquals(1, res);
        }

        // this query won't find everything we insert above because of bugs in
        // executeUpdate when there's a setFloat/setDouble called on a varchar
        // column.  See comments in bug#117
        //query = "select gender, city, empid from sales.emps where name like '";
        //query += name + "%'";
        // for now, use this query instead
        query = "select gender, city, empid from sales.emps where empno>1000";

        preparedStmt = connection.prepareStatement(query);

        resultSet = preparedStmt.executeQuery();
        int empid;
        while (resultSet.next())
        {
            empid = resultSet.getInt(3);
            switch (empid)
            {
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
                assertEquals(gender8, resultSet.getBoolean(1));
                // ERROR:  same as bug #117
                // assertEquals(city8, resultSet.getBoolean(2));
                break;
            case 109:
                assertEquals(gender9, resultSet.getString(1));
                assertEquals(city9, resultSet.getString(2));
                break;
            case 110:
                // ERROR - the actual result looks correct, probably merely object type not matching
                // assertEquals(genderx, resultSet.getObject(1));
           	    // assertEquals(cityx, resultSet.getObject(2));
                break;
            default:
                // uncomment this when I can do a like sql query
                assertEquals(1,2);
                break;
            }
        }

        resultSet.close();
        resultSet = null;
    }

    /**
     * Test integer Data Type in JDBC
     *
     * @throws Exception .
     */
    public void testInteger() throws Exception
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

        query = "select empno, deptno, empid, age from sales.emps where name = ?";
        preparedStmt = connection.prepareStatement(query);
        preparedStmt.setString(1, name);

        resultSet = preparedStmt.executeQuery();
        while (resultSet.next())
        {
            assertEquals(empno, resultSet.getShort(1));
            assertEquals(deptno, resultSet.getByte(2));
            assertEquals(empid, resultSet.getInt(3));
            assertEquals(age, resultSet.getLong(4));
        }

        resultSet.close();

        preparedStmt.setString(1, name2);
        resultSet = preparedStmt.executeQuery();
        while (resultSet.next())
        {
            assertEquals(666, resultSet.getFloat(1), 0.0001);
            assertEquals(deptno2, resultSet.getDouble(2), 0.0001);
            // ERROR: Bug#119: getBigDecimal not supported
            // assertEquals(empid2, resultSet.getBigDecimal(3));
            // ERROR: Bug#117: getBoolean returns SQLException like getXXX on char data types (where XXX is numeric type)
            // assertEquals(age2, resultSet.getBoolean(4));
        }

        resultSet.close();

        preparedStmt.setString(1, name3);
        resultSet = preparedStmt.executeQuery();
        while (resultSet.next())
        {
            assertEquals(empno3, resultSet.getString(1));
            assertEquals(deptno3, resultSet.getObject(2));
        }

        resultSet.close();

        resultSet = null;
    }


    /**
     * Test re-execution of a prepared query.
     *
     * @throws Exception .
     */
    public void testPreparedQuery() throws Exception
    {
        String sql = "select * from sales.emps";
        preparedStmt = connection.prepareStatement(sql);
        for (int i = 0; i < 5; ++i) {
            resultSet = preparedStmt.executeQuery();
            if (catalog.isFennelEnabled()) {
                assertEquals(4,getResultSetCount());
            } else {
                assertEquals(0,getResultSetCount());
            }
            resultSet.close();
            resultSet = null;
        }
    }

    /**
     * Test re-execution of an unprepared query.  There's no black-box way to
     * verify that caching is working, but if it is, this will at least
     * exercise it.
     *
     * @throws Exception .
     */
    public void testCachedQuery() throws Exception
    {
        repeatQuery();
    }

    /**
     * Test re-execution of an unprepared query with statement caching
     * disabled.
     *
     * @throws Exception .
     */
    public void testUncachedQuery() throws Exception
    {
        // disable caching
        stmt.execute("alter system set \"codeCacheMaxBytes\" = min");
        repeatQuery();

        // re-enable caching
        stmt.execute("alter system set \"codeCacheMaxBytes\" = max");
    }

    private void repeatQuery() throws Exception
    {
        String sql = "select * from sales.emps";
        for (int i = 0; i < 3; ++i) {
            resultSet = stmt.executeQuery(sql);
            if (catalog.isFennelEnabled()) {
                assertEquals(4,getResultSetCount());
            } else {
                assertEquals(0,getResultSetCount());
            }
            resultSet.close();
            resultSet = null;
        }
    }

    /**
     * Test retrieval of ResultSetMetaData without actually executing query.
     */
    public void testPreparedMetaData() throws Exception
    {
        String sql = "select name from sales.emps";
        preparedStmt = connection.prepareStatement(sql);
        ResultSetMetaData metaData = preparedStmt.getMetaData();
        assertEquals(1,metaData.getColumnCount());
        assertTrue(metaData.isSearchable(1));
        assertEquals(
            ResultSetMetaData.columnNoNulls,metaData.isNullable(1));
        assertEquals("NAME",metaData.getColumnName(1));
        assertEquals(128,metaData.getPrecision(1));
        assertEquals(Types.VARCHAR,metaData.getColumnType(1));
        assertEquals("VARCHAR",metaData.getColumnTypeName(1));
    }

    // TODO:  re-execute DDL, DML

    /**
     * Test valid usage of a dynamic parameter and retrieval of associated
     * metadata.
     */
    public void testDynamicParameter() throws Exception
    {
        String sql = "select empid from sales.emps where name=?";
        preparedStmt = connection.prepareStatement(sql);
        ParameterMetaData pmd = preparedStmt.getParameterMetaData();
        assertEquals(
            1,pmd.getParameterCount());
        assertEquals(
            128,pmd.getPrecision(1));
        assertEquals(
            Types.VARCHAR,pmd.getParameterType(1));
        assertEquals(
            "VARCHAR",pmd.getParameterTypeName(1));

        preparedStmt.setString(1,"Wilma");
        resultSet = preparedStmt.executeQuery();
        if (catalog.isFennelEnabled()) {
            compareResultSet(Collections.singleton("1"));
        }
        preparedStmt.setString(1,"Eric");
        resultSet = preparedStmt.executeQuery();
        if (catalog.isFennelEnabled()) {
            compareResultSet(Collections.singleton("3"));
        }
        preparedStmt.setString(1,"George");
        resultSet = preparedStmt.executeQuery();
        assertEquals(0,getResultSetCount());
        preparedStmt.setString(1,null);
        resultSet = preparedStmt.executeQuery();
        assertEquals(0,getResultSetCount());
    }

    /**
     * Test metadata for dynamic parameter in an UPDATE statement.
     */
    public void testDynamicParameterInUpdate() throws Exception
    {
        String sql = "update sales.emps set age = ?";
        preparedStmt = connection.prepareStatement(sql);
        ParameterMetaData pmd = preparedStmt.getParameterMetaData();
        assertEquals(
            1,pmd.getParameterCount());
        assertEquals(
            Types.INTEGER,pmd.getParameterType(1));
        assertEquals(
            "INTEGER",pmd.getParameterTypeName(1));
    }

    /**
     * Test invalid usage of a dynamic parameter.
     */
    public void testInvalidDynamicParameter() throws Exception
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
     * Test invalid attempt to execute a statement with a dynamic parameter
     * without preparation.
     */
    public void testDynamicParameterExecuteImmediate() throws Exception
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
}

// End FarragoJdbcTest.java
