/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

import java.math.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * FarragoTestUDR contains definitions for user-defined routines used by tests.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoTestUDR
{
    //~ Methods ----------------------------------------------------------------

    public static String noargs()
    {
        return "get your kicks on route 66";
    }

    public static String substring24(String in)
    {
        try {
            return in.substring(2, 4);
        } catch (NullPointerException ex) {
            // NOTE jvs 21-Dec-2006:  hide the fact that jrockit-R27
            // no longer includes the string "null"
            throw new NullPointerException("null");
        }
    }

    public static String toHexString(int i)
    {
        return Integer.toHexString(i);
    }

    public static String toHexString(Integer i)
    {
        if (i == null) {
            return "nada";
        } else {
            return Integer.toHexString(i.intValue());
        }
    }

    public static BigDecimal decimalAbs(BigDecimal dec)
    {
        if (dec == null) {
            return null;
        } else {
            return dec.abs();
        }
    }

    public static int atoi(String s)
    {
        return Integer.parseInt(s);
    }

    public static Integer atoiWithNullForErr(String s)
    {
        try {
            return new Integer(Integer.parseInt(s));
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    public static void setSystemProperty(String name, String value)
    {
        System.setProperty(name, value);
    }

    public static int accessSql()
    {
        try {
            Connection conn =
                DriverManager.getConnection(
                    "jdbc:default:connection");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("values 777");
            rs.next();

            // NOTE jvs 19-Jan-2005:  no need for cleanup; default connection
            // is cleaned up automatically.
            return rs.getInt(1);
        } catch (SQLException ex) {
            return 0;
        }
    }

    public static String decryptPublicKey(byte [] keyBytes)
    {
        if (keyBytes == null) {
            return null;
        }
        return new String(keyBytes);
    }

    public static int throwSQLException()
        throws SQLException
    {
        throw new SQLException("nothing but a failure");
    }

    public static int throwNPE()
    {
        throw new NullPointerException();
    }

    public static long generateRandomNumber(long seed)
    {
        Random r = (Random) FarragoUdrRuntime.getContext();
        if (r == null) {
            r = new Random(seed);
            FarragoUdrRuntime.setContext(r);
        }
        return r.nextLong();
    }

    public static int gargle()
    {
        Object obj = FarragoUdrRuntime.getContext();
        if (obj == null) {
            ClosableAllocation trigger =
                new ClosableAllocation() {
                    public void closeAllocation()
                    {
                        System.setProperty("feeble", "minded");
                    }
                };
            FarragoUdrRuntime.setContext(trigger);
        }
        return 0;
    }

    public static void ramp(int n, PreparedStatement resultInserter)
        throws SQLException
    {
        for (int i = 0; i < n; ++i) {
            resultInserter.setInt(1, i);
            resultInserter.executeUpdate();
        }
    }

    public static void stringify(
        ResultSet inputSet,
        String delimiter,
        PreparedStatement resultInserter)
        throws SQLException
    {
        generateRows(inputSet, null, delimiter, resultInserter);
    }

    public static void stringifyColumns(
        ResultSet inputSet,
        List<String> columns,
        String delimiter,
        PreparedStatement resultInserter)
        throws SQLException
    {
        generateRows(inputSet, columns, delimiter, resultInserter);
    }

    public static void stringify2ColumnLists(
        List<String> columns1,
        List<String> columns2,
        ResultSet inputSet,
        String delimiter,
        PreparedStatement resultInserter)
        throws SQLException
    {
        columns1.addAll(columns2);
        generateRows(inputSet, columns1, delimiter, resultInserter);
    }

    public static void combineStringifyColumns(
        ResultSet inputSet1,
        List<String> columns1,
        ResultSet inputSet2,
        List<String> columns2,
        String delimiter,
        PreparedStatement resultInserter)
        throws SQLException
    {
        generateRows(inputSet1, columns1, delimiter, resultInserter);
        generateRows(inputSet2, columns2, delimiter, resultInserter);
    }

    public static void combineStringifyColumnsJumbledArgs(
        List<String> columns2,
        ResultSet inputSet1,
        String delimiter,
        ResultSet inputSet2,
        List<String> columns1,
        PreparedStatement resultInserter)
        throws SQLException
    {
        generateRows(inputSet1, columns1, delimiter, resultInserter);
        generateRows(inputSet2, columns2, delimiter, resultInserter);
    }

    private static void generateRows(
        ResultSet inputSet,
        List<String> columns,
        String delimiter,
        PreparedStatement resultInserter)
        throws SQLException
    {
        // Test ParameterMetaData
        assert (resultInserter.getParameterMetaData().getParameterCount() == 1);

        // Also test ResultSetMetaData
        ResultSetMetaData metaData = inputSet.getMetaData();
        int n = metaData.getColumnCount();
        int numGenCols = (columns == null) ? n : columns.size();
        StringBuilder sb = new StringBuilder();
        while (inputSet.next()) {
            sb.setLength(0);
            int currCol = 0;
            for (int i = 1; i <= n; ++i) {
                // exclude columns not contained in the input list, if one is
                // specified
                if ((columns != null)
                    && !columns.contains(metaData.getColumnName(i)))
                {
                    continue;
                }
                sb.append(inputSet.getString(i));
                if (++currCol < numGenCols) {
                    sb.append(delimiter);
                }
            }
            resultInserter.setString(
                1,
                sb.toString());
            resultInserter.executeUpdate();
        }
    }

    public static void badStringifyColumns1(
        ResultSet inputSet,
        List columns,
        String delimiter,
        PreparedStatement resultInserter)
        throws SQLException
    {
    }

    public static void badStringifyColumns2(
        ResultSet inputSet,
        List<Integer> columns,
        String delimiter,
        PreparedStatement resultInserter)
        throws SQLException
    {
    }

    public static void badStringifyColumns3(
        ResultSet inputSet,
        Map<String, Integer> columns,
        String delimiter,
        PreparedStatement resultInserter)
        throws SQLException
    {
    }

    public static void digest(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws SQLException
    {
        int nInput = inputSet.getMetaData().getColumnCount();
        int nOutput = resultInserter.getParameterMetaData().getParameterCount();
        assert (nOutput == (nInput + 1));

        // NOTE jvs 6-Aug-2006: This is just an example.  It's a terrible
        // digest; don't use it for anything real!

        while (inputSet.next()) {
            int digest = 0;
            for (int i = 0; i < nInput; ++i) {
                Object obj = inputSet.getObject(i + 1);
                resultInserter.setObject(i + 1, obj);
                if (obj != null) {
                    digest += obj.hashCode();
                }
            }
            resultInserter.setInt(nInput + 1, digest);
            resultInserter.executeUpdate();
        }
    }

    public static void longerRamp(int n, PreparedStatement resultInserter)
        throws SQLException
    {
        // Let the data server decide how to transform n (as a matter of fact,
        // it will double it).
        Integer nBoxed =
            (Integer) FarragoUdrRuntime.getDataServerRuntimeSupport(
                new Integer(n));
        ramp(
            nBoxed.intValue(),
            resultInserter);
    }

    public static void foreignTime(
        Timestamp ts,
        String tsZoneId,
        String foreignZoneId,
        PreparedStatement resultInserter)
        throws SQLException
    {
        // convert timestamp to the specified time zone
        // (makes this method more useful for testing)
        TimeZone tsZone = TimeZone.getTimeZone(tsZoneId);
        ZonelessTimestamp zts = new ZonelessTimestamp();
        zts.setZonedTime(ts.getTime(), TimeZone.getDefault());
        long millis = zts.getJdbcTimestamp(tsZone);

        // display the time in the foreign time zone
        TimeZone foreignZone = TimeZone.getTimeZone(foreignZoneId);
        Calendar cal = Calendar.getInstance(foreignZone);
        resultInserter.setTimestamp(1, new Timestamp(millis), cal);
        resultInserter.setDate(2, new java.sql.Date(millis), cal);
        resultInserter.setTime(3, new Time(millis), cal);
        resultInserter.executeUpdate();
    }
    
    public static void setSessionVariable(String name, String value)
    throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            
            sess.getSessionVariables().set(name, value);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }
}

// End FarragoTestUDR.java
