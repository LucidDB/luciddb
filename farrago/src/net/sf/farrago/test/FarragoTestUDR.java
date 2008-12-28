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
import net.sf.farrago.syslib.*;
import net.sf.farrago.db.*;
import net.sf.farrago.util.*;

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

    public static String repeat(String s, int n)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; ++i) {
            sb.append(s);
        }
        return sb.toString();
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

    public static String itoa(int i)
    {
        if (i < 0 || i > 0xFFFF) {
            throw new IllegalArgumentException(
                "Cannot convert '" + i + "' to a Java char");
        }
        return new String(new char[] { (char)i }, 0, 1);
    }

    public static String itoaWithNulForErr(int i)
    {
        if (i < 0 || i > 0xFFFF) {
            return null;
        }
        return new String(new char[] { (char)i }, 0, 1);
    }

    public static void setSystemProperty(String name, String value)
    {
        System.setProperty(name, value);
    }

    public static void setFarragoProperty(String name, String value)
    {
        FarragoProperties.instance().setProperty(name, value);
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

    public static void nullableRamp(
        Integer n,
        PreparedStatement resultInserter)
        throws SQLException
    {
        if (n == null) {
            n = new Integer(0);
        }
        ramp(n.intValue(), resultInserter);
    }

    // NOTE jvs 17-Nov-2008:  This one is kind of dangerous,
    // because it can cancel another statement executed after
    // the one which actually invoked the UDX.  You can avoid this
    // by using a dedicated session to invoke the UDX.
    public static void noiseWithCancel(
        long n,
        long seed,
        long cancelDelayMillis,
        PreparedStatement resultInserter)
        throws Exception
    {
        Random r = new Random();
        for (long i = 0; i < n; ++i) {
            resultInserter.setLong(1, r.nextLong());
            resultInserter.executeUpdate();
        }
        final FarragoSession session = FarragoUdrRuntime.getSession();
        Timer timer = new Timer(true);
        TimerTask task = new TimerTask() 
            {
                public void run()
                {
                    session.cancel();
                }
            };
        timer.schedule(task, cancelDelayMillis);
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
        assert (nOutput == (nInput + 1))
            : descibeInputOutput(inputSet, resultInserter);

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

    private static String descibeInputOutput(
        ResultSet inputSet, PreparedStatement resultInserter)
        throws SQLException
    {
        StringBuffer buf = new StringBuffer();
        final ResultSetMetaData resultSetMetaData = inputSet.getMetaData();
        for (int i = 0; i < resultSetMetaData.getColumnCount(); i++) {
            buf.append(" in#").append(i + 1).append("=")
                .append(resultSetMetaData.getColumnName(i + 1));
        }
        final ParameterMetaData parameterMetaData =
            resultInserter.getParameterMetaData();
        for (int i = 0; i < parameterMetaData.getParameterCount(); i++) {
            buf.append(" out#").append(i + 1).append("=")
                .append(parameterMetaData.getParameterClassName(i + 1));
        }
        return buf.toString();
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

    public static void removeDupsFromPresortedCursor(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws SQLException
    {
        int nInput = inputSet.getMetaData().getColumnCount();
        int nOutput = resultInserter.getParameterMetaData().getParameterCount();
        assert (nOutput == nInput);
        boolean first = true;
        Object [] prevRow = new Object[nInput];
        Object [] currRow = new Object[nInput];
        Object [] tmp;
        while (inputSet.next()) {
            for (int i = 0; i < nInput; ++i) {
                currRow[i] = inputSet.getObject(i + 1);
            }
            if (first) {
                // skip comparison on first row, since we have no
                // prev to compare it with yet
                first = false;
            } else {
                for (int i = 0; i < nInput; ++i) {
                    int c =
                        FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                            prevRow[i],
                            currRow[i]);
                    if (c < 0) {
                        // we've seen the start of a new group, so
                        // emit the row for the last one
                        emitUdxRow(resultInserter, prevRow);
                    } else if (c > 0) {
                        // out of order row detected (for a real UDX, this
                        // should have proper i18n)
                        throw new SQLException(
                            "input row not presorted correctly:  "
                            + Arrays.asList(currRow));
                    } else {
                        // rows are equal, so do nothing; keep going
                        // until we see a difference or EOS
                    }
                }
            }
            // swap prev/curr row buffers
            tmp = prevRow;
            prevRow = currRow;
            currRow = tmp;
        }
        if (!first) {
            // unless set was empty, we have one last row buffered;
            // need to emit it now
            emitUdxRow(resultInserter, prevRow);
        }
    }

    private static void emitUdxRow(
        PreparedStatement resultInserter,
        Object [] row)
        throws SQLException
    {
        for (int i = 0; i < row.length; ++i) {
            resultInserter.setObject(i + 1, row[i]);
        }
        resultInserter.executeUpdate();
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

    public static void simulateCatalogRecovery()
        throws Exception
    {
        FarragoSession callerSession = FarragoUdrRuntime.getSession();
        FarragoDatabase db = ((FarragoDbSession) callerSession).getDatabase();
        db.simulateCatalogRecovery();
    }
    
    /**
     * Sets a label within a UDR.  This currently results in an exception.
     * 
     * @param labelName name of the label or null
     */
    public static void setLabel(String labelName)
    throws Exception
    {
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = conn.createStatement();

        if (labelName == null) {
            labelName = "null";
        } else {
            labelName = "'" + labelName + "'";
        }
        stmt.executeUpdate("alter session set \"label\" = " + labelName);
    }
    
    /**
     * Returns some subset of the input columns as specified by the columns
     * parameter.
     * 
     * @param inputSet the input rows
     * @param columns the list of column names that determine which columns from
     * the input to return, as well as the order of the columns
     * @param resultInserter used to return the resulting output
     * 
     * @throws SQLException
     */
    public static void returnInput(
        ResultSet inputSet,
        List<String> columns,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData metaData = inputSet.getMetaData();
        int nInputCols = metaData.getColumnCount();
        
        // First map the source of all the columns that need to be passed back in
        // the result.
        List<Integer> returnColumns = new ArrayList<Integer>();
        buildColumnMap(columns, nInputCols, metaData, returnColumns);
        
        // Then, for each row, retrieve those column values.
        while (inputSet.next()) {
            addOutputColumn(returnColumns, inputSet, resultInserter, 0);
            resultInserter.executeUpdate();
        }
    }
    
    private static void buildColumnMap(
        List<String> columns,
        int nInputCols,
        ResultSetMetaData metaData,
        List<Integer> returnColumns)
        throws SQLException
    {
        for (String column : columns) {
            for (int i = 1; i <= nInputCols; i++) {
                if (metaData.getColumnName(i).equals(column)) {
                    returnColumns.add(i);
                    break;
                }
            }
        }
        assert(returnColumns.size() == columns.size());
    }
    
    private static void addOutputColumn(
        List<Integer> returnColumns,
        ResultSet inputSet,
        PreparedStatement resultInserter,
        int offset)
        throws SQLException
    {
        for (int i = 0; i < returnColumns.size(); i++) {
            Object obj = inputSet.getObject(returnColumns.get(i));
            resultInserter.setObject(offset + i+1, obj);
        }
    }
    
    /**
     * Returns subsets of columns from two inputs.  Both inputs must have the
     * same number of rows, as the subsets of columns will be concatenated side by
     * side into the result rows.  Note that this UDX is NOT doing a join between
     * the two inputs.
     * 
     * @param inputSet1 first set of input rows
     * @param inputSet2 second set of input rows
     * @param columns1 subset of column names from the first input
     * @param columns2 subset of column names from the second input
     * @param resultInserter used to return the resulting output
     * @throws SQLException
     */
    public static void returnTwoInputs(
        ResultSet inputSet1,
        ResultSet inputSet2,
        List<String> columns1,
        List<String> columns2,
        PreparedStatement resultInserter)
        throws SQLException
    {
        ResultSetMetaData metaData1 = inputSet1.getMetaData();
        int nInputCols1 = metaData1.getColumnCount();
        ResultSetMetaData metaData2 = inputSet2.getMetaData();
        int nInputCols2 = metaData2.getColumnCount();
        
        List<Integer> returnColumns1 = new ArrayList<Integer>();
        buildColumnMap(columns1, nInputCols1, metaData1, returnColumns1);
        List<Integer> returnColumns2 = new ArrayList<Integer>();
        buildColumnMap(columns2, nInputCols2, metaData2, returnColumns2);
        
        do {
            if (!inputSet1.next()) {
                assert(!inputSet2.next());
                break;
            } else if (!inputSet2.next()) {
                assert(false);
            }
            addOutputColumn(returnColumns1, inputSet1, resultInserter, 0);
            addOutputColumn(
                returnColumns2, inputSet2, resultInserter, returnColumns1.size());
            resultInserter.executeUpdate();
        } while (true);
    }     
}

// End FarragoTestUDR.java
