/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.sql;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.fun.SqlTrimFunction;
import org.eigenbase.sql.parser.ParserPosition;
import org.eigenbase.sql.test.SqlOperatorTests;
import org.eigenbase.sql.test.SqlTester;
import org.eigenbase.sql.type.CallOperands;
import org.eigenbase.util.Util;

import java.util.HashMap;

import java.sql.*;

/**
 * A <code>SqlJdbcFunctionCall</code> is a node of a parse tree which
 * represents a JDBC function call. A JDBC call is of the form
 * <code>{fn NAME(arg0, arg1, ...)}</code>.
 *
 * <p>See <a href="http://java.sun.com/products/jdbc/driverdevs.html">Sun's
 * documentation for writers of JDBC drivers</a>.
 *
 *  * <table>
 * <tr>
 * <th>Function Name</th>
 * <th>Function Returns</th>
 * </tr>
 *
 * <tr><td colspan="2"><br/><h3>NUMERIC FUNCTIONS</h3></td></tr>
 *
 * <tr><td>ABS(number)</td>
 * 		   <td>Absolute value of number</td></tr>
 * <tr><td>ACOS(float)</td>
 * 		   <td>Arccosine, in radians, of float</td></tr>
 * <tr><td>ASIN(float)</td>
 * 		   <td>Arcsine, in radians, of float</td></tr>
 * <tr><td>ATAN(float)</td>
 * 		   <td>Arctangent, in radians, of float</td></tr>
 * <tr><td>ATAN2(float1, float2)</td>
 * 		   <td>Arctangent, in radians, of float2 / float1</td></tr>
 * <tr><td>CEILING(number)</td>
 * 		   <td>Smallest integer &gt;= number</td></tr>
 * <tr><td>COS(float)</td>
 * 		   <td>Cosine of float radians</td></tr>
 * <tr><td>COT(float)</td>
 * 		   <td>Cotangent of float radians</td></tr>
 * <tr><td>DEGREES(number)</td>
 * 		   <td>Degrees in number radians</td></tr>
 * <tr><td>EXP(float)</td>
 * 		   <td>Exponential function of float</td></tr>
 * <tr><td>FLOOR(number)</td>
 * 		   <td>Largest integer &lt;= number</td></tr>
 * <tr><td>LOG(float)</td>
 * 		   <td>Base e logarithm of float</td></tr>
 * <tr><td>LOG10(float)</td>
 * 		   <td>Base 10 logarithm of float</td></tr>
 * <tr><td>MOD(integer1, integer2)</td>
 * 		   <td>Rh3ainder for integer1 / integer2</td></tr>
 * <tr><td>PI()</td>
 * 		   <td>The constant pi</td></tr>
 * <tr><td>POWER(number, power)</td>
 * 		   <td>number raised to (integer) power</td></tr>
 * <tr><td>RADIANS(number)</td>
 * 		   <td>Radians in number degrees</td></tr>
 * <tr><td>RAND(integer)</td>
 * 		   <td>Random floating point for seed integer</td></tr>
 * <tr><td>ROUND(number, places)</td>
 * 		   <td>number rounded to places places</td></tr>
 * <tr><td>SIGN(number)</td>
 * 		   <td>-1 to indicate number is &lt; 0;
 * 		   0 to indicate number is = 0;
 * 		   1 to indicate number is &gt; 0</td></tr>
 * <tr><td>SIN(float)</td>
 * 		   <td>Sine of float radians</td></tr>
 * <tr><td>SQRT(float)</td>
 * 		   <td>Square root of float</td></tr>
 * <tr><td>TAN(float)</td>
 * 		   <td>Tangent of float radians</td></tr>
 * <tr><td>TRUNCATE(number, places)</td>
 * 		   <td>number truncated to places places</td></tr>
 *
 * <tr><td colspan="2"><br/><h3>STRING FUNCTIONS</h3></td></tr>
 *
 * <tr><td>ASCII(string)</td>
 * 		   <td>Integer representing the ASCII code value of the
 * 		   leftmost character in string</td></tr>
 * <tr><td>CHAR(code)</td>
 * 		   <td>Character with ASCII code value code, where
 * 		   code is between 0 and 255</td></tr>
 * <tr><td>CONCAT(string1, string2)</td>
 * 		   <td>Character string formed by appending string2
 * 		   to string1; if a string is null, the result is
 * 		   DBMS-dependent</td></tr>
 * <tr><td>DIFFERENCE(string1, string2)</td>
 * 		   <td>Integer indicating the difference between the
 * 		   values returned by the function SOUNDEX for
 * 		   string1 and string2</td></tr>
 * <tr><td>INSERT(string1, start, length, string2)</td>
 * 		   <td>A character string formed by deleting length
 * 		   characters from string1 beginning at start,
 * 		   and inserting string2 into string1 at start</td></tr>
 * <tr><td>LCASE(string)</td>
 * 		   <td>Converts all uppercase characters in string to
 * 		   lowercase</td></tr>
 * <tr><td>LEFT(string, count)</td>
 * 		   <td>The count leftmost characters from string</td></tr>
 * <tr><td>LENGTH(string)</td>
 * 		   <td>Number of characters in string, excluding trailing
 * 		   blanks</td></tr>
 * <tr><td>LOCATE(string1, string2[, start])</td>
 * 		   <td>Position in string2 of the first occurrence of
 * 		   string1, searching from the beginning of
 * 		   string2; if start is specified, the search begins
 * 		   from position start. 0 is returned if string2
 * 		   does not contain string1. Position 1 is the first
 * 		   character in string2.</td></tr>
 * <tr><td>LTRIM(string)</td>
 * 		   <td>Characters of string with leading blank spaces
 * 		   rh3oved</td></tr>
 * <tr><td>REPEAT(string, count)</td>
 * 		   <td>A character string formed by repeating string
 * 		   count times</td></tr>
 * <tr><td>REPLACE(string1, string2, string3)</td>
 * 		   <td>Replaces all occurrences of string2 in string1
 * 		   with string3</td></tr>
 * <tr><td>RIGHT(string, count)</td>
 * 		   <td>The count rightmost characters in string </td></tr>
 * <tr><td>RTRIM(string)</td>
 * 		   <td>The characters of string with no trailing blanks</td></tr>
 * <tr><td>SOUNDEX(string)</td>
 * 		   <td>A character string, which is data source-dependent,
 * 		   representing the sound of the words in
 * 		   string; this could be a four-digit SOUNDEX
 * 		   code, a phonetic representation of each word,
 * 		   etc.</td></tr>
 * <tr><td>SPACE(count)</td>
 * 		   <td>A character string consisting of count spaces</td></tr>
 * <tr><td>SUBSTRING(string, start, length)</td>
 * 		   <td>A character string formed by extracting length
 * 		   characters from string beginning at start </td></tr>
 * <tr><td>UCASE(string)</td><td>Converts all lowercase characters in string to
 * 		   uppercase </td></tr>
 *
 * <tr><td colspan="2"><br/><h3>TIME and DATE FUNCTIONS</h3></td></tr>
 *
 * <tr><td>CURDATE()</td>
 * 		   <td>The current date as a date value</td></tr>
 * <tr><td>CURTIME()</td>
 * 		   <td>The current local time as a time value</td></tr>
 * <tr><td>DAYNAME(date)</td>
 * 		   <td>A character string representing the day component
 * 		   of date; the name for the day is specific to
 * 		   the data source</td></tr>
 * <tr><td>DAYOFMONTH(date)</td>
 * 		   <td>An integer from 1 to 31 representing the day of
 * 		   the month in date</td></tr>
 * <tr><td>DAYOFWEEK(date)</td>
 * 		   <td>An integer from 1 to 7 representing the day of
 * 		   the week in date; 1 represents Sunday</td></tr>
 * <tr><td>DAYOFYEAR(date)</td>
 * 		   <td>An integer from 1 to 366 representing the day of
 * 		   the year in date</td></tr>
 * <tr><td>HOUR(time)</td>
 * 		   <td>An integer from 0 to 23 representing the hour
 * 		   component of time</td></tr>
 * <tr><td>MINUTE(time)</td>
 * 		   <td>An integer from 0 to 59 representing the minute
 * 		   component of time</td></tr>
 * <tr><td>MONTH(date)</td>
 * 		   <td>An integer from 1 to 12 representing the month
 * 		   component of date</td></tr>
 * <tr><td>MONTHNAME(date)</td>
 * 		   <td>A character string representing the month component
 * 		   of date; the name for the month is specific
 * 		   to the data source</td></tr>
 * <tr><td>NOW()</td>
 * 		   <td>A timestamp value representing the current date
 * 		   and time</td></tr>
 * <tr><td>QUARTER(date)</td>
 * 		   <td>An integer from 1 to 4 representing the quarter
 * 		   in date; 1 represents January 1 through March
 * 		   31</td></tr>
 * <tr><td>SECOND(time)</td>
 * 		   <td>An integer from 0 to 59 representing the second
 * 		   component of time</td></tr>
 * <tr><td>TIMESTAMPADD(interval,count, timestamp)</td>
 * 		   <td>A timestamp calculated by adding count
 * 		   number of interval(s) to timestamp; interval may
 * 		   be one of the following: SQL_TSI_FRAC_SECOND,
 * 		   SQL_TSI_SECOND, SQL_TSI_MINUTE,
 * 		   SQL_TSI_HOUR, SQL_TSI_DAY, SQL_TSI_WEEK,
 * 		   SQL_TSI_MONTH, SQL_TSI_QUARTER, or
 * 		   SQL_TSI_YEAR</td></tr>
 * <tr><td>TIMESTAMPDIFF(interval,timestamp1, timestamp2)</td>
 * 		   <td>An integer representing the number of
 * 		   interval(s) by which timestamp2 is greater than
 * 		   timestamp1; interval may be one of the following:
 * 		   SQL_TSI_FRAC_SECOND, SQL_TSI_SECOND,
 * 		   SQL_TSI_MINUTE, SQL_TSI_HOUR, SQL_TSI_DAY,
 * 		   SQL_TSI_WEEK, SQL_TSI_MONTH,
 * 		   SQL_TSI_QUARTER, or SQL_TSI_YEAR</td></tr>
 * <tr><td>WEEK(date)</td>
 * 		   <td>An integer from 1 to 53 representing the week of
 * 		   the year in date</td></tr>
 * <tr><td>YEAR(date)</td>
 * 		   <td>An integer representing the year component of
 * 		   date</td></tr>
 *
 * <tr><td colspan="2"><br/><h3>SYSTEM FUNCTIONS</h3></td></tr>
 *
 * <tr><td>DATABASE()</td>
 * 		   <td>Name of the database</td></tr>
 * <tr><td>IFNULL(expression, value)</td>
 * 		   <td>value if expression is null;
 * 		   expression if expression is not null</td></tr>
 * <tr><td>USER()</td>
 * 		   <td>User name in the DBMS
 *
 * <tr><td colspan="2"><br/><h3>CONVERSION FUNCTIONS</h3></td></tr>
 *
 * <tr><td>CONVERT(value, SQLtype)</td>
 * 		   <td>value converted to SQLtype where SQLtype may
 * 		   be one of the following SQL types:
 * 		   BIGINT, BINARY, BIT, CHAR, DATE, DECIMAL, DOUBLE,
 * 		   FLOAT, INTEGER, LONGVARBINARY, LONGVARCHAR,
 * 		   REAL, SMALLINT, TIME, TIMESTAMP,
 * 		   TINYINT, VARBINARY, or VARCHAR</td></tr>
 *
 * </table>
 *
 * @author Wael Chatila
 * @since June 28, 2004
 * @version $Id$
 **/
public class SqlJdbcFunctionCall extends SqlFunction
{
    private static final String numericFunctions;
    private static final String stringFunctions;
    private static final String timeDateFunctions;
    private static final String systemFunctions;

    /**
     * List of all numeric function names defined by JDBC.
     */
    private static final String [] allNumericFunctions = {
        "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEILING", "COS", "COT",
        "DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "MOD", "PI",
        "POWER", "RADIANS", "RAND", "ROUND", "SIGN", "SIN", "SQRT",
        "TAN", "TRUNCATE"
    };

    /**
     * List of all string function names defined by JDBC.
     */
    private static final String [] allStringFunctions = {
        "ASCII", "CHAR", "CONCAT", "DIFFERENCE", "INSERT", "LCASE",
        "LEFT", "LENGTH", "LOCATE", "LTRIM", "REPEAT", "REPLACE",
        "RIGHT", "RTRIM", "SOUNDEX", "SPACE", "SUBSTRING", "UCASE"
    };

    /**
     * List of all time/date function names defined by JDBC.
     */
    private static final String [] allTimeDateFunctions = {
        "CURDATE", "CURTIME", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK",
        "DAYOFYEAR", "HOUR", "MINUTE", "MONTH", "MONTHNAME", "NOW",
        "QUARTER", "SECOND", "TIMESTAMPADD", "TIMESTAMPDIFF",
        "WEEK", "YEAR"
    };

    /**
     * List of all system function names defined by JDBC.
     */
    private static final String [] allSystemFunctions = {
        "DATABASE", "IFNULL", "USER"
    };

    //~ Instance fields -------------------------------------------------------

    String jdbcName;
    MakeCall lookupMakeCallObj;
    private SqlCall lookupCall;

    //    private SqlCall thisCall;
    SqlNode [] thisOperands;

    static 
    {
        numericFunctions = constructFuncList(allNumericFunctions);
        stringFunctions = constructFuncList(allStringFunctions);
        timeDateFunctions = constructFuncList(allTimeDateFunctions);
        systemFunctions = constructFuncList(allSystemFunctions);
    }
    
    private static String constructFuncList(String [] functionNames)
    {
        StringBuffer sb = new StringBuffer();
        boolean first = true;
        for (int i = 0; i < functionNames.length; ++i) {
            String funcName = functionNames[i];
            if (JdbcToInternalLookupTable.instance.lookup(funcName) == null) {
                continue;
            }
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(funcName);
        }
        return sb.toString();
    }

    //~ Constructors ----------------------------------------------------------

    public SqlJdbcFunctionCall(String name)
    {
        super("{fn "+name+"}", SqlKind.JdbcFn, null, null, null, null);
        jdbcName = name;
        lookupMakeCallObj = JdbcToInternalLookupTable.instance.lookup(name);
        lookupCall = null;
    }

    //~ Methods ---------------------------------------------------------------

    public SqlCall createCall(
        SqlNode [] operands,
        ParserPosition pos)
    {
        thisOperands = operands;
        return super.createCall(operands, pos);
    }

    public void test(SqlTester tester)
    {
        SqlOperatorTests.testJdbcFn(tester);
    }

    protected boolean checkArgTypes(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope, boolean throwOnFailure)
    {
        // no op, arg checking is done in getType
        return true;
    }

    public SqlCall getLookupCall()
    {
        if (null == lookupCall) {
            lookupCall = lookupMakeCallObj.createCall(thisOperands, null);
        }
        return lookupCall;
    }

    public String getAllowedSignatures()
    {
        return lookupMakeCallObj.operator.getAllowedSignatures(name);
    }

    public SqlOperator.OperandsCountDescriptor getOperandsCountDescriptor()
    {
        return OperandsCountDescriptor.variadicCountDescriptor;
    }

    protected RelDataType getType(
        SqlValidator validator,
        SqlValidator.Scope scope,
        RelDataTypeFactory typeFactory,
        CallOperands callOperands)
    {
        if (null == lookupMakeCallObj) {
            // only expected to come here if validator called this method
            throw validator.newValidationError(
                (SqlCall) callOperands.getUnderlyingObject(),
                    EigenbaseResource.instance().newFunctionUndefined(name));
        }

        if (!lookupMakeCallObj.checkNumberOfArg(callOperands.size())) {
            // only expected to come here if validator called this method
            throw validator.newValidationError(
                (SqlCall) callOperands.getUnderlyingObject(),
                    EigenbaseResource.instance().newWrongNumberOfParam(
                    name,
                    new Integer(thisOperands.length),
                    getArgCountMismatchMsg()));
        }

        if (!lookupMakeCallObj.operator.checkArgTypes(
                    getLookupCall(),
                    validator,
                    scope,
                    false)) {
            // only expected to come here if validator called this method
            SqlCall call = (SqlCall) callOperands.getUnderlyingObject();
            throw call.newValidationSignatureError(validator, scope);
        }
        return lookupMakeCallObj.operator.getType(
            validator,
            scope,
            getLookupCall());
    }

    private String getArgCountMismatchMsg()
    {
        StringBuffer ret = new StringBuffer();
        int [] possible = lookupMakeCallObj.getPossibleArgCounts();
        for (int i = 0; i < possible.length; i++) {
            if (i > 0) {
                ret.append(" or ");
            }
            ret.append(possible[i]);
        }
        ret.append(" parameter(s)");
        return ret.toString();
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.print("{fn ");
        writer.print(jdbcName);
        writer.print("(");
        for (int i = 0; i < operands.length; i++) {
            if (i > 0) {
                writer.print(", ");
            }
            operands[i].unparse(writer, leftPrec, rightPrec);
        }
        writer.print(") }");
    }

    /**
     * @see DatabaseMetaData#getNumericFunctions
     */
    public static String getNumericFunctions()
    {
        return numericFunctions;
    }

    /**
     * @see DatabaseMetaData#getStringFunctions
     */
    public static String getStringFunctions()
    {
        return stringFunctions;
    }
    
    /**
     * @see DatabaseMetaData#getTimeDateFunctions
     */
    public static String getTimeDateFunctions()
    {
        return timeDateFunctions;
    }

    /**
     * @see DatabaseMetaData#getSystemFunctions
     */
    public static String getSystemFunctions()
    {
        return systemFunctions;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Represent a Strategy Object to create a {@link SqlCall} by providing
     * the feature of reording, adding/dropping operands.
     */
    private static class MakeCall
    {
        final SqlOperator operator;
        final int [] order;
        /**
         * List of the possible numbers of operands this function can take.
         */
        final int [] argCounts;

        private MakeCall(
            SqlOperator operator,
            int argCount)
        {
            this.operator = operator;
            this.order = null;
            this.argCounts = new int [] { argCount };
        }

        /**
         * Creates a MakeCall strategy object with reordering of operands.
         *
         * <p>The reordering is specified by an int array where the value of
         * element at position <code>i</code> indicates to which element in a
         * new SqlNode[] array the operand goes.
         *
         * @pre order != null
         * @pre order[i] < order.length
         * @pre order.length > 0
         * @pre argCounts == order.length
         * @param operator
         * @param order
         */
        MakeCall(
            SqlOperator operator,
            int argCount,
            int [] order)
        {
            Util.pre(null != order, "null!=order");
            Util.pre(order.length > 0, "order.length > 0");
            // Currently operation overloading when reordering is necessary is
            // NOT implemented
            Util.pre(argCount == order.length, "argCounts==order.length");
            this.operator = operator;
            this.order = order;
            this.argCounts = new int [] { order.length };

            // sanity checking ...
            for (int i = 0; i < order.length; i++) {
                Util.pre(order[i] < order.length, "order[i] < order.length");
            }
        }

        final int [] getPossibleArgCounts()
        {
            return this.argCounts;
        }

        /**
         * Uses the data in {@link #order} to reorder a SqlNode[] array.
         * @param operands
         */
        protected SqlNode [] reorder(SqlNode [] operands)
        {
            assert (operands.length == order.length);
            SqlNode [] newOrder = new SqlNode[operands.length];
            for (int i = 0; i < operands.length; i++) {
                assert operands[i] != null;
                int joyDivision = order[i];
                assert newOrder[joyDivision] == null : "mapping is not 1:1";
                newOrder[joyDivision] = operands[i];
            }
            return newOrder;
        }

        /**
         * Creates and return a {@link SqlCall}. If the MakeCall strategy object
         * was created with a reording specified the call will be created with
         * the operands reordered, otherwise no change of ordering is applied
         * @param operands
         */
        SqlCall createCall(
            SqlNode [] operands,
            ParserPosition pos)
        {
            if (null == order) {
                return operator.createCall(operands, pos);
            }
            return operator.createCall(
                reorder(operands),
                pos);
        }

        /**
         * Returns false if number of arguments are unexpected, otherwise true.
         * This function is supposed to be called with an {@link SqlNode} array
         * of operands direct from the oven, e.g no reording or adding/dropping
         * of operands...else it would make much sense to have this methods
         */
        boolean checkNumberOfArg(int length)
        {
            for (int i = 0; i < argCounts.length; i++) {
                if (argCounts[i] == length) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Lookup table between JDBC functions and internal representation
     */
    private static class JdbcToInternalLookupTable
    {
        private final HashMap map = new HashMap();

        private JdbcToInternalLookupTable()
        {
            SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();
            // A table of all functions can be found at
            // http://java.sun.com/products/jdbc/driverdevs.html
            // which is also provided in the javadoc for this class.
            map.put(
                "ABS",
                new MakeCall(opTab.absFunc, 1));
            map.put(
                "LOG",
                new MakeCall(opTab.lnFunc, 1));
            map.put(
                "LOG10",
                new MakeCall(opTab.logFunc, 1));
            map.put(
                "MOD",
                new MakeCall(opTab.modFunc, 2));
            map.put(
                "POWER",
                new MakeCall(opTab.powFunc, 2));

            map.put(
                "CONCAT",
                new MakeCall(opTab.concatOperator, 2));
            map.put(
                "INSERT",
                new MakeCall(
                    opTab.overlayFunc,
                    4,
                    new int [] { 0, 2, 3, 1 }));
            map.put(
                "LCASE",
                new MakeCall(opTab.lowerFunc, 1));
            map.put(
                "LENGTH",
                new MakeCall(opTab.characterLengthFunc, 1));
            map.put(
                "LOCATE",
                new MakeCall(opTab.positionFunc, 2));
            map.put(
                "LTRIM",
                new MakeCall(opTab.trimFunc, 1) {
                    SqlCall createCall(SqlNode [] operands)
                    {
                        assert (null != operands);
                        assert (1 == operands.length);
                        SqlNode [] newOperands = new SqlNode[3];
                        newOperands[0] =
                            SqlTrimFunction.Flag.createLeading(null);
                        newOperands[1] =
                            SqlLiteral.createCharString(" ", null);
                        newOperands[2] = operands[0];

                        return super.createCall(newOperands, null);
                    }
                });
            map.put(
                "RTRIM",
                new MakeCall(opTab.trimFunc, 1) {
                    SqlCall createCall(SqlNode [] operands)
                    {
                        assert (null != operands);
                        assert (1 == operands.length);
                        SqlNode [] newOperands = new SqlNode[3];
                        newOperands[0] =
                            SqlTrimFunction.Flag.createTrailing(null);
                        newOperands[1] =
                            SqlLiteral.createCharString(" ", null);
                        newOperands[2] = operands[0];

                        return super.createCall(newOperands, null);
                    }
                });
            map.put(
                "SUBSTRING",
                new MakeCall(opTab.substringFunc, 3));
            map.put(
                "UCASE",
                new MakeCall(opTab.upperFunc, 0));

            map.put(
                "CURDATE",
                new MakeCall(opTab.currentDateFunc, 0));
            map.put(
                "CURTIME",
                new MakeCall(opTab.localTimeFunc, 0));
            map.put(
                "NOW",
                new MakeCall(opTab.currentTimestampFunc, 0));
        }

        /**
         * Tries to lookup a given function name JDBC to an internal
         * representation. Returns null if no function defined.
         */
        public MakeCall lookup(String name)
        {
            return (MakeCall) map.get(name);
        }

        /**
         * The {@link org.eigenbase.util.Glossary#SingletonPattern singleton}
         * instance.
         */
        static final JdbcToInternalLookupTable instance =
            new JdbcToInternalLookupTable();
    }
}

// End SqlJdbcFunctionCall.java
