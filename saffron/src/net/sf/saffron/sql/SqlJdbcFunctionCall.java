/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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
package net.sf.saffron.sql;

import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.sql.fun.SqlStdOperatorTable;
import net.sf.saffron.sql.fun.SqlTrimFunction;
import net.sf.saffron.sql.parser.ParserPosition;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.util.Util;

import java.util.HashMap;

/**
 * A <code>SqlJdbcFunctionCall</code> is a node of a parse tree which
 * represents a JDBC function call. A JDBC call is on the form
 * <code>{fn NAME(arg0, arg1, ...)}</code>
 * @author Wael Chatila
 * @since June 28, 2004
 * @version $Id$
 **/
public class SqlJdbcFunctionCall extends SqlFunction
{
    String jdbcName;
    MakeCall lookupMakeCallObj;
    private SqlCall lookupCall;
//    private SqlCall thisCall;
    SqlNode[] thisOperands;

    public SqlCall createCall(SqlNode[] operands, ParserPosition parserPosition) {
        thisOperands = operands;
        return super.createCall(operands, parserPosition);
    }

    public SqlJdbcFunctionCall(String name) {
        super("{fn "+name+"}", null, null, null);
        jdbcName=name;
        lookupMakeCallObj = JdbcToInteralLookupTable.lookup(name);
        lookupCall=null;
    }

    public void test(SqlTester tester) {
        /* empty implementation */
    }

    public SqlCall getLookupCall() {
        if (null==lookupCall) {
            lookupCall = lookupMakeCallObj.createCall(thisOperands, null);
        }
        return lookupCall;
    }

    public String getAllowedSignatures() {
        return lookupMakeCallObj.operator.getAllowedSignatures(name);
    }

    public int getNumOfOperands(int desiredCount) {
        return thisOperands.length;
    }

    public SaffronType getType(SqlValidator validator, SqlValidator.Scope scope,
                               SqlCall call) {
        if (null==lookupMakeCallObj) {
            //todo add position data
            throw validator.newValidationError("Function "+name+" is not defined");
        }

        if (!lookupMakeCallObj.checkNumberOfArg(call.operands.length)) {
            //todo add position data
            throw validator.newValidationError(getNumOfArgMismatchMsg());
        }

        if (!lookupMakeCallObj.operator.checkArgTypesNoThrow(getLookupCall(),
                validator, scope)){
            //todo add position data
            throw call.newValidationSignatureError(validator, scope);
        }
        return lookupMakeCallObj.operator.getType(
                validator, scope, getLookupCall());
    }

    public SaffronType getType(SaffronTypeFactory typeFactory, SaffronType[] argTypes) {
        return lookupMakeCallObj.operator.getType(typeFactory, argTypes);
    }

    private String getNumOfArgMismatchMsg() {
        StringBuffer ret = new StringBuffer();
        ret.append("Encountered ");
        ret.append(name);
        ret.append(" with ");
        ret.append(thisOperands.length);
        ret.append(" parameter(s), was expecting ");
        int[] possible = lookupMakeCallObj.getPossibleNumOfOperands();
        for (int i = 0; i < possible.length; i++) {
            if (i>0) {
                ret.append(" or ");
            }
            ret.append(possible[i]);
        }
        ret.append(" parameter(s)");
        return ret.toString();
    }

    public void unparse(
            SqlWriter writer,
            SqlNode[] operands,
            int leftPrec,
            int rightPrec) {
        writer.print("{fn ");
        writer.print(jdbcName);
        writer.print("(");
        for (int i = 0; i < operands.length; i++) {
            if (i>0) {
                writer.print(", ");
            }
            operands[i].unparse(writer,leftPrec,rightPrec);
        }
        writer.print(") }");
    }

    /**
     * Represent a Strategy Object to create a {@link SqlCall} by providing
     * the feature of reording, adding/dropping operands.
     */
    private static class MakeCall {
        final SqlOperator operator;
        final int[]  order;
        final int[] numOfArgs;

        MakeCall(SqlOperator operator, int numOfArgs) {
            this.operator=operator;
            this.order = null;
            this.numOfArgs = new int[]{numOfArgs};
        }

        MakeCall(SqlOperator operator, int[] numOfArgs) {
            this.operator=operator;
            this.order = null;
            this.numOfArgs = numOfArgs;
        }

        /**
         * Creates a MakeCall strategy object with reording of operands.
         * The reordering is specified by an int array where value of element
         * at position <code>i</code> indicates to which element in a new SqlNode[]
         * array the operand goes.
         * @pre order!=null
         * @pre order[i] < order.length
         * @pre order.length > 0
         * @pre numOfArgs==order.length (currently operation overloading when
         *                               reording is necessary is NOT implemented)
         * @param operator
         * @param order
         */
        MakeCall(SqlOperator operator, final int numOfArgs, int[] order) {
            Util.pre(null!=order,"null!=order");
            Util.pre(order.length > 0,"order.length > 0");
            Util.pre(numOfArgs==order.length,"numOfArgs==order.length");
            this.operator=operator;
            this.order=order;
            this.numOfArgs = new int[]{order.length};

            // sanity checking ...
            for (int i = 0; i < order.length; i++) {
                Util.pre(order[i] < order.length,"order[i] < order.length");
            }
        }

        final int[] getPossibleNumOfOperands() {
            return this.numOfArgs;
        }

        /**
         * Uses the data in {@link this.order} to reorder a SqlNode[] array.
         * @param operands
         */
        protected SqlNode[] reorder(SqlNode[] operands) {
            assert(operands.length==order.length);
            SqlNode[] newOrder = new SqlNode[operands.length];
            for (int i = 0; i < operands.length; i++) {
                newOrder[order[i]] = operands[i];
            }
            return newOrder;
        }

        /**
         * Creates and return a {@link SqlCall}. If the MakeCall strategy object
         * was created with a reording specified the call will be created with
         * the operands reordered, otherwise no change of ordering is applied
         * @param operands
         */
        SqlCall createCall(SqlNode[] operands, ParserPosition parserPosition) {
            if (null==order) {
                return operator.createCall(operands, parserPosition);
            }
            return operator.createCall(reorder(operands), parserPosition);
        }

        /**
         * Returns false if number of arguments are unexpected, otherwise true.
         * This function is supposed to be called with an {@link SqlNode} array
         * of operands direct from the oven, e.g no reording or adding/dropping
         * of operands...else it would make much sense to have this methods
         */
        boolean checkNumberOfArg(int length) {
            for (int i = 0; i < numOfArgs.length; i++) {
                if (numOfArgs[i]==length) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Lookup table between JDBC functions and internal representation
     */
    private static class JdbcToInteralLookupTable {
        private static final HashMap map;
        static {
            SqlStdOperatorTable opTab = SqlOperatorTable.std();
            map = new HashMap();
            // A table of all functions can be found at http://java.sun.com/products/jdbc/driverdevs.html
            // which is also provided at the end of this file.

            //~ SCALARS -------
            map.put("ABS", new MakeCall(opTab.absFunc,1));
            map.put("LOG", new MakeCall(opTab.lnFunc,1));
            map.put("LOG10", new MakeCall(opTab.logFunc,1));
            map.put("MOD", new MakeCall(opTab.modFunc,2));
            map.put("POWER", new MakeCall(opTab.powFunc,2));
            //~ STRING FUNS -------
            map.put("CONCAT", new MakeCall(opTab.concatOperator,2));
            map.put("INSERT", new MakeCall(opTab.overlayFunc,4, new int[]{0,2,3,1}));
            map.put("LCASE", new MakeCall(opTab.lowerFunc,1));
            map.put("LENGTH", new MakeCall(opTab.characterLengthFunc,1));
            map.put("LOCATE", new MakeCall(opTab.positionFunc,new int[]{2}));
            map.put("LTRIM", new MakeCall(opTab.trimFunc,1) {
                SqlCall createCall(SqlNode[] operands) {
                    assert(null!=operands);
                    assert(1==operands.length);
                    SqlNode[] newOperands = new SqlNode[3];
                    newOperands[0] = SqlTrimFunction.Flag.createLeading(null);
                    newOperands[1] = SqlLiteral.createString("'\\t\\r\\n\\f '", null, null);
                    newOperands[2] = operands[0];

                    return super.createCall(newOperands, null);
                }
            });
            map.put("RTRIM", new MakeCall(opTab.trimFunc,1) {
                SqlCall createCall(SqlNode[] operands) {
                    assert(null!=operands);
                    assert(1==operands.length);
                    SqlNode[] newOperands = new SqlNode[3];
                    newOperands[0] = SqlTrimFunction.Flag.createTrailing(null);
                    newOperands[1] = SqlLiteral.createString("'\\t\\r\\n\\f '", null,null);
                    newOperands[2] = operands[0];

                    return super.createCall(newOperands,null);
                }
            });
            map.put("SUBSTRING", new MakeCall(opTab.substringFunc,3));
            map.put("UCASE", new MakeCall(opTab.upperFunc,0));
            //~ TIME FUNS -------
            map.put("CURDATE", new MakeCall(opTab.currentDateFunc,0));
            map.put("CURTIME", new MakeCall(opTab.localTimeFunc,0));
            map.put("NOW", new MakeCall(opTab.currentTimestampFunc,0));
            //~ SYSTEM FUNS -------
            //~ CONVERSION FUNS -------

        }

        /**
         * Tries to lookup a given function name JDBC to an internal
         * representation. Returns null if no function defined.
         */
        public static MakeCall lookup(String name) {
            return (MakeCall) map.get(name);
        }
    }
}


// This table can be found at http://java.sun.com/products/jdbc/driverdevs.html
//NUMERIC FUNCTIONS
//
//Function Name            Function Returns
//
//ABS(number)              Absolute value of number
//ACOS(float)              Arccosine, in radians, of float
//ASIN(float)              Arcsine, in radians, of float
//ATAN(float)              Arctangent, in radians, of float
//ATAN2(float1, float2)    Arctangent, in radians, of float2 / float1
//CEILING(number)	         Smallest integer >= number
//COS(float)               Cosine of float radians
//COT(float)               Cotangent of float radians
//DEGREES(number)	         Degrees in number radians
//EXP(float)               Exponential function of float
//FLOOR(number)	         Largest integer <= number
//LOG(float)               Base e logarithm of float
//LOG10(float)	         Base 10 logarithm of float
//MOD(integer1, integer2)	 Remainder for integer1 / integer2
//PI()	                 The constant pi
//POWER(number, power)	 number raised to (integer) power
//RADIANS(number)	         Radians in number degrees
//RAND(integer)	         Random floating point for seed integer
//ROUND(number, places)	 number rounded to places places
//SIGN(number)	         -1 to indicate number is < 0;
//                         0 to indicate number is = 0;
//                         1 to indicate number is > 0
//SIN(float)               Sine of float radians
//SQRT(float)              Square root of float
//TAN(float)               Tangent of float radians
//TRUNCATE(number, places) number truncated to places places
//
//STRING FUNCTIONS
//
//
//Function Name                Function Returns
//
//ASCII(string)                Integer representing the ASCII code value of the
//                             leftmost character in string
//CHAR(code)                   Character with ASCII code value code, where
//                             code is between 0 and 255
//CONCAT(string1, string2)     Character string formed by appending string2
//                             to string1; if a string is null, the result is
//                             DBMS-dependent
//DIFFERENCE(string1, string2) Integer indicating the difference between the
//                             values returned by the function SOUNDEX for
//                             string1 and string2
//INSERT(string1, start, 	     A character string formed by deleting length
//length, string2)             characters from string1 beginning at start,
//                             and inserting string2 into string1 at start
//LCASE(string)                Converts all uppercase characters in string to
//                             lowercase
//LEFT(string, count)          The count leftmost characters from string
//LENGTH(string)               Number of characters in string, excluding trailing
//                             blanks
//LOCATE(string1,              Position in string2 of the first occurrence of
//string2[, start])            string1, searching from the beginning of
//                             string2; if start is specified, the search begins
//                             from position start. 0 is returned if string2
//                             does not contain string1. Position 1 is the first
//                             character in string2.
//LTRIM(string)                Characters of string with leading blank spaces
//                             removed
//REPEAT(string, count)        A character string formed by repeating string
//                             count times
//REPLACE(string1, string2,    Replaces all occurrences of string2 in string1
//string3) 	                 with string3
//RIGHT(string, count)         The count rightmost characters in string
//RTRIM(string)                The characters of string with no trailing blanks
//SOUNDEX(string)              A character string, which is data source-dependent,
//                             representing the sound of the words in
//                             string; this could be a four-digit SOUNDEX
//                             code, a phonetic representation of each word,
//                             etc.
//SPACE(count)                 A character string consisting of count spaces
//SUBSTRING(string, start,     A character string formed by extracting length
//length)                      characters from string beginning at start
//UCASE(string)                Converts all lowercase characters in string to
//                             uppercase
//
//
//TIME and DATE FUNCTIONS
//Function Name            Function Returns
//
//CURDATE()                The current date as a date value
//CURTIME()                The current local time as a time value
//DAYNAME(date)            A character string representing the day component
//                         of date; the name for the day is specific to
//                         the data source
//DAYOFMONTH(date)         An integer from 1 to 31 representing the day of
//                         the month in date
//DAYOFWEEK(date)          An integer from 1 to 7 representing the day of
//                         the week in date; 1 represents Sunday
//DAYOFYEAR(date)          An integer from 1 to 366 representing the day of
//                         the year in date
//HOUR(time)               An integer from 0 to 23 representing the hour
//                         component of time
//MINUTE(time)             An integer from 0 to 59 representing the minute
//                         component of time
//MONTH(date)              An integer from 1 to 12 representing the month
//                         component of date
//MONTHNAME(date)          A character string representing the month component
//                         of date; the name for the month is specific
//                         to the data source
//NOW()                    A timestamp value representing the current date
//                         and time
//QUARTER(date)            An integer from 1 to 4 representing the quarter
//                         in date; 1 represents January 1 through March
//                         31
//SECOND(time)             An integer from 0 to 59 representing the second
//                         component of time
//TIMESTAMPADD(interval,   A timestamp calculated by adding count num-
//count, timestamp)	     ber of interval(s) to timestamp; interval may
//                         be one of the following: SQL_TSI_FRAC_SECOND,
//                         SQL_TSI_SECOND, SQL_TSI_MINUTE,
//                         SQL_TSI_HOUR, SQL_TSI_DAY, SQL_TSI_WEEK,
//                         SQL_TSI_MONTH, SQL_TSI_QUARTER, or
//                         SQL_TSI_YEAR
//TIMESTAMPDIFF(interval,  An integer representing the number of inter-
//timestamp1, timestamp2)	 val(s) by which timestamp2 is greater than
//                         timestamp1; interval may be one of the following:
//                         SQL_TSI_FRAC_SECOND, SQL_TSI_SECOND,
//                         SQL_TSI_MINUTE, SQL_TSI_HOUR, SQL_TSI_DAY,
//                         SQL_TSI_WEEK, SQL_TSI_MONTH,
//                         SQL_TSI_QUARTER, or SQL_TSI_YEAR
//WEEK(date)               An integer from 1 to 53 representing the week of
//                         the year in date
//YEAR(date)               An integer representing the year component of
//                         date
//
//
//
//
//SYSTEM FUNCTIONS
//
//
//Function Name               Function Returns
//
//DATABASE()                  Name of the database
//IFNULL(expression, value)   value if expression is null;
//                            expression if expression is not null
//USER()                      User name in the DBMS
//
//
//
//
//CONVERSION FUNCTIONS
//Function Name               Function Returns
//
//CONVERT(value, SQLtype)     value converted to SQLtype where SQLtype may
//                            be one of the following SQL types:
//                            BIGINT, BINARY, BIT, CHAR, DATE, DECIMAL, DOUBLE,
//                            FLOAT, INTEGER, LONGVARBINARY, LONGVARCHAR,
//                            REAL, SMALLINT, TIME, TIMESTAMP,
//                            TINYINT, VARBINARY


// End SqlJdbcFunctionCall.java