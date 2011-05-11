/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 LucidEra, Inc.
// Copyright (C) 2007-2007 The Eigenbase Project
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
package com.lucidera.luciddb.applib.string;

import com.lucidera.luciddb.applib.resource.*;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * Split strings into individual tokens on multiple rows using a separator
 * character.
 *
 * @author Oscar Gothberg
 * @author Jakob Bergendahl
 * @version $Id$
 */
public abstract class SplitStringUdx
{
    /**
     * Splits input string into a single column table.
     * @param inputString the string to be split.
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @param trimTokens trim leading and trailing whitespace from split
     * tokens (TRUE/FALSE)
     * @param resultInserter output handler.
     */
    public static void execute(
        String inputString,
        String separator,
        String escape,
        boolean trimTokens,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        if (!checkSeparatorAndEscape(separator, escape)) {
            return;
        }

        // NULL input gives empty output
        if (inputString == null) {
          return;
        }

        Iterator<String> tokens = splitString(
            inputString, 
            separator.charAt(0), 
            escape.charAt(0), 
            trimTokens).iterator();
        while (tokens.hasNext()) {
            resultInserter.setString(1, tokens.next());
            resultInserter.executeUpdate();
        }
    }

    /**
     * Splits strings in a single column input table into rows.
     * @param inputSet the one-column table containing strings to be split.
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @param trimTokens trim leading and trailing whitespace from split
     * tokens (TRUE/FALSE)
     * @param resultInserter output handler.
     */
    public static void execute(
        ResultSet inputSet,
        String separator,
        String escape,
        boolean trimTokens,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        // check that inputSet has only one column
        if (inputSet.getMetaData().getColumnCount() != 1) {
            throw ApplibResourceObject.get().
                InputMustBeSingleColumn.ex();
        }
        
        // hand this over to the multi-column execute
        ArrayList<String> v = new ArrayList<String>();
        v.add(inputSet.getMetaData().getColumnName(1));
        execute(
            inputSet,
            v,
            separator,
            escape,
            trimTokens,
            resultInserter);
    }

    /** 
     * Splits strings in one column of a multicolumn input table into rows.
     * @param inputSet the multi-column table containing a column of strings to
     * be split.
     * @param columnNameList ROW argument to designate which column of strings
     * is to be split.
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @param trimTokens trim leading and trailing whitespace from split
     * tokens (TRUE/FALSE)
     * @param resultInserter output handler.
     */
    public static void execute(
        ResultSet inputSet,
        List<String> columnNameList,
        String separator,
        String escape,
        boolean trimTokens,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        if (!checkSeparatorAndEscape(separator, escape)) {
            return;
        }

        // check that columnNameList is exactly one column
        if (columnNameList.size() != 1) {
            throw ApplibResourceObject.get().
                SplitColNameMustBeSingleColumn.ex();
        }
        
        processTableInput(
            inputSet,
            inputSet.findColumn(
                columnNameList.iterator().next()),
            separator.charAt(0), 
            escape.charAt(0), 
            trimTokens,
            null,
            null,
            resultInserter);
    }

    /**
     * Splits input string into a two column table and inserts a sequence number
     * in the last column.
     * @param inputString the string to be split.
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @param trimTokens trim leading and trailing whitespace from split
     * tokens (TRUE/FALSE)
     * @param startNum the starting sequence number. If null, sequence will 
     * start at 1.
     * @param increment the increment that is added to the sequence number for 
     * every row.
     * @param resultInserter output handler.
     */
    public static void splitSingleStringWithSequence(
        String inputString,
        String separator,
        String escape,
        boolean trimTokens,
        Long startNum,
        Long increment,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        if (!checkSeparatorAndEscape(separator, escape)) {
            return;
        }

        // NULL input gives empty output
        if (inputString == null) {
          return;
        }
      
        // Default value for START_NUM and INCREMENT_BY is 1
        long sequenceNumber = (startNum != null) ? startNum : 1L;
        long incr = (increment != null) ? increment : 1L;

        if (incr == 0) {
            throw ApplibResourceObject.get().
                IncrementByMustNotBeZero.ex();
        }
        
        Iterator<String> tokens = splitString(
            inputString, 
            separator.charAt(0), 
            escape.charAt(0), 
            trimTokens).iterator();
        while (tokens.hasNext()) {
            resultInserter.setString(1, tokens.next());
            resultInserter.setLong(2, sequenceNumber);
            sequenceNumber += incr;
            resultInserter.executeUpdate();
        }
    }

    /**
     * Splits strings in a single column input table into rows and inserts a 
     * sequence number in the last column.
     * @param inputSet the one-column table containing strings to be split.
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @param trimTokens trim leading and trailing whitespace from split
     * tokens (TRUE/FALSE)
     * @param startNum the starting sequence number. If null, sequence will 
     * start at 1.
     * @param increment the increment that is added to the sequence number for 
     * every row.
     * @param resultInserter output handler.
     */
    public static void splitSingleColumnWithSequence(
        ResultSet inputSet,
        String separator,
        String escape,
        boolean trimTokens,
        Long startNum,
        Long increment,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        // check that inputSet has only one column
        if (inputSet.getMetaData().getColumnCount() != 1) {
            throw ApplibResourceObject.get().
                InputMustBeSingleColumn.ex();
        }
        
        // hand this over to the multi-column execute
        ArrayList<String> v = new ArrayList<String>();
        v.add(inputSet.getMetaData().getColumnName(1));
        splitMultiColumnWithSequence(
            inputSet,
            v,
            separator,
            escape,
            trimTokens,
            startNum,
            increment,
            resultInserter);
    }

    /**
     * Splits strings in one column of a multicolumn input table into rows and 
     * inserts a sequence number in the last column sequence number.
     * @param inputSet the multi-column table containing a column of strings to
     * be split.
     * @param columnNameList ROW argument to designate which column of strings
     * is to be split.
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @param trimTokens trim leading and trailing whitespace from split
     * tokens (TRUE/FALSE)
     * @param startNum the starting sequence number. If null, sequence will 
     * start at 1.
     * @param increment the increment that is added to the sequence number for 
     * every row.
     * @param resultInserter output handler.
     */
    public static void splitMultiColumnWithSequence(
        ResultSet inputSet,
        List<String> columnNameList,
        String separator,
        String escape,
        boolean trimTokens,
        Long startNum,
        Long increment,
        PreparedStatement resultInserter)
        throws SQLException, ApplibException
    {
        if (!checkSeparatorAndEscape(separator, escape)) {
            return;
        }

        // check that columnNameList is exactly one column
        if (columnNameList.size() != 1) {
            throw ApplibResourceObject.get().
                SplitColNameMustBeSingleColumn.ex();
        }
        
        // Default value for START_NUM and INCREMENT_BY is 1
        startNum = (startNum != null) ? startNum : new Long(1);
        increment = (increment != null) ? increment : new Long(1);
        
        if (increment == 0) {
            throw ApplibResourceObject.get().
                IncrementByMustNotBeZero.ex();
        }

        processTableInput(
            inputSet,
            inputSet.findColumn(
                columnNameList.iterator().next()),
            separator.charAt(0), 
            escape.charAt(0), 
            trimTokens, 
            startNum,
            increment,
            resultInserter);
    }

    /**
     * Does the actual string splitting.
     * @param inputSet the input cursor.
     * @param stringColIndex index of the column containing strings to be split.
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @param trimTokens trim leading and trailing whitespace from split
     * @param startNum the starting sequence number or null if no sequence number
     * should be inserted.
     * @param increment the increment that is added to the sequence number for 
     * every row (not used if startNum == null).
     * @param resultInserter output handler.
     */
    private static void processTableInput(
        ResultSet inputSet,
        int stringColIndex,
        char separator,
        char escape,
        boolean trimTokens,
        Long startNum,
        Long increment,
        PreparedStatement resultInserter)
        throws SQLException
    {
        int columnCount = inputSet.getMetaData().getColumnCount();
        long sequenceNumber = 0;
        long incr = 1;
        
        if (startNum != null && increment != null) {
            sequenceNumber = startNum;
            incr = increment;
        }

        while (inputSet.next()) {
            Iterator<String> tokens = splitString(
                inputSet.getString(stringColIndex), 
                separator, 
                escape, 
                trimTokens).iterator();

            // if an input row didn't generate any legal tokens, keep the rest
            // of the row but with the split column value set to null
            if (!tokens.hasNext()) {
                for (int colIndex = 1; colIndex <= columnCount; colIndex++) {
                    if (colIndex == stringColIndex) {
                        resultInserter.setNull(colIndex, java.sql.Types.VARCHAR);
                    } else {
                        resultInserter.setObject(colIndex, inputSet.getObject(colIndex));
                    }
                }
                if (startNum != null) {
                    resultInserter.setLong(columnCount+1, sequenceNumber);
                    sequenceNumber += incr;
                }
                resultInserter.executeUpdate();
            }

            // expand rows, one row for each token
            while (tokens.hasNext()) {
                for (int colIndex = 1; colIndex <= columnCount; colIndex++) {
                    if (colIndex == stringColIndex) {
                        resultInserter.setString(colIndex, tokens.next());
                    } else {
                        resultInserter.setObject(colIndex, inputSet.getObject(colIndex));
                    }
                }
                if (startNum != null) {
                    resultInserter.setLong(columnCount+1, sequenceNumber);
                    sequenceNumber += incr;
                }
                resultInserter.executeUpdate();
            }
        }
    }

    /**
     * Does the actual string splitting.
     * @param theString the string to be split
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @param trimTokens trim leading and trailing whitespace from split
     */
    private static List<String> splitString(
        String theString,
        char separator,
        char escape,
        boolean trimTokens)
    {
        ArrayList<String> v = new ArrayList<String>();
        int start = 0;
        String tmp;
        int i = 0;

        // one row of null input returns a row of null
        // applicable for split_rows versions only, split_string_to_rows
        // won't get here
        if (theString == null) {
            v.add(null);
            return v;
        }

        while (i <= theString.length()) {
            if (i == theString.length() || theString.charAt(i) == separator) {
                if (i==0 || i == theString.length() || theString.charAt(i-1) != escape) {
                    // found the end of a token
                    tmp = theString.substring(start, i);

                    if (trimTokens) {
                        tmp = tmp.trim();
                    }
                    
                    if (tmp.length() > 0) {
                        v.add(tmp);
                    }
                    start = i+1;
                } else if (theString.charAt(i) == separator
                    && theString.charAt(i-1) == escape) {
                    // split character was escaped. get rid of escape character.
                    theString = theString.substring(0,i-1) + theString.substring(i);
                    continue;
                }
            }
            i++;
        }
        return v;
    }
 
    /**
     * Checks parameters from SQL. 
     * @param separator the single-character token to be used as splitter.
     * @param escape escape character to prevent separator character from
     * causing split.
     * @returns true if (and only if) both parameters are one character 
     * strings. false if either parameter is null.
     */    
    private static boolean checkSeparatorAndEscape(
        String separator, 
        String escape) 
        throws ApplibException
    {
        // check for NULL values, supposed to return empty
        if (separator == null || escape == null) { 
            return false;
        }
        
        // Note: None of the following two checks work, since LucidDB  
        // maps CHAR(1) to a 1-character String.

        // check that separator has exactly one character
        if (separator.length() != 1) {
            throw ApplibResourceObject.get().
                SeparatorMustBeOneCharacter.ex();
        }
        
        // check that escape has exactly one character
        if (escape.length() != 1) {
            throw ApplibResourceObject.get().
                EscapeCharMustBeOneCharacter.ex();
        }
        return true;
    }
}

// End SplitStringUdx.java
