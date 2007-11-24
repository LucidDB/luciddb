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
 * @version $Id$
 */
public abstract class SplitStringUdx
{
    /**
     * Execute with single-string input.
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
     * Execute with multi-string, single-column, input.
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
     * Execute with multi-string, multi-column, input.
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
     * @param resultInserter output handler.
     */
    private static void processTableInput(
        ResultSet inputSet,
        int stringColIndex,
        char separator,
        char escape,
        boolean trimTokens,
        PreparedStatement resultInserter)
        throws SQLException
    {
        int columnCount = inputSet.getMetaData().getColumnCount();
        
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
        
        while (i <= theString.length()) {
            if (i == theString.length() || theString.charAt(i) == separator) {
                if (i==0 || theString.charAt(i-1) != escape) {
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
}

// End SplitStringUdx.java
