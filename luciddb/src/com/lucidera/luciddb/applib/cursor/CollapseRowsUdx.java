/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
package com.lucidera.luciddb.applib.cursor;

import java.sql.*;
import java.util.*;
import com.lucidera.luciddb.applib.resource.*;

/**
 * Collapses one to many relationships into one to one relationships.  In the 
 * case where A maps to B, this will result in A mapping to a concatenation 
 * of all Bs which previously mapped to A.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class CollapseRowsUdx
{
    private static final int MAX_CONCAT_LEN = 16384;
    
    public static void execute(
        ResultSet inputSet, String delimiter, PreparedStatement resultInserter)
        throws ApplibException
    {
        // validate the number of input and output columns
        try{
            if ((resultInserter.getParameterMetaData().getParameterCount() 
                    != 3) || (inputSet.getMetaData().getColumnCount() != 2))
            {
                throw ApplibResourceObject.get().InputOutputColumnError.ex();
            }
        } catch (SQLException e) {
            throw ApplibResourceObject.get().CannotGetMetaData.ex(e);
        }

        Map<String, List<String>> relMap = new HashMap();
        List<String> currentList;
        String currentKey;
        String currentValue;

        try {
            while (inputSet.next()) {
                currentKey = inputSet.getString(1);
                currentValue = inputSet.getString(2);
                
                currentList = relMap.get(currentKey);
                
                // create list if none exists
                if (currentList == null) {
                    currentList = new ArrayList();
                } 
                
                // add value to list unless it's a null
                if (currentValue != null) {
                    currentList.add(currentValue);
                }
                
                relMap.put(currentKey, currentList);
            }

            // output table
            Iterator<String> keyIter = relMap.keySet().iterator();
            while (keyIter.hasNext()) {
                currentKey = keyIter.next();
                resultInserter.setString(1, currentKey);
                
                currentList = relMap.get(currentKey);
                
                if ((currentList == null) || currentList.isEmpty()) {
                    // inserts null for the concatenation if list doesn't
                    // exist or is empty
                    resultInserter.setString(2, null);
                    resultInserter.setInt(3, 0);
                } else {
                    // inserts concatenation of all items in list
                    StringBuilder sb = new StringBuilder();
                    
                    int numItems = 0;
                    int delimLen = delimiter.length();

                    for (String val : currentList) {
                        int newLen = val.length();
                        // all items after first are preceded by delimiter;
                        // account for truncation accordingly
                        if (numItems > 0) {
                            newLen += delimLen;
                        }
                        if (sb.length() + newLen > MAX_CONCAT_LEN) {
                            // truncate to avoid going over the limit
                            break;
                        }
                        if (numItems > 0) {
                            sb.append(delimiter);
                        }
                        sb.append(val);
                        numItems++;
                    }

                    // REVIEW jvs 24-Jan-2007:  if first item was so big that
                    // it exceeded the truncation limit, we will emit 0
                    // and empty string (rather than null as in the case
                    // of no items at all).

                    resultInserter.setString(2, sb.toString());
                    resultInserter.setInt(3, numItems);
                }
                resultInserter.executeUpdate();
            }
        } catch (SQLException e) {
            throw ApplibResourceObject.get().DatabaseAccessError.ex(
                e.toString(), e);
        }
    }
}

// End CollapseOneToManyRelationshipsUdx.java
