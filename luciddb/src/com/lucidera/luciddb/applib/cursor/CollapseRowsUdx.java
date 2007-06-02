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

        HashMap<String, ArrayList<String>> relMap = new HashMap();
        ArrayList currentList;
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
                    Iterator<String> valIter = currentList.iterator();
                    String concatenation = valIter.next();
                    int numItems = 1;

                    while (valIter.hasNext()) {
                        concatenation = concatenation + delimiter + 
                            valIter.next();
                        numItems++;
                    }
                    resultInserter.setString(2, concatenation);
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
