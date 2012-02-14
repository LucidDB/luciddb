/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.applib.cursor;

import java.sql.*;

import java.util.*;

import net.sf.farrago.syslib.*;

import org.eigenbase.applib.resource.*;


/**
 * Collapses one to many relationships into one to one relationships. In the
 * case where A maps to B, this will result in A mapping to a concatenation of
 * all Bs which previously mapped to A.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class CollapseRowsUdx
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int MAX_CONCAT_LEN = 16384;

    //~ Methods ----------------------------------------------------------------

    public static void execute(
        ResultSet inputSet,
        String delimiter,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        try {
            ResultSetMetaData rsmd = inputSet.getMetaData();

            // validate the number of input and output columns
            if ((resultInserter.getParameterMetaData().getParameterCount()
                    != 3)
                || (rsmd.getColumnCount() != 2))
            {
                throw ApplibResource.instance().InputOutputColumnError.ex();
            }

            // validate that parent column datatype is VARCHAR or CHAR
            if (!((rsmd.getColumnType(1) == java.sql.Types.VARCHAR)
                    || (rsmd.getColumnType(1) == java.sql.Types.CHAR)))
            {
                throw ApplibResource.instance().InvalidColumnDatatype.ex(
                    rsmd.getColumnLabel(1),
                    "VARCHAR");
            }
        } catch (SQLException e) {
            throw ApplibResource.instance().CannotGetMetaData.ex(e);
        }

        String currentChildren;
        int childCount;
        boolean exceededLength = false;
        String currentParent;

        try {
            // return no rows, if no rows passed in
            if (!inputSet.next()) {
                return;
            }

            currentParent = inputSet.getString(1);
            String child = inputSet.getString(2);
            if (child == null) {
                currentChildren = null;
                childCount = 0;
            } else if (child.length() <= MAX_CONCAT_LEN) {
                currentChildren = child;
                childCount = 1;
            } else {
                // first item exceeds truncation limit
                currentChildren = "";
                childCount = 0;
                exceededLength = true;
            }

            while (inputSet.next()) {
                String parent = inputSet.getString(1);
                int compare =
                    FarragoSyslibUtil.compareKeysUsingGroupBySemantics(
                        currentParent,
                        parent);
                if (compare > 0) {
                    // row is out of order
                    throw ApplibResource.instance().InputRowsNotSorted.ex(
                        inputSet.getMetaData().getColumnLabel(1),
                        parent);
                } else if (compare < 0) {
                    // new parent value, emit row for previous parent value
                    resultInserter.setString(1, currentParent);
                    resultInserter.setString(2, currentChildren);
                    resultInserter.setInt(3, childCount);
                    resultInserter.executeUpdate();

                    currentParent = parent;
                    child = inputSet.getString(2);
                    if (child == null) {
                        currentChildren = null;
                        childCount = 0;
                        exceededLength = false;
                    } else if (child.length() <= MAX_CONCAT_LEN) {
                        currentChildren = child;
                        childCount = 1;
                        exceededLength = false;
                    } else {
                        // first item exceeds truncation limit
                        currentChildren = "";
                        childCount = 0;
                        exceededLength = true;
                    }
                } else {
                    // same parent value
                    if (exceededLength) {
                        continue;
                    }

                    child = inputSet.getString(2);
                    if (child == null) {
                        continue;
                    }

                    if (currentChildren == null) {
                        // no previous children
                        if (child.length() <= MAX_CONCAT_LEN) {
                            currentChildren = child;
                            childCount++;
                        } else {
                            // first item exceeds truncation limit
                            currentChildren = "";
                            childCount = 0;
                            exceededLength = true;
                        }
                    } else {
                        // append child if truncation limit not exceeded
                        int newLen = child.length() + delimiter.length();
                        if ((currentChildren.length() + newLen)
                            <= MAX_CONCAT_LEN)
                        {
                            currentChildren += delimiter + child;
                            childCount++;
                        } else {
                            exceededLength = true;
                        }
                    }
                }
            }

            // output the last row
            resultInserter.setString(1, currentParent);
            resultInserter.setString(2, currentChildren);
            resultInserter.setInt(3, childCount);
            resultInserter.executeUpdate();
        } catch (SQLException e) {
            throw ApplibResource.instance().DatabaseAccessError.ex(
                e.toString(),
                e);
        }
    }
}

// End CollapseRowsUdx.java
