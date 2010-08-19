/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
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
