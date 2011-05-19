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

import java.io.*;

import java.sql.*;

import java.util.*;
import java.util.zip.*;

import org.eigenbase.applib.resource.*;


/**
 * Generates a CRC value per row/record for a table
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class GenerateCrcUdx
{
    //~ Static fields/initializers ---------------------------------------------

    // value used for nulls so crc is unique even if rows contain nulls
    private static final String NULL_MAGIC =
        "54686973-6973-614E-554C-4C76616C7565";

    //~ Methods ----------------------------------------------------------------

    public static void execute(
        ResultSet inputSet,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        calculateCrc(inputSet, null, false, resultInserter);
    }

    public static void execute(
        ResultSet inputSet,
        List<String> columnNames,
        boolean exclude,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        calculateCrc(inputSet, columnNames, exclude, resultInserter);
    }

    private static boolean useColumnForCrc(
        List<String> columns,
        boolean exclude,
        String columnName)
    {
        if (columns == null) {
            // use column since no subset is specified
            return true;
        } else if (columns.contains(columnName)) {
            return exclude ? false : true;
        } else {
            return exclude ? true : false;
        }
    }

    private static void calculateCrc(
        ResultSet inputSet,
        List<String> columns,
        boolean exclude,
        PreparedStatement resultInserter)
        throws ApplibException
    {
        int nInput = 0;
        int nOutput = 0;

        try {
            nInput = inputSet.getMetaData().getColumnCount();
            nOutput = resultInserter.getParameterMetaData().getParameterCount();
        } catch (SQLException e) {
            throw ApplibResource.instance().InputOutputColumnError.ex(e);
        }

        assert (nOutput == (nInput + 1));

        // checks for specifying column subset used to calculate crc
        if (columns != null) {
            assert (nInput >= columns.size());

            // Check that columns exist in ResultSet
            Iterator<String> columnIter = columns.iterator();
            while (columnIter.hasNext()) {
                String columnName = columnIter.next();
                try {
                    inputSet.findColumn(columnName);
                } catch (SQLException e) {
                    // could be database access error or invalid column name
                    throw ApplibResource.instance().DatabaseAccessError.ex(
                        e.toString(),
                        e);
                }
            }
        }

        Checksum checksum = new CRC32();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outStream =
            new DataOutputStream(
                new CheckedOutputStream(byteStream, checksum));
        try {
            ResultSetMetaData rsmd = inputSet.getMetaData();

            while (inputSet.next()) {
                checksum.reset();
                byteStream.reset();
                for (int i = 1; i <= nInput; i++) {
                    Object obj = inputSet.getObject(i);
                    resultInserter.setObject(i, obj);

                    // check if column should be used to calcuate CRC
                    if (useColumnForCrc(
                            columns,
                            exclude,
                            rsmd.getColumnName(i)))
                    {
                        if (obj == null) {
                            // add magic value to the stream for nulls
                            outStream.writeBytes(NULL_MAGIC);
                        } else {
                            outStream.writeBytes(obj.toString());
                        }
                    }
                }
                resultInserter.setLong(nInput + 1, checksum.getValue());
                resultInserter.executeUpdate();
            }
        } catch (SQLException e) {
            throw ApplibResource.instance().DatabaseAccessError.ex(
                e.toString(),
                e);
        } catch (IOException ex) {
            throw ApplibResource.instance().WriteIoError.ex(
                ex.toString(),
                ex);
        } finally {
            try {
                outStream.close();
            } catch (IOException e) {
                // TODO: log a warning for this
            }
        }
    }
}

// End GenerateCrcUdx.java
