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
import java.io.*;
import java.util.zip.*;
import java.util.List;
import java.util.Iterator;

import com.lucidera.luciddb.applib.resource.*;

/**
 * Generates a CRC value per row/record for a table 
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class GenerateCrcUdx
{
    // value used for nulls so crc is unique even if rows contain nulls
    private static final String NULL_MAGIC = 
        "54686973-6973-614E-554C-4C76616C7565";

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
            nOutput = resultInserter.getParameterMetaData(
                ).getParameterCount();
        } catch (SQLException e) {
            throw ApplibResourceObject.get().InputOutputColumnError.ex(e);
        }

        assert (nOutput == nInput + 1);

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
                    throw ApplibResourceObject.get().DatabaseAccessError.ex(
                        e.toString(), e);
                }
            }
        }

        Checksum checksum = new CRC32();
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outStream = new DataOutputStream(
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
                            columns, exclude, rsmd.getColumnName(i)))
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
            throw ApplibResourceObject.get().DatabaseAccessError.ex(
                e.toString(), e);
        } catch (IOException ex) {
            throw ApplibResourceObject.get().WriteIoError.ex(
                ex.toString(), ex);
        }finally {
            try {
                outStream.close();
            } catch (IOException e) {
                // TODO: log a warning for this
            }
        }
    }
}

// End GenerateCrcUdx.java
