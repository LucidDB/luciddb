/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 DynamoBI Corporation
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
package org.eigenbase.applib.impexp;

import java.io.*;

import java.net.*;

import java.sql.*;

import java.util.*;
import java.util.zip.*;

/**
 * Purpose: Allow serialized rows to be read from a file and output in a table
 * function.<br>
 * Please refer to
 * http://pub.eigenbase.org/wiki/LucidDbAppLib_READ_ROWS_FROM_FILE<br>
 *
 * @author Ray Zhang
 * @since Dec-14-2009
 */
public class ReadRowsFromFileUDX
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PREFIX_ONE = "file://";
    public static final String PREFIX_TWO = "classpath://";
    public static final String PREFIX_THREE = "jar:";
    public static final String PREFIX_FOUR = "http://";

    //~ Methods ----------------------------------------------------------------

    public static InputStream openFile(String url)
        throws Exception
    {
        InputStream ret = null;

        if (url.trim().startsWith(PREFIX_ONE)) {
            url = url.trim().substring(7);
            ret = new FileInputStream(url);
        } else if (url.trim().startsWith(PREFIX_TWO)) {
            url = url.trim().substring(12);
            URL myURL = ReadRowsFromFileUDX.class.getResource(url);
            if (myURL != null) {
                ret = ReadRowsFromFileUDX.class.getResourceAsStream(url);
            } else {
                throw new Exception("Bad File Location! Please check!");
            }
        } else if (url.trim().startsWith(PREFIX_FOUR)) {
            URL myURL = new URL(url);
            URLConnection uc = myURL.openConnection();
            ret = uc.getInputStream();
        } else {
            throw new Exception(
                "Please use [file://] or [classpath://] as a prefix to input url");
        }

        return ret;
    }

    public static void execute(
        ResultSet inputSet,
        String url,
        boolean is_compressed,
        PreparedStatement resultInserter)
        throws Exception
    {
        InputStream fileIn = openFile(url);

        GZIPInputStream gzIn = null;
        ObjectInputStream objIn = null;
        if (is_compressed) {
            gzIn = new GZIPInputStream(fileIn);
            objIn = new ObjectInputStream(gzIn);
        } else {
            objIn = new ObjectInputStream(fileIn);
        }

        boolean is_header = true;
        int counter = 0;
        while (true) {
            try {
                List entity = (ArrayList) objIn.readObject();
                if (is_header) {
                    // check if header info is matched.
                    List header_from_cursor = getHeaderInfoFromCursor(inputSet);
                    List header_from_file = (ArrayList) entity.get(1);

                    if (verifyHeaderInfo(
                            header_from_cursor,
                            header_from_file))
                    {
                        is_header = false;
                    } else {
                        throw new Exception(
                            "Header Info was unmatched! Please check");
                    }
                } else {
                    int col_count = entity.size();
                    for (int i = 0; i < col_count; i++) {
                        resultInserter.setObject((i + 1), entity.get(i));
                    }
                    resultInserter.executeUpdate();
                }
                counter++;
            } catch (EOFException ex) {
                break;
            } catch (Exception e) {
                throw new Exception(
                    "Error: " + e.getMessage() + "\n" + counter
                    + " rows are inserted successfully.");
            }
        }

        // release all resources.
        objIn.close();
        if (is_compressed) {
            gzIn.close();
        }
        fileIn.close();
    }

    protected static boolean verifyHeaderInfo(
        List header_from_cursor,
        List header_from_file)
    {
        boolean is_matched = false;

        // 1. check column raw count
        if (header_from_cursor.size() == header_from_file.size()) {
            // 2. check the length of every field.
            int col_raw_count = header_from_cursor.size();
            for (int i = 0; i < col_raw_count; i++) {
                int length_of_field_from_cursor =
                    (Integer) header_from_cursor.get(i);
                int length_of_field_from_file =
                    (Integer) header_from_file.get(i);
                if (length_of_field_from_cursor == length_of_field_from_file) {
                    is_matched = true;
                } else {
                    is_matched = false;
                    break;
                }
            }
        }

        return is_matched;
    }

    protected static List getHeaderInfoFromCursor(ResultSet rs_in)
        throws SQLException
    {
        int columnCount = rs_in.getMetaData().getColumnCount();
        List ret = new ArrayList(columnCount);
        for (int i = 0; i < columnCount; i++) {
            ret.add(rs_in.getMetaData().getColumnDisplaySize(i + 1));
        }

        return ret;
    }
}

// End ReadRowsFromFileUDX.java
