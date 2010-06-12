/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2010-2010 LucidEra, Inc.
// Copyright (C) 2010-2010 The Eigenbase Project
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
package com.lucidera.luciddb.applib.impexp;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

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
    public static final String PREFIX_ONE = "file://";
    public static final String PREFIX_TWO = "classpath://";
    public static final String PREFIX_THREE = "jar:";

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

                    if (verifyHeaderInfo(header_from_cursor, header_from_file))
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
                throw new Exception("Error: " + e.getMessage() + "\n" + counter
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
