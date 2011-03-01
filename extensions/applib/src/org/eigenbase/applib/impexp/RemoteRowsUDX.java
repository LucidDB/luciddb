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
 * Purpose: Allow serialized rows to be streamed via HTTP from remote Java
 * applications (PDI / Talend).<br>
 * Please refer to http://pub.eigenbase.org/wiki/LucidDbAppLib_REMOTE_ROWS<br>
 *
 * @author Ray Zhang
 * @since Dec-16-2009
 */
public class RemoteRowsUDX
{

    private static final String HEADER_PREFIX = "RemoteRowsUDX: Header Mismatch: ";
    //~ Methods ----------------------------------------------------------------

    public static void execute(
        ResultSet inputSet,
        int port,
        boolean is_compressed,
        PreparedStatement resultInserter)
        throws Exception
    {
        ServerSocket ss = new ServerSocket(port);
        Socket socket = null;

        try {
            socket = ss.accept();

            InputStream sIn = socket.getInputStream();
            GZIPInputStream gzIn = null;
            ObjectInputStream objIn = null;

            if (is_compressed) {
                gzIn = new GZIPInputStream(sIn);
                objIn = new ObjectInputStream(gzIn);
            } else {
                objIn = new ObjectInputStream(sIn);
            }

            boolean is_header = true;
            int row_counter = 0;

            while (true) {
                try {
                    List entity = (ArrayList) objIn.readObject();

                    // disable header format check.
                    if (is_header) {
                        //   check if header info is matched.
                        List header_from_cursor =
                            getHeaderInfoFromCursor(inputSet);
                        List header_from_file = (ArrayList) entity.get(1);

                        verifyHeaderInfo( header_from_cursor, header_from_file);
                        is_header = false;

                    } else {
                        int col_count = entity.size();
                        for (int i = 0; i < col_count; i++) {
                            resultInserter.setObject((i + 1), entity.get(i));
                        }
                        resultInserter.executeUpdate();
                        row_counter++;
                    }
                } catch (EOFException ex) {
                    break;
                } catch (Exception e) {
                    StringWriter writer = new StringWriter();
                    e.printStackTrace(new PrintWriter(writer, true));
                    throw new Exception(
                        "Error: " + writer.toString() + "\n"
                        + row_counter + " rows are inserted successfully.");
                }
            }

            // release all resources.
            objIn.close();
            if (is_compressed) {
                gzIn.close();
            }
            sIn.close();
            if (is_header == false) {
                socket.close();
            }
        } catch (Exception ex) {
            if (socket != null) {
                socket.close();
            }

            ss.close();
            throw ex;
        }

        ss.close();
    }

    protected static boolean verifyHeaderInfo(
        List header_from_cursor,
        List header_from_file) throws Exception
    {
        boolean is_matched = false;
	

        // 1. check column raw count
        if (  header_from_cursor.size() != header_from_file.size()) {
    		throw new Exception(HEADER_PREFIX + 
    			"Header Size Mismatch: " +
    			"cursor = " + header_from_cursor.size() +
    			" from source = " + header_from_file.size() );
        }
        //TODO: Check datatypes
        
        return true;
    }

    /**
     * Extract every type of column from cursor meta data.<br>
     * Notice: CHAR/VARCHAR is considered as STRING.
     *
     * @param rs_in
     *
     * @return list of types of cursor.
     *
     * @throws SQLException
     */
    protected static List<String> getHeaderInfoFromCursor(ResultSet rs_in)
        throws SQLException
    {
        int columnCount = rs_in.getMetaData().getColumnCount();
        List<String> ret = new ArrayList<String>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            String type = rs_in.getMetaData().getColumnTypeName(i + 1);
            ret.add(type);
        }
        return ret;
    }
}

// End RemoteRowsUDX.java
