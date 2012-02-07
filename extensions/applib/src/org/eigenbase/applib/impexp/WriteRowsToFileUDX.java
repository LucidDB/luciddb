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
package org.eigenbase.applib.impexp;

import java.io.*;

import java.net.*;

import java.sql.*;

import java.util.*;
import java.util.zip.*;


/**
 * Purpose: Allow serialized rows to be written to a file.<br>
 * Please refer to
 * http://pub.eigenbase.org/wiki/LucidDbAppLib_WRITE_ROWS_TO_FILE<br>
 *
 * @author Ray Zhang
 * @since Dec-14-2009
 */
public class WriteRowsToFileUDX
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PREFIX_ONE = "file://";
    public static final String PREFIX_TWO = "classpath://";
    public static final String SEPARATOR = "/";

    //~ Methods ----------------------------------------------------------------

    public static Map<String, String> parseURL(String url)
    {
        Map<String, String> ret = new HashMap<String, String>();

        url = url.trim();
        String file_path = "";
        String file_name = "";

        int index = url.lastIndexOf(SEPARATOR);

        if (index != -1) {
            file_path = url.substring(0, index + 1);
            file_name = url.substring(index + 1, url.length());
        } else {
            file_name = url;
        }

        ret.put("FILE_PATH", file_path);
        ret.put("FILE_NAME", file_name);

        return ret;
    }

    public static File openFile(String url)
        throws MalformedURLException
    {
        File ret = null;
        url = url.trim();
        if (url.startsWith(PREFIX_ONE)) {
            url = url.substring(7);
            ret = new File(url);
        } else if (url.startsWith(PREFIX_TWO)) {
            url = url.substring(12);
            Map<String, String> myFile = parseURL(url);
            URL myURL = Object.class.getResource(myFile.get("FILE_PATH"));
            if (myURL != null) {
                myURL = new URL(myURL.toString() + myFile.get("FILE_NAME"));
                ret = new File(myURL.getFile());
            } else {
                throw new MalformedURLException(
                    "Bad File Location: "
                    + myFile.get("FILE_PATH")
                    + " is not exist. Please change it.");
            }
        } else {
            throw new RuntimeException(
                "Please use [file://] or [classpath://] as a prefix to input url");
        }

        return ret;
    }

    /**
     * @author Ray Zhang
     */
    public static void execute(
        ResultSet inputSet,
        String url,
        boolean is_compressed,
        PreparedStatement resultInserter)
        throws Exception
    {
        int status = 0;
        String err_msg = "";
        int row_count = 0;

        FileOutputStream fileOut = null;
        ObjectOutputStream objOut = null;
        GZIPOutputStream gzOut = null;

        try {
            fileOut = new FileOutputStream(openFile(url));
            if (is_compressed) {
                gzOut = new GZIPOutputStream(fileOut);
                objOut = new ObjectOutputStream(gzOut);
            } else {
                objOut = new ObjectOutputStream(fileOut);
            }

            int columnCount = inputSet.getMetaData().getColumnCount();
            List header = new ArrayList();

            header.add(1);
            List formater = new ArrayList(columnCount);
            for (int i = 0; i < columnCount; i++) {
                formater.add(
                    inputSet.getMetaData().getColumnDisplaySize(i + 1));
            }

            header.add(formater);

            objOut.writeObject(header);
            objOut.flush();

            header = null;

            while (inputSet.next()) {
                List row_entity = new ArrayList();
                for (int i = 0; i < columnCount; i++) {
                    row_entity.add(inputSet.getObject(i + 1));
                }

                objOut.writeObject(row_entity);
                objOut.flush();
                row_count++;
                row_entity = null;
            }

            err_msg =
                row_count
                + " rows have been written in the specific file successfully!";
        } catch (Exception ex) {
            //status = 1;
            //err_msg = ex.getMessage();
            throw ex;
        } finally {
            try {
                if (objOut != null) {
                    objOut.close();
                }

                if (is_compressed && (gzOut != null)) {
                    gzOut.close();
                }
                if (fileOut != null) {
                    fileOut.close();
                }
            } catch (IOException e) {
                status = 1;
                err_msg = e.getMessage();
            }
        }
        resultInserter.setInt(1, status);
        resultInserter.setString(2, err_msg);
        resultInserter.executeUpdate();
    }
}

// End WriteRowsToFileUDX.java
