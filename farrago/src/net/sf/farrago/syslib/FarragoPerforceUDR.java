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
package net.sf.farrago.syslib;

import java.io.*;

import java.net.*;

import java.sql.*;


/**
 * FarragoPerforceUDR is a set of user-defined routines for accessing
 * information from the Eigenbase Perforce server. Currently just for fun, and
 * guaranteed brittle.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class FarragoPerforceUDR
{
    //~ Methods ----------------------------------------------------------------

    public static void getChangelists(
        String filePattern,
        int maxChanges,
        PreparedStatement resultInserter)
        throws Exception
    {
        String urlString =
            "http://perforce.eigenbase.org:8080/@md=d&cd=//&pat="
            + filePattern + "&c=yA1@//?ac=43";
        if (maxChanges != -1) {
            urlString += "&mx=" + maxChanges;
        }
        URL url = new URL(urlString);
        InputStream inputStream = null;

        boolean readingChanges = false;

        int iField = 1;

        try {
            inputStream = url.openStream();
            InputStreamReader reader = new InputStreamReader(inputStream);
            LineNumberReader lineReader = new LineNumberReader(reader);
            for (;;) {
                String line = lineReader.readLine();
                if (line == null) {
                    return;
                }
                int lineNumber = lineReader.getLineNumber();

                if (line.equals("Description</th>")) {
                    readingChanges = true;
                    continue;
                }

                if (!readingChanges) {
                    continue;
                }

                if (line.equals("</Form>")) {
                    break;
                }

                if (line.startsWith("<td")) {
                    continue;
                }

                if (line.startsWith("<th")) {
                    continue;
                }

                if (line.startsWith("<tr")) {
                    continue;
                }

                if (line.equals("</tr>")) {
                    continue;
                }

                if (line.equals("</td>")) {
                    continue;
                }

                if (line.startsWith("<img")) {
                    continue;
                }

                if (line.startsWith("<pre")) {
                    continue;
                }

                if (line.endsWith("<br></td>")) {
                    String s = line;
                    if (s.length() > 2044) {
                        s = s.substring(0, 2044) + " ...";
                    }
                    resultInserter.setString(iField, s);
                    resultInserter.executeUpdate();
                    iField = 1;
                    continue;
                }

                if (line.startsWith("<a ")) {
                    int i = line.indexOf('>');
                    int j = line.indexOf('<', 1);
                    if ((i == -1) || (j == -1)) {
                        continue;
                    }
                    line = line.substring(i + 1, j);
                }

                if (line.endsWith("</td>")) {
                    line = line.substring(0, line.length() - 5);
                }

                resultInserter.setString(iField, line);
                ++iField;
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}

// End FarragoPerforceUDR.java
