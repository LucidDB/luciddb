/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2008 The Eigenbase Project
// Copyright (C) 2008 SQLstream, Inc.
// Copyright (C) 2008 Dynamo BI Corporation
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
