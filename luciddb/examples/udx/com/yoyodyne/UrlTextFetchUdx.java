package com.yoyodyne;

import java.net.*;
import java.io.*;
import java.sql.*;

public class UrlTextFetchUdx
{
    public static void execute(
        String urlString,
        PreparedStatement resultInserter)
        throws Exception
    {
        URL url = new URL(urlString);
        InputStream inputStream = null;
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
                resultInserter.setInt(1, lineNumber);
                resultInserter.setString(2, line);
                resultInserter.executeUpdate();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }
    }
}

// End UrlTextFetchUdx.java
