package com.yoyodyne;

import java.util.*;
import java.sql.*;

public class HtmlTokenizerUdx
{
    public static void execute(
        ResultSet lineInput,
        PreparedStatement resultInserter)
        throws Exception
    {
        while (lineInput.next()) {
            int lineNumber = lineInput.getInt(1);
            String line = lineInput.getString(2);
            StringTokenizer tokenizer = new StringTokenizer(
                line,
                "<>/-'\"{}(); \t\n\r\f");
            int tokenNumber = 0;
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                resultInserter.setInt(1, lineNumber);
                resultInserter.setInt(2, tokenNumber);
                resultInserter.setString(3, token);
                resultInserter.executeUpdate();
                ++tokenNumber;
            }
        }
    }
}

// End HtmlTokenizerUdx.java
