/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.farrago.namespace.flatfile;

import java.sql.*;
import java.util.Properties;

import net.sf.farrago.namespace.impl.*;

/**
 * Decodes options used for a flat file data server.
 *
 * @author John V. Pham
 * @version $Id$
 */
class FlatFileParams extends MedAbstractBase
{
    //~ Static fields/initializers --------------------------------------------

    public static final String PROP_DIRECTORY = "DIRECTORY";
    public static final String PROP_FILE_EXTENSION = "FILE_EXTENSION";
    public static final String PROP_CONTROL_FILE_EXTENSION =
        "CONTROL_FILE_EXTENSION";
    public static final String PROP_FIELD_DELIMITER = "FIELD_DELIMITER";
    public static final String PROP_LINE_DELIMITER = "LINE_DELIMITER";
    public static final String PROP_QUOTE_CHAR = "QUOTE_CHAR";
    public static final String PROP_ESCAPE_CHAR = "ESCAPE_CHAR";
    public static final String PROP_WITH_HEADER = "WITH_HEADER";
    public static final String PROP_WITH_ERROR_LOGGING = "WITH_ERROR_LOGGING";
    public static final String PROP_NUM_ROWS_SCAN = "NUM_ROWS_SCAN";
    public static final String PROPVAL_DAT = "DAT";
    public static final String PROPVAL_BCP = "BCP";
    public static final String PROPVAL_ERR = "ERR";

    private Properties props;
    private char fieldDelimiter, lineDelimiter;
    private char quoteChar, escapeChar;
    private boolean withHeader, withErrorLogging;
    private int numRowsScan;
    
    //~ Constructors ----------------------------------------------------------

    public FlatFileParams() {}

    public void decode(Properties props)
        throws SQLException
    {
        this.props = props;
        fieldDelimiter = decodeDelimiter(
            props.getProperty(PROP_FIELD_DELIMITER, ","));
        lineDelimiter = decodeDelimiter(
            props.getProperty(PROP_LINE_DELIMITER, "\\n"));
        quoteChar = decodeSpecialChar(
            props.getProperty(PROP_QUOTE_CHAR, "\""), '"');
        escapeChar = decodeSpecialChar(
            props.getProperty(PROP_ESCAPE_CHAR, "\""), '"');
        withHeader = getBooleanProperty(props, PROP_WITH_HEADER, true);
        withErrorLogging =
            getBooleanProperty(props, PROP_WITH_ERROR_LOGGING, true);
        numRowsScan = getIntProperty(props, PROP_NUM_ROWS_SCAN, 5);
    }
    
    /**
     * Decodes a delimiter string into a character. Recognizes the escape
     * sequences \t, \r, \n. The two line characters \r and \n are canonized
     * into a universal line character \n.
     *
     * This function's behavior is based on odd heuristics. An initial double
     * quote denotes no delimiter. A tab escape "\t" becomes a tab.
     * Otherwise preference is given to the line characters escapes "\r" and
     * "\n". These escapes are recognized from the 0, 1, and 2 index
     * positions. The escape cannot quote any other character, and becomes a
     * delimiter. Any other character becomes a delimiter.
     *
     * REVIEW: this behavior seems overly complex and awkward
     */
    private char decodeDelimiter(String delim)
    {
        if (delim == null || delim.length() == 0 || delim.charAt(0) == '"') {
            return 0;
        }
        if (delim.indexOf("\\t") == 0) {
            return '\t';
        }
        int a = delim.indexOf("\\r");
        int b = delim.indexOf("\\n");
        if (a == 0 || a == 1 || a == 2 || b == 0 || b == 1 || b == 2) {
            return '\n';
        }
        return delim.charAt(0);
    }

    private char decodeSpecialChar(String specialChar, char defaultChar)
    {
        if (specialChar == null || specialChar.length() == 0) {
            return defaultChar;
        }
        return specialChar.charAt(0);
    }

    private Properties getProperties() 
    {
        return props;
    }
    
    public String getDirectory() 
    {
        return getProperties().getProperty(PROP_DIRECTORY, null);
    }

    public String getFileExtenstion()
    {
        return getProperties().getProperty(PROP_FILE_EXTENSION, PROPVAL_DAT);
    }
        
    public String getControlFileExtenstion()
    {
        return getProperties().getProperty(PROP_CONTROL_FILE_EXTENSION,
            PROPVAL_BCP);
    }

    public char getFieldDelimiter() 
    {
        return fieldDelimiter;
    }

    public char getLineDelimiter()
    {
        return lineDelimiter;
    }

    public char getQuoteChar() 
    {
        return quoteChar;
    }

    public char getEscapeChar()
    {
        return escapeChar;
    }
    
    public boolean getWithHeader() 
    {
        return withHeader;
    }

    public boolean getWithErrorLogging()
    {
        return withErrorLogging;
    }

    public int getNumRowsScan()
    {
        return numRowsScan;
    }
}

// End FlatFileParams.java
