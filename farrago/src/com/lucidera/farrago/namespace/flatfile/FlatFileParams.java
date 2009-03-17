/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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

import java.io.*;

import java.sql.*;

import java.util.*;

import net.sf.farrago.namespace.impl.*;

import org.eigenbase.resource.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * Decodes options used for a flat file data server. An instance of this class
 * must be initialized with <code>decode()</code> before calling its other
 * methods. The conventions for parameters are as follows:
 *
 * <ul>
 * <li>Boolean and integer properties use the default behavior</li>
 * <li>Delimiter characters are translated to canonical form</li>
 * <li>Special characters are read as single characters</li>
 * <li>Directories become empty string or end with the File.separator</li>
 * <li>File name extensions become empty string or begin with a prefix</li>
 * </ul>
 *
 * @author John Pham
 * @version $Id$
 */
class FlatFileParams
    extends MedAbstractBase
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PROP_DIRECTORY = "DIRECTORY";
    public static final String PROP_FILE_EXTENSION = "FILE_EXTENSION";
    public static final String PROP_CONTROL_FILE_EXTENSION =
        "CONTROL_FILE_EXTENSION";
    public static final String PROP_FIELD_DELIMITER = "FIELD_DELIMITER";
    public static final String PROP_LINE_DELIMITER = "LINE_DELIMITER";
    public static final String PROP_QUOTE_CHAR = "QUOTE_CHAR";
    public static final String PROP_ESCAPE_CHAR = "ESCAPE_CHAR";
    public static final String PROP_WITH_HEADER = "WITH_HEADER";
    public static final String PROP_NUM_ROWS_SCAN = "NUM_ROWS_SCAN";
    public static final String PROP_WITH_LOGGING = "WITH_LOGGING";
    public static final String PROP_LOG_DIRECTORY = "LOG_DIRECTORY";
    public static final String PROP_DATE_FORMAT = "DATE_FORMAT";
    public static final String PROP_TIME_FORMAT = "TIME_FORMAT";
    public static final String PROP_TIMESTAMP_FORMAT = "TIMESTAMP_FORMAT";
    public static final String PROP_LENIENT = "LENIENT";
    public static final String PROP_TRIM = "TRIM";
    public static final String PROP_MAPPED = "MAPPED";

    public static final String FILE_EXTENSION_PREFIX = ".";
    public static final String LOG_FILE_EXTENSION = "err";

    protected static final String DEFAULT_FILE_EXTENSION = "txt";
    protected static final String DEFAULT_CONTROL_FILE_EXTENSION = "bcp";
    protected static final String DEFAULT_FIELD_DELIMITER = ",";
    protected static final String DEFAULT_LINE_DELIMITER = "\\n";
    protected static final String DEFAULT_QUOTE_CHAR = "\"";
    protected static final String DEFAULT_ESCAPE_CHAR = "\"";
    protected static final boolean DEFAULT_WITH_HEADER = true;
    protected static final int DEFAULT_NUM_ROWS_SCAN = 5;
    protected static final boolean DEFAULT_WITH_LOGGING = true;
    protected static final boolean DEFAULT_LENIENT = true;
    protected static final boolean DEFAULT_TRIM = true;
    protected static final boolean DEFAULT_MAPPED = false;

    //~ Enums ------------------------------------------------------------------

    /**
     * Enumeration for schema types used by the flat file reader.
     */
    public enum SchemaType
    {
        /**
         * Schema name for a special type of query that returns a one column
         * result with space separated columns sizes
         */
        DESCRIBE("DESCRIBE"),

        /**
         * Schema name for a special type of query that returns parsed text
         * columns (including headers) as they appear in a text file. Sample
         * queries are limited to a specified number of rows
         */
        SAMPLE("SAMPLE"),

        /**
         * Schema name for a typical query, in which columns are casted to typed
         * data
         */
        QUERY(new String[] { "BCP", "DEFAULT" }),

        /**
         * Schema name for a query in which columns are returned as text.
         * Similar to sample, except headers are not returned, and there is no
         * limit on the amount of rows returned.
         */
        QUERY_TEXT("TEXT");

        private static Map<String, SchemaType> types;

        static {
            types = new HashMap<String, SchemaType>();
            for (SchemaType type : SchemaType.values()) {
                for (String name : type.schemaNames) {
                    types.put(name, type);
                }
            }
        }

        public static SchemaType getSchemaType(String schemaName)
        {
            return types.get(schemaName);
        }

        private String schemaName;
        private String [] schemaNames;

        private SchemaType(String schemaName)
        {
            this.schemaName = schemaName;
            this.schemaNames = new String[] { schemaName };
        }

        private SchemaType(String [] schemaNames)
        {
            this.schemaName = schemaNames[0];
            this.schemaNames = schemaNames;
        }

        public String getSchemaName()
        {
            return schemaName;
        }
    }

    //~ Instance fields --------------------------------------------------------

    private Properties props;
    private String directory, logDirectory;
    private String fileExtension, controlFileExtension;
    private char fieldDelimiter, lineDelimiter;
    private char quoteChar, escapeChar;
    private boolean withHeader, withLogging;
    private int numRowsScan;
    private boolean lenient, trim, mapped;
    private String dateFormat, timeFormat, timestampFormat;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs an uninitialized FlatFileParams. The method <code>
     * decode()</code> must be called before the object is used.
     *
     * @param props foreign server parameters
     */
    public FlatFileParams(Properties props)
    {
        this.props = props;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * The main entry point into paremter decoding. Throws an exception when
     * there are error parsing parameters.
     *
     * @throws SQLException
     */
    public void decode()
        throws SQLException
    {
        directory =
            decodeDirectory(
                props.getProperty(PROP_DIRECTORY, null));
        fileExtension =
            decodeExtension(
                props.getProperty(
                    PROP_FILE_EXTENSION,
                    DEFAULT_FILE_EXTENSION));
        controlFileExtension =
            decodeExtension(
                props.getProperty(
                    PROP_CONTROL_FILE_EXTENSION,
                    DEFAULT_CONTROL_FILE_EXTENSION));
        fieldDelimiter =
            decodeDelimiter(
                props.getProperty(
                    PROP_FIELD_DELIMITER,
                    DEFAULT_FIELD_DELIMITER));
        lineDelimiter =
            decodeDelimiter(
                props.getProperty(
                    PROP_LINE_DELIMITER,
                    DEFAULT_LINE_DELIMITER));
        quoteChar =
            decodeSpecialChar(
                props.getProperty(PROP_QUOTE_CHAR, DEFAULT_QUOTE_CHAR),
                DEFAULT_QUOTE_CHAR);
        escapeChar =
            decodeSpecialChar(
                props.getProperty(PROP_ESCAPE_CHAR, DEFAULT_ESCAPE_CHAR),
                DEFAULT_ESCAPE_CHAR);
        withHeader =
            getBooleanProperty(
                props,
                PROP_WITH_HEADER,
                DEFAULT_WITH_HEADER);
        withLogging =
            getBooleanProperty(
                props,
                PROP_WITH_LOGGING,
                DEFAULT_WITH_LOGGING);
        numRowsScan =
            getIntProperty(
                props,
                PROP_NUM_ROWS_SCAN,
                DEFAULT_NUM_ROWS_SCAN);
        logDirectory =
            decodeDirectory(
                props.getProperty(PROP_LOG_DIRECTORY, null));
        lenient =
            getBooleanProperty(
                props,
                PROP_LENIENT,
                DEFAULT_LENIENT);
        trim =
            getBooleanProperty(
                props,
                PROP_TRIM,
                DEFAULT_TRIM);
        mapped =
            getBooleanProperty(
                props,
                PROP_MAPPED,
                DEFAULT_MAPPED);
        dateFormat =
            decodeDatetimeFormat(
                props.getProperty(PROP_DATE_FORMAT));
        timeFormat =
            decodeDatetimeFormat(
                props.getProperty(PROP_TIME_FORMAT));
        timestampFormat =
            decodeDatetimeFormat(
                props.getProperty(PROP_TIMESTAMP_FORMAT));
    }

    /**
     * Decodes a delimiter string into a canonical delimiter character. This
     * method recognizes the escape sequences \t, \r, \n. Combinations of the
     * two line characters \r and \n are all reduced to universal line character
     * \n.
     *
     * <p>This function comes from legacy code and is based on odd heuristics.
     *
     * <ul>
     * <li>An initial double quote denotes no delimiter.</li>
     * <li>A tab escape "\t" becomes a tab.</li>
     * <li>Otherwise preference is given to the line characters sequences "\r"
     * and "\n". These escapes are recognized from the 0, 1, and 2 index
     * positions.</li>
     * <li>The backslash character cannot escape any other character, and itself
     * becomes a delimiter.</li>
     * <li>Any other character becomes a delimiter.</li>
     * </ul>
     *
     * <p>REVIEW: this behavior seems overly complex and awkward
     *
     * @param delim delimiter string
     *
     * @return canonical delimiter character represented by delimiter string
     */
    private char decodeDelimiter(String delim)
    {
        if ((delim == null)
            || (delim.length() == 0)
            || (delim.charAt(0) == '"'))
        {
            return 0;
        }
        if (delim.indexOf("\\t") == 0) {
            return '\t';
        }
        int a = delim.indexOf("\\r");
        int b = delim.indexOf("\\n");
        if ((a == 0)
            || (a == 1)
            || (a == 2)
            || (b == 0)
            || (b == 1)
            || (b == 2))
        {
            return '\n';
        }

        // Windows parsing seems to directly provide carriage returns;
        // replace these with newline too.
        if (delim.charAt(0) == '\r') {
            return '\n';
        }
        return delim.charAt(0);
    }

    /**
     * Decodes a quote or escape character. While a null value is converted to
     * the default character, an empty value is interpreted as 0.
     *
     * @param specialChar string containing special character, may be empty
     * @param defaultChar default string, must not be empty
     *
     * @return the first character of specialChar or defaultChar
     */
    private char decodeSpecialChar(String specialChar, String defaultChar)
    {
        if (specialChar == null) {
            Util.pre(defaultChar.length() > 0,
                "defaultChar.length() > 0");
            return defaultChar.charAt(0);
        } else if (specialChar.length() == 0) {
            return 0;
        }
        return specialChar.charAt(0);
    }

    /**
     * Decodes a directory name into a useful format. If the name is null,
     * returns an empty string. Otherwise, ensures the directory name ends with
     * File.separator.
     *
     * @param directory directory name, may be null
     *
     * @return empty string or directory name, ending with File.separator
     */
    private String decodeDirectory(String directory)
    {
        if (directory == null) {
            return "";
        }

        // REVIEW jvs 27-Apr-2006:  I put in the explicit slash check
        // to allow unit tests to specify a trailing slash; this
        // works on Windows, which is forgiving about slash direction,
        // but on other platforms we'll need to do something
        // about the error messages in the unit tests.
        if (directory.endsWith(File.separator) || directory.endsWith("/")) {
            return directory;
        }
        return directory + File.separator;
    }

    private String decodeExtension(String extension)
    {
        Util.pre(extension != null, "extension != null");
        if ((extension.length() == 0)
            || extension.startsWith(FILE_EXTENSION_PREFIX))
        {
            return extension;
        }
        return FILE_EXTENSION_PREFIX + extension;
    }

    private String decodeDatetimeFormat(String format)
    {
        if (format == null) {
            return null;
        }
        try {
            DateTimeUtil.checkDateFormat(format);
        } catch (IllegalArgumentException ex) {
            throw EigenbaseResource.instance().InvalidDatetimeFormat.ex(
                format,
                ex);
        }
        return format;
    }

    public String getDirectory()
    {
        return directory;
    }

    public String getFileExtenstion()
    {
        return fileExtension;
    }

    public String getControlFileExtenstion()
    {
        return controlFileExtension;
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

    public int getNumRowsScan()
    {
        return numRowsScan;
    }

    public boolean getWithLogging()
    {
        return withLogging;
    }

    public String getLogDirectory()
    {
        return logDirectory;
    }

    public String getDateFormat()
    {
        return dateFormat;
    }

    public String getTimeFormat()
    {
        return timeFormat;
    }

    public String getTimestampFormat()
    {
        return timestampFormat;
    }

    public boolean getLenient()
    {
        return lenient;
    }

    public boolean getTrim()
    {
        return trim;
    }

    public boolean getMapped()
    {
        return mapped;
    }

    /**
     * Lookup the type of a schema based upon it's schema name. The queryDefault
     * parameter allows the type to default to QUERY when the schema name is
     * unrecognized. This is useful for foreign tables, which use a local schema
     * name.
     *
     * @param schemaName name of schema to lookup
     * @param queryDefault whether to make a query the default type
     */
    public static SchemaType getSchemaType(
        String schemaName,
        boolean queryDefault)
    {
        SchemaType type = SchemaType.getSchemaType(schemaName);
        if ((type == null) && queryDefault) {
            type = SchemaType.QUERY;
        }
        return type;
    }
}

// End FlatFileParams.java
