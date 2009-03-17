/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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

import java.nio.charset.*;

import java.util.*;
import java.util.regex.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FlatFileBCPFile provides a way to read/write from/to control (bcp) files
 *
 * @author Sunny Choi
 * @version $Id$
 */
class FlatFileBCPFile
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String NEWLINE = "\n";
    private static final String QUOTE = "\"";
    private static final String TAB = "\t";

    private static final Pattern IntegerPattern = Pattern.compile("^[0-9]+$");
    private static final Pattern DoublePattern =
        Pattern.compile("^[0-9]*(\\.)[0-9]+$");
    private static final String EmptyLinePattern = "^\\s*$";

    //~ Enums ------------------------------------------------------------------

    public enum BcpType
    {
        SQLCHAR, SQLNCHAR, SQLVARCHAR, SQLNVARCHAR, SQLBINARY, SQLVARBINARY,
        SQLDATE, SQLTIME, SQLDATETIME, SQLDATETIM4, SQLTIMESTAMP, SQLDECIMAL,
        SQLNUMERIC, SQLMONEY, SQLMONEY4, SQLTINYINT, SQLSMALLINT, SQLINT,
        SQLBIGINT, SQLREAL, SQLFLT4, SQLFLT8, SQLBIT, SQLVARIANT, SQLUDT,
        SQLUNIQUEID
    }

    //~ Instance fields --------------------------------------------------------

    FileWriter ctrlWriter;
    RelDataType [] types;
    FarragoTypeFactory typeFactory;
    File ctrlFile;
    String fileName;

    String [] colDataType;
    String [] colDataLength;
    String [] colNames;

    //~ Constructors -----------------------------------------------------------

    FlatFileBCPFile(String filePath, FarragoTypeFactory typeFactory)
    {
        this.fileName = filePath;
        this.typeFactory = typeFactory;
        this.ctrlFile = new File(fileName);
        this.colDataType = null;
        this.colDataLength = null;
        this.colNames = null;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Checks if this file exists
     */
    public boolean exists()
    {
        return this.ctrlFile.exists();
    }

    /**
     * Starts a new bcp file, writes just the version number
     */
    public boolean create()
    {
        try {
            this.ctrlWriter = new FileWriter(this.ctrlFile, false);
            ctrlWriter.write("6.0" + NEWLINE);
        } catch (IOException ie) {
            try {
                this.ctrlWriter.close();
                this.ctrlFile.delete();
            } catch (IOException ex) {
            }
            throw FarragoResource.instance().FileWriteFailed.ex(
                this.fileName);
        } finally {
            try {
                ctrlWriter.close();
            } catch (Exception ie) {
                return false;
            }
        }
        return true;
    }

    /**
     * Writes the main body of the control file, given a table row
     */
    public boolean write(String [] row, FlatFileParams params)
    {
        try {
            this.ctrlWriter = new FileWriter(this.ctrlFile, true);
            if (params != null) {
                expandRowsAndWrite(row, params);
            } else {
                for (String col : row) {
                    ctrlWriter.write(col + NEWLINE);
                }
            }
        } catch (IOException ie) {
            try {
                this.ctrlWriter.close();
                this.ctrlFile.delete();
            } catch (IOException ex) {
            }
            throw FarragoResource.instance().FileWriteFailed.ex(
                this.fileName);
        } finally {
            try {
                ctrlWriter.close();
            } catch (Exception ie) {
                return false;
            }
        }
        return true;
    }

    /**
     * Updates, if necessary, the control file's host file data types and
     * lengths, given a table row of information
     */
    public void update(String [] row, boolean isHeader)
    {
        if (isHeader) {
            this.colNames = new String[row.length];
            int numCol = 0;
            for (String col : row) {
                this.colNames[numCol] = QUOTE + col + QUOTE;
                numCol++;
            }
        } else {
            if (this.colDataType == null) {
                this.colDataType = new String[row.length];
                this.colDataLength = new String[row.length];
            }

            int numCol = 0;
            for (String col : row) {
                String newType = getType(col);
                String newLength = getTypeLength(col);

                if (changeType(this.colDataType[numCol], newType)) {
                    this.colDataType[numCol] = newType;
                }
                if (changeLength(this.colDataLength[numCol], newLength)) {
                    this.colDataLength[numCol] = newLength;
                }
                numCol++;
            }
        }
    }

    private boolean changeType(String origType, String newType)
    {
        if (newType == null) {
            return false;
        }
        if (origType == null) {
            return true;
        }
        if (origType.equals("SQLINT")) {
            return true;
        }
        if ((origType.equals("SQLBIGINT"))
            && (!newType.equals("SQLINT")))
        {
            return true;
        }
        if (origType.equals("SQLVARCHAR")) {
            return false;
        }
        if (newType.equals("SQLVARCHAR")) {
            return true;
        }
        return false;
    }

    private boolean changeLength(String origVal, String newVal)
    {
        if (newVal == null) {
            return false;
        }
        if (origVal == null) {
            return true;
        }
        if ((Integer.valueOf(origVal).compareTo(
                    Integer.valueOf(newVal))) < 0)
        {
            return true;
        }
        return false;
    }

    private boolean expandRowsAndWrite(String [] row, FlatFileParams params)
    {
        if (this.colDataType == null) {
            String txtFile =
                this.fileName.substring(
                    0,
                    (this.fileName.length() - 4));
            try {
                this.ctrlWriter.close();
                this.ctrlFile.delete();
            } catch (IOException ie) {
            }
            throw FarragoResource.instance().CannotDeriveColumnTypes.ex(
                txtFile + params.getFileExtenstion());
        }
        for (int i = 0; i < row.length; i++) {
            if (this.colDataType[i] == null) {
                // TODO: return warning that column values were all null
                // setting column to type SQLVARCHAR/256
                this.colDataType[i] = "SQLVARCHAR";
                this.colDataLength[i] = "1";
            }
            if (this.colDataType[i].equals("SQLVARCHAR")) {
                int varcharPrec =
                    ((((Integer.valueOf(
                                        this.colDataLength[i]) + 128) / 256)
                            + 1) * 256);
                this.colDataLength[i] = Integer.toString(varcharPrec);
            }
            int colNo = i + 1;
            row[i] =
                colNo + TAB + this.colDataType[i] + TAB + "0" + TAB
                + this.colDataLength[i] + TAB + QUOTE;

            if (i == (row.length - 1)) {
                row[i] = row[i].concat(escape(params.getLineDelimiter()));
            } else {
                row[i] = row[i].concat(escape(params.getFieldDelimiter()));
            }
            row[i] = row[i].concat(QUOTE + TAB + colNo + TAB);

            if ((this.colNames == null) || (this.colNames[i] == null)) {
                row[i] = row[i].concat("COLUMN" + colNo);
            } else {
                row[i] = row[i].concat(this.colNames[i]);
            }
            try {
                this.ctrlWriter.write(row[i] + NEWLINE);
            } catch (IOException ie) {
                try {
                    this.ctrlWriter.close();
                    this.ctrlFile.delete();
                } catch (IOException ex) {
                }
                throw FarragoResource.instance().FileWriteFailed.ex(
                    this.fileName);
            }
        }
        return true;
    }

    /**
     * Assumes from {@link FlatFileParams} that the only escaped delimiter
     * strings are \n and \t
     */
    private String escape(char in)
    {
        String out = String.valueOf(in).replace("\n", "\\n");
        out = out.replace("\t", "\\t");
        return out;
    }

    /**
     * Guess type of String to be one of VARCHAR, FLOAT, BIGINT, INTEGER
     */
    private String getType(String in)
    {
        if (in == null) {
            return null;
        }
        in = in.trim();
        if (IntegerPattern.matcher(in).matches()) {
            if (Integer.valueOf(getTypeLength(in)) > 9) {
                return "SQLBIGINT";
            } else {
                return "SQLINT";
            }
        }
        if (DoublePattern.matcher(in).matches()) {
            return "SQLFLT8";
        }

        // TODO: support DATE/TIME/TIMESTAMP

        return "SQLVARCHAR";
    }

    private String getTypeLength(String in)
    {
        if (in == null) {
            return null;
        }
        return Integer.toString(in.trim().length());
    }

    /**
     * Parses a control file for the datatypes and column names
     */
    public boolean parse()
    {
        FileReader ctrlReader;
        LineNumberReader reader;
        int columnCount = 0;
        int index = 0;

        try {
            ctrlReader = new FileReader(fileName);
            reader = new LineNumberReader(ctrlReader);
        } catch (FileNotFoundException fe) {
            throw FarragoResource.instance().FileNotFound.ex(
                this.fileName);
        }

        try {
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }

                // convention is 1-indexed line numbers
                int lineNumber = reader.getLineNumber();

                // ignore empty lines
                if (line.matches(EmptyLinePattern)) {
                    continue;
                }

                // skip line 1: version #
                if (lineNumber == 1) {
                    continue;
                } else if (lineNumber == 2) {
                    try {
                        columnCount = Integer.parseInt(line);
                    } catch (NumberFormatException nfe) {
                        throw newParseError(lineNumber);
                    }
                    types = new RelDataType[columnCount];
                    colNames = new String[columnCount];
                    continue;
                }

                if (index >= columnCount) {
                    throw FarragoResource.instance()
                    .InvalidControlFileTooManyCols.ex(fileName);
                }

                String [] bcpLine = line.split("\\s+");
                String lineCopy = line;

                Vector bcpLineTmp = new Vector();
                for (int i = 0; i < bcpLine.length; i++) {
                    String currStr = bcpLine[i];
                    if (!(currStr.startsWith(QUOTE))) {
                        bcpLineTmp.add(currStr);
                        lineCopy = lineCopy.substring(currStr.length());
                    } else {
                        // e.g. column names with spaces
                        try {
                            int fromIdx = lineCopy.indexOf(currStr) + 1;
                            int endIdx = lineCopy.indexOf(QUOTE, fromIdx);
                            String quotedStr =
                                lineCopy.substring(fromIdx, endIdx);
                            lineCopy = lineCopy.substring(endIdx);
                            bcpLineTmp.add(quotedStr);
                            quotedStr = QUOTE + quotedStr + QUOTE;
                            String [] numOfSpaces = quotedStr.split("\\s+");
                            i = i + numOfSpaces.length - 1;
                        } catch (Exception ex) {
                            throw newParseError(lineNumber);
                        }
                    }
                }
                Object [] bcpLineTmpArray = bcpLineTmp.toArray();
                bcpLine = new String[bcpLineTmpArray.length];
                for (int i = 0; i < bcpLineTmpArray.length; i++) {
                    bcpLine[i] = (String) bcpLineTmpArray[i];
                }
                if (bcpLine.length < 7) {
                    throw newParseError(lineNumber);
                }
                String datatype = bcpLine[1];
                if (!(datatype.startsWith("SQL"))) {
                    throw newParseError(lineNumber);
                }
                String typeLength = bcpLine[3];
                try {
                    Integer.parseInt(typeLength);
                } catch (NumberFormatException nfe) {
                    throw newParseError(lineNumber);
                }
                String colId = bcpLine[6];

                SqlTypeName typeName = convertBCPSqlToSqlType(datatype);

                if (typeName.allowsPrec()) {
                    int typeLen = Integer.parseInt(typeLength);
                    if (typeName.allowsScale()) {
                        typeLen =
                            Math.min(
                                typeLen,
                                SqlTypeName.MAX_NUMERIC_PRECISION);
                        int typeScale = 0;
                        if (bcpLine.length > 8) {
                            try {
                                typeScale = Integer.parseInt(bcpLine[8]);
                                typeScale = Math.max(0, typeScale);
                                typeScale =
                                    Math.min(
                                        typeScale,
                                        SqlTypeName.MAX_NUMERIC_SCALE);
                            } catch (NumberFormatException ex) {
                                typeScale = 0;
                            }
                        }
                        types[index] =
                            typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(
                                    typeName,
                                    typeLen,
                                    typeScale),
                                true);
                    } else {
                        if ((typeName.equals(SqlTypeName.TIMESTAMP))
                            || (typeName.equals(SqlTypeName.TIME)))
                        {
                            typeLen = typeName.getDefaultPrecision();
                        }
                        types[index] =
                            typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(typeName,
                                    typeLen),
                                true);
                    }
                } else {
                    types[index] =
                        typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(typeName),
                            true);
                }
                colNames[index] = colId;
                index++;
            }

            if (columnCount <= 0) {
                throw FarragoResource.instance().InvalidControlFile.ex(
                    this.fileName);
            }
            if (index < columnCount) {
                throw FarragoResource.instance().InvalidControlFileTooFewCols
                .ex(this.fileName);
            }
        } catch (IOException ie) {
            throw FarragoResource.instance().FileNotFound.ex(
                this.fileName);
        } finally {
            try {
                reader.close();
                ctrlReader.close();
            } catch (Exception ie) {
                return false;
            }
        }
        return true;
    }

    public static RelDataType forceSingleByte(
        RelDataTypeFactory typeFactory,
        RelDataType type)
    {
        if (type.getSqlTypeName().getFamily() != SqlTypeFamily.CHARACTER) {
            return type;
        }
        Charset singleByteCharset = Util.getDefaultCharset();
        if (typeFactory.getDefaultCharset().equals(singleByteCharset)) {
            return type;
        }

        // For character data, flat file reader can currently only
        // deal with ISO-8859-1
        type =
            typeFactory.createTypeWithCharsetAndCollation(
                type,
                singleByteCharset,
                new SqlCollation(
                    SaffronProperties.instance().defaultCollation.get(),
                    SqlCollation.Coercibility.Implicit));
        return type;
    }

    private EigenbaseException newParseError(int line)
    {
        return FarragoResource.instance().InvalidControlFileOnRow.ex(
            fileName,
            Integer.toString(line));
    }

    /**
     * Converts a BCP SQL type to one of {@link SqlTypeName}
     */
    public static SqlTypeName convertBCPSqlToSqlType(String datatype)
    {
        BcpType bcpType;
        try {
            bcpType = BcpType.valueOf(datatype);
        } catch (IllegalArgumentException ex) {
            bcpType = BcpType.SQLVARCHAR;
        }

        switch (bcpType) {
        case SQLCHAR:
        case SQLNCHAR:
            return SqlTypeName.CHAR;
        case SQLVARCHAR:
        case SQLNVARCHAR:
            return SqlTypeName.VARCHAR;
        case SQLBINARY:
            return SqlTypeName.BINARY;
        case SQLVARBINARY:
            return SqlTypeName.VARBINARY;
        case SQLDATE:
            return SqlTypeName.DATE;
        case SQLTIME:
            return SqlTypeName.TIME;
        case SQLDATETIME:
        case SQLDATETIM4:
        case SQLTIMESTAMP:
            return SqlTypeName.TIMESTAMP;
        case SQLDECIMAL:
        case SQLNUMERIC:
        case SQLMONEY:
        case SQLMONEY4:
            return SqlTypeName.DECIMAL;
        case SQLINT:
            return SqlTypeName.INTEGER;
        case SQLBIGINT:
            return SqlTypeName.BIGINT;
        case SQLSMALLINT:
            return SqlTypeName.SMALLINT;
        case SQLTINYINT:
            return SqlTypeName.TINYINT;
        case SQLREAL:
            return SqlTypeName.REAL;
        case SQLFLT4:
            return SqlTypeName.FLOAT;
        case SQLFLT8:
            return SqlTypeName.DOUBLE;
        case SQLBIT:
            return SqlTypeName.BOOLEAN;
        case SQLVARIANT:
            return SqlTypeName.BINARY;
        case SQLUNIQUEID:
        case SQLUDT:
        default:
            return SqlTypeName.VARCHAR;
        }
    }
}

// End FlatFileBCPFile.java
