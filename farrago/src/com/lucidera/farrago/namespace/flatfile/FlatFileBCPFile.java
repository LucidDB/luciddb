/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.regex.Pattern;

import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

/**
 * FlatFileBCPFile provides a way to read/write from/to control (bcp) files
 *
 * @author Sunny Choi
 * @version $Id$
 */
class FlatFileBCPFile
{
    FileWriter ctrlWriter;
    RelDataType[] types;
    FarragoTypeFactory typeFactory;
    File ctrlFile;
    String fileName;

    String[] colDataType;
    String[] colDataLength;
    String[] colNames;

    private static final String NEWLINE = "\n";
    private static final String QUOTE = "\"";
    private static final String TAB = "\t";

    private static final Pattern IntegerPattern =
        Pattern.compile("^[0-9]+$");
    private static final Pattern DoublePattern =
        Pattern.compile("^[0-9]*(\\.)[0-9]+$");

    FlatFileBCPFile(String filePath, FarragoTypeFactory typeFactory)
    {
        this.fileName = filePath;
        this.typeFactory = typeFactory;
        this.ctrlFile = new File(fileName);
        this.colDataType = null;
        this.colDataLength = null;
        this.colNames = null;
    }

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
            ctrlWriter.write("6.0"+NEWLINE);
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        } finally {
            try {
                ctrlWriter.close();
            } catch (IOException ie) {
                ie.printStackTrace();
            }
        }
        return true;
    }

    /**
     * Writes the main body of the control file, given a table row
     */
    public boolean write(String[] row, FlatFileParams params)
    {
        try {
            this.ctrlWriter = new FileWriter(this.ctrlFile, true);
            if (params != null) {
                expandRowsAndWrite(row, params);
            } else {
                for (String col : row) {
                    ctrlWriter.write(col+NEWLINE);
                }
            }
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        } finally {
            try {
                ctrlWriter.close();
            } catch (IOException ie) {
                ie.printStackTrace();
            }
        }
        return true;
    }

    /**
     * Updates, if necessary, the control file's host file data types
     * and lengths, given a table row of information
     */
    public void update(String[] row, boolean isHeader)
    {
        if (isHeader) {
            this.colNames = new String[row.length];
            int numCol = 0;
            for (String col : row) {
                this.colNames[numCol] = col;
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
                String newLength= getTypeLength(col);

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
        if (origType == null) {
            return true;
        }
        if (origType.equals("SQLINT")) {
            return true;
        }
        if ((origType.equals("SQLBIGINT")) &&
            (!newType.equals("SQLINT"))) {
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
        if (origVal == null) {
            return true;
        }
        if ((Integer.valueOf(origVal).compareTo(
                 Integer.valueOf(newVal))) < 0) {
            return true;
        }
        return false;
    }

    private boolean expandRowsAndWrite(String[] row, FlatFileParams params)
    {
        for (int i=0; i<row.length; i++) {
            if (this.colDataType[i].equals("SQLVARCHAR")) {
                int varcharPrec =
                    ((((Integer.valueOf(
                            this.colDataLength[i])+128)/256)+1)*256);
                this.colDataLength[i] = Integer.toString(varcharPrec);
            }
            int colNo = i+1;
            row[i] = colNo + TAB + this.colDataType[i] + TAB + "0" + TAB +
                this.colDataLength[i] + TAB + QUOTE;

            if (i == row.length-1) {
                row[i] = row[i].concat(escape(params.getLineDelimiter()));
            } else {
                row[i] = row[i].concat(escape(params.getFieldDelimiter()));
            }
            row[i] = row[i].concat(QUOTE + TAB + colNo + TAB);

            if (this.colNames == null) {
                row[i] = row[i].concat("COLUMN" + colNo);
            } else {
                row[i] = row[i].concat(this.colNames[i]);
            }
            try {
                this.ctrlWriter.write(row[i]+NEWLINE);
            } catch (IOException ie) {
                ie.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Assumes from {@link FlatFileParams} that the only escaped
     * delimiter strings are \n and \t
     */
    private String escape(char in)
    {
        String out = String.valueOf(in).replace("\n", "\\n");
        out = out.replace("\t", "\\t");
        return out;
    }

    /**
     * Guess type of String to be one of
     * VARCHAR, FLOAT, BIGINT, INTEGER
     */
    private String getType(String in)
    {
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
        return Integer.toString(in.trim().length());
    }

    /**
     * Parses a control file for the datatypes and column names
     */
    public boolean parse()
    {
        FileReader ctrlReader;
        LineNumberReader reader;
        try {
            ctrlReader = new FileReader(fileName);
            reader = new LineNumberReader(ctrlReader);
        } catch (FileNotFoundException fe) {
            // fe.printStackTrace();
            return false;
        }

        String line;
        try {
            while((line=reader.readLine()) != null) {
                // skip line 1: version #
                if (reader.getLineNumber() == 2) {
                    int colNo = Integer.parseInt(line);
                    types = new RelDataType[colNo];
                    this.colNames = new String[colNo];
                }

                if (reader.getLineNumber() > 2) {
                    String[] bcpLine = line.split("\\s+");
                    String datatype = bcpLine[1];
                    String typeLength = bcpLine[3];
                    String colId = bcpLine[6];

                    SqlTypeName typeName =
                        convertBCPSqlToSqlType(datatype);
                    
                    if (typeName.allowsPrec()) {
                        if (typeName.allowsScale()) {
                            // TODO: how to get scale from bcp?
                            int typeLen =
                                SqlTypeName.Decimal.MAX_NUMERIC_PRECISION;
                            if (Integer.parseInt(typeLength) < typeLen) {
                                typeLen = Integer.parseInt(typeLength);
                            }
                            types[reader.getLineNumber()-3] =
                                typeFactory.createTypeWithNullability(
                                    typeFactory.createSqlType(typeName,
                                        typeLen,
                                        0),
                                    true);
                        } else {
                            int typeLen = Integer.parseInt(typeLength);
                            if ((typeName.equals(SqlTypeName.Timestamp)) ||
                                (typeName.equals(SqlTypeName.Time))) {
                                typeLen = typeName.getDefaultPrecision();
                            }
                            types[reader.getLineNumber()-3] =
                                typeFactory.createTypeWithNullability(
                                    typeFactory.createSqlType(typeName,
                                        typeLen),
                                    true);
                        }
                    } else {
                        types[reader.getLineNumber()-3] =
                            typeFactory.createTypeWithNullability(
                                typeFactory.createSqlType(typeName),
                                true);
                    }
                    colNames[reader.getLineNumber()-3] = colId;
                }
            }
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        } finally {
            try {
                reader.close();
                ctrlReader.close();
            } catch (IOException ie) {
                ie.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /**
     * Converts a BCP SQL type to one of {@link SqlTypeName}
     */
    public static SqlTypeName convertBCPSqlToSqlType(String datatype)
    {
        if (datatype.equals("SQLCHAR") || datatype.equals("SQLNCHAR")) {
            return SqlTypeName.Char;
        } else if (datatype.equals("SQLVARCHAR") ||
            datatype.equals("SQLNVARCHAR")) {
            return SqlTypeName.Varchar;
        } else if (datatype.equals("SQLBINARY")) {
            return SqlTypeName.Binary;
        } else if (datatype.equals("SQLVARBINARY")) {
            return SqlTypeName.Varbinary;
        } else if (datatype.equals("SQLDATETIME") ||
            datatype.equals("SQLDATETIM4")) {
            return SqlTypeName.Timestamp;
        } else if (datatype.equals("SQLDECIMAL") ||
            datatype.equals("SQLNUMERIC")) {
            return SqlTypeName.Decimal;
        } else if (datatype.equals("SQLINT")) {
            return SqlTypeName.Integer;
        } else if (datatype.equals("SQLBIGINT")) {
            return SqlTypeName.Bigint;
        } else if (datatype.equals("SQLSMALLINT")) {
            return SqlTypeName.Smallint;
        } else if (datatype.equals("SQLTINYINT")) {
            return SqlTypeName.Tinyint;
        } else if (datatype.equals("SQLMONEY") ||
            datatype.equals("SQLMONEY4")) {
            return SqlTypeName.Decimal;
        } else if (datatype.equals("SQLREAL")) {
            return SqlTypeName.Real;
        } else if (datatype.equals("SQLFLT4")) {
            return SqlTypeName.Float;
        } else if (datatype.equals("SQLFLT8")) {
            return SqlTypeName.Double;
        } else if (datatype.equals("SQLBIT")) {
            return SqlTypeName.Boolean;
        } else if (datatype.equals("SQLUNIQUEID")) {
            return SqlTypeName.Varchar;
        } else if (datatype.equals("SQLVARIANT")) {
            return SqlTypeName.Binary;
        } else if (datatype.equals("SQLUDT")) {
            return SqlTypeName.Varchar;
        } else { // unknown datatype
            return SqlTypeName.Varchar;
        }
    }
}

// End FlatFileBCPFile.java
