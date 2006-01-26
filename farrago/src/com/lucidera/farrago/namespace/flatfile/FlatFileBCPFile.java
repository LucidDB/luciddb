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

import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.LineNumberReader;

import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;

/**
 * FlatFileBCPFile provides a way to read from control (bcp) files
 *
 * @author Sunny Choi
 * @version $Id$
 */
class FlatFileBCPFile
{
    RelDataType[] types;
    String[] colNames;
    FileReader ctrlReader;
    LineNumberReader reader;
    FarragoTypeFactory typeFactory;
    String fileName;

    FlatFileBCPFile(String filePath, FarragoTypeFactory typeFactory)
    {
        this.fileName = filePath;
        this.typeFactory = typeFactory;
    }

    boolean parse()
    {
        try {
            this.ctrlReader = new FileReader(fileName);
            this.reader = new LineNumberReader(ctrlReader);
        } catch (FileNotFoundException fe) {
            fe.printStackTrace();
            return false;
        }

        String line;
        try {
            while((line=reader.readLine()) != null) {
                // skip line 1: version #
                if (reader.getLineNumber() == 2) {
                    int colNo = Integer.parseInt(line);
                    types = new RelDataType[colNo];
                    colNames = new String[colNo];
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
                            types[reader.getLineNumber()-3] =
                                typeFactory.createSqlType(
                                    typeName,
                                    Integer.parseInt(typeLength),
                                    0);
                        } else {
                            types[reader.getLineNumber()-3] =
                                typeFactory.createSqlType(
                                    typeName,
                                    Integer.parseInt(typeLength));
                        }
                    } else {
                        types[reader.getLineNumber()-3] =
                            typeFactory.createSqlType(typeName);
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

    static SqlTypeName convertBCPSqlToSqlType(String datatype)
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

