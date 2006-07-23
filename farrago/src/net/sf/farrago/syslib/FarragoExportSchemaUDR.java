/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import java.sql.*;

import java.text.*;

import java.util.*;

import net.sf.farrago.resource.*;

import org.eigenbase.util.*;


/**
 * FarragoExportSchemaUDR provides system procedures to export tables from a
 * local or foreign schema into CSV files.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class FarragoExportSchemaUDR
{

    //~ Static fields/initializers ---------------------------------------------

    private static final String QUOTE = "\"";
    private static final String TAB = "\t";
    private static final String NEWLINE = "\r\n";
    private static final String [] TABLE_TYPES =
        {
            "TABLE",
            "FOREIGN TABLE",
            "VIEW"
        };

    private static final String LOGFILE_PREFIX = "Export_";

    //~ Methods ----------------------------------------------------------------

    /**
     * Exports tables within a schema to CSV/BCP files
     *
     * @param catalog name of the catalog where schema resides, if null, default
     * catalog
     * @param schema name of local schema
     * @param exclude if true, tables matching either the table_list of the
     * table_pattern will be excluded. if false, tables will be included
     * @param table_list comma separated list of tables or null value if
     * table_pattern is being used
     * @param table_pattern table name pattern where '_' represents any single
     * character
     * @param directory the directory in which to place the exported CSV and BCP
     * files
     * @param with_bcp indicates whether BCP files should be created. If true,
     * BCP files will be created. If false, they will not be created
     */
    public static void exportSchemaToCsv(
        String catalog,
        String schema,
        boolean exclude,
        String table_list,
        String table_pattern,
        String directory,
        boolean with_bcp)
        throws SQLException
    {
        ResultSet rs = null;
        HashSet<String> tableNames = new HashSet<String>();
        HashSet<String> tableList = null;

        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        try {
            // get default catalog if catalog isn't set
            if (catalog == null) {
                catalog = conn.getCatalog();
            }

            // query db for the table names
            if ((table_list != null) && (table_pattern != null)) {
                throw FarragoResource.instance()
                .ExportSchemaSpecifyListOrPattern.ex();
            } else if ((table_list == null) && (table_pattern != null)) {
                // use table_pattern to retrieve table names
                rs =
                    conn.getMetaData().getTables(
                        catalog,
                        schema,
                        table_pattern,
                        TABLE_TYPES);

                // filter out the excluded table names
                if (exclude) {
                    HashSet<String> exTbls = new HashSet<String>();
                    while (rs.next()) {
                        exTbls.add(rs.getString(3));
                    }
                    ResultSet tempRs =
                        conn.getMetaData().getTables(
                            catalog,
                            schema,
                            "%",
                            TABLE_TYPES);

                    while (tempRs.next()) {
                        String tname = tempRs.getString(3);
                        if (!exTbls.contains(tname)) {
                            tableNames.add(tname);
                        }
                    }
                    tempRs.close();
                } else {
                    // get table names matching table_pattern
                    while (rs.next()) {
                        tableNames.add(rs.getString(3));
                    }
                }
            } else {
                // either table_list is being used or there is no filtering
                // retrive all the table names in the schema.
                rs =
                    conn.getMetaData().getTables(
                        catalog,
                        schema,
                        "%",
                        TABLE_TYPES);
                while (rs.next()) {
                    tableNames.add(rs.getString(3));
                }

                // using table_list, verify table names from table_list
                if (table_list != null) {
                    StringTokenizer strTok =
                        new StringTokenizer(
                            table_list,
                            ",");
                    StringBuilder incorrectTables = null;
                    while (strTok.hasMoreTokens()) {
                        String tblInList = strTok.nextToken().trim();
                        if (tableNames.contains(tblInList)) {
                            if (exclude) {
                                tableNames.remove(tblInList);
                            } else {
                                if (tableList == null) {
                                    tableList = new HashSet<String>();
                                }
                                tableList.add(tblInList);
                            }
                        } else {
                            // a table in the table_list is incorrect
                            if (incorrectTables == null) {
                                incorrectTables = new StringBuilder();
                                incorrectTables.append(tblInList);
                            } else {
                                incorrectTables.append(", " + tblInList);
                            }
                        }
                    }

                    if (incorrectTables != null) {
                        // table in list was incorrect, throw exception
                        throw FarragoResource.instance()
                        .ExportSchemaTableNotFound.ex(
                            incorrectTables.toString());
                    }

                    if (!exclude) {
                        tableNames = tableList;
                    }
                }
            }

            // no tables to export
            if (tableNames.isEmpty()) {
                throw FarragoResource.instance().ExportSchemaNoTables.ex(
                    catalog,
                    schema,
                    String.valueOf(exclude),
                    table_list,
                    table_pattern);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }

        // create Csv files
        toCsv(catalog, schema, directory, with_bcp, tableNames, conn);
    }

    /**
     * Exports tables within a foreign schema to CSV/BCP files
     *
     * @param foreign_server name of the foreign server
     * @param foreign_schema name of the foreign schema
     * @param exclude if true, tables matching either the table_list of the
     * table_pattern will be excluded. if false, tables will be included
     * @param table_list comma separated list of tables or null value if
     * table_pattern is being used
     * @param table_pattern table name pattern where '_' represents any single
     * character and '%' represents any sequence of zero or more characters. Set
     * to null value if table_list is being used
     * @param directory the directory in which to place the exported CSV and BCP
     * files
     * @param with_bcp indicates whether bcp files should be created. if true,
     * bcp files will be created. If false, they will not be created
     */
    public static void exportForeignSchemaToCsv(
        String foreign_server,
        String foreign_schema,
        boolean exclude,
        String table_list,
        String table_pattern,
        String directory,
        boolean with_bcp)
        throws SQLException
    {
        StringBuilder importSql = new StringBuilder();
        String tmpLocalSchema =
            "_TMP_LOCAL_SCHEMA"
            + UUID.randomUUID().toString();

        boolean tmpSchemaExists = false;

        if ((table_list != null) && (table_pattern != null)) {
            throw FarragoResource.instance().ExportSchemaSpecifyListOrPattern
            .ex();
        }

        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = conn.createStatement();

        try {
            // create temporary local schema
            try {
                stmt.executeUpdate(
                    "create schema " + QUOTE + tmpLocalSchema
                    + QUOTE);
            } catch (SQLException e) {
                throw FarragoResource.instance()
                .ExportSchemaCreateTempSchemaError.ex(
                    tmpLocalSchema,
                    e.getMessage(),
                    e);
            }
            tmpSchemaExists = true;

            importSql.append(
                "import foreign schema " + QUOTE + foreign_schema
                + QUOTE + " ");

            // if there is filtering
            if (!((table_list == null) && (table_pattern == null))) {
                if (exclude) {
                    importSql.append("except ");
                } else {
                    importSql.append("limit to ");
                }

                if (table_list != null) {
                    // remove all spaces in table list
                    // this means we can't have table names with spaces
                    table_list = table_list.replaceAll("\\s*", "");
                    importSql.append(
                        "(" + QUOTE
                        + table_list.replaceAll(",", QUOTE + "," + QUOTE)
                        + QUOTE
                        + ") ");
                } else {
                    importSql.append(
                        "table_name like '" + table_pattern
                        + "' ");
                }
            }
            importSql.append(
                "from server " + QUOTE + foreign_server + QUOTE + " into "
                + QUOTE + tmpLocalSchema + QUOTE);

            // import foreign schema into temp schema
            try {
                stmt.executeUpdate(importSql.toString());
            } catch (SQLException e) {
                throw FarragoResource.instance()
                .ExportSchemaImportForeignSchemaError.ex(
                    tmpLocalSchema,
                    e.getMessage(),
                    e);
            }

            // get table names within temp schema
            ResultSet rs =
                conn.getMetaData().getTables(
                    null,
                    tmpLocalSchema,
                    "%",
                    TABLE_TYPES);
            HashSet<String> tableNames = new HashSet<String>();
            if (rs.next() == false) {
                // no tables to export
                throw FarragoResource.instance().ExportSchemaNoTablesImported
                .ex(
                    foreign_server,
                    foreign_schema,
                    String.valueOf(exclude),
                    table_list,
                    table_pattern,
                    tmpLocalSchema);
            } else {
                tableNames = new HashSet<String>();
                tableNames.add(rs.getString(3));
                while (rs.next()) {
                    tableNames.add(rs.getString(3));
                }
            }
            rs.close();

            // create Csv files
            toCsv(null, tmpLocalSchema, directory, with_bcp, tableNames, conn);

            // drop temp schema
            try {
                stmt.executeUpdate(
                    "drop schema " + QUOTE + tmpLocalSchema
                    + QUOTE + " cascade");
            } catch (SQLException se) {
                throw FarragoResource.instance().ExportSchemaDropTempSchemaError
                .ex(
                    tmpLocalSchema,
                    se.getMessage());
            }

            tmpSchemaExists = false;
        } catch (EigenbaseException ee) {
            if (tmpSchemaExists) {
                try {
                    stmt.executeUpdate(
                        "drop schema " + QUOTE + tmpLocalSchema
                        + QUOTE + " cascade");
                } catch (SQLException se) {
                    throw FarragoResource.instance()
                    .ExportSchemaDropTempSchemaError.ex(
                        tmpLocalSchema,
                        se.getMessage(),
                        ee);
                }
                tmpSchemaExists = false;
            }
            throw ee;
        } finally {
            if (tmpSchemaExists) {
                try {
                    stmt.executeUpdate(
                        "drop schema " + QUOTE + tmpLocalSchema
                        + QUOTE + " cascade");
                } catch (SQLException ex1) {
                    // TODO: warn that we tried our best and schema still
                    // wasn't dropped. If it gets here, any previous exception
                    // will be lost.
                    throw FarragoResource.instance()
                    .ExportSchemaDropTempSchemaError.ex(
                        tmpLocalSchema,
                        ex1.getMessage(),
                        ex1);
                }
            }
            stmt.close();
            conn.close();
        }
    }

    /**
     * Helper function which creates and writes data to CSV and BCP files
     *
     * @param catalog name of catalog
     * @param schema name of local schema
     * @param directory location to write CSV and BCP files
     * @param with_bcp if true creates BCP files, if false, doesn't
     * @param tableNames HashSet with names of the table to export
     * @param conn connection to the database
     */
    private static void toCsv(
        String catalog,
        String schema,
        String directory,
        Boolean with_bcp,
        HashSet<String> tableNames,
        Connection conn)
        throws SQLException
    {
        File csvFile = null;
        File bcpFile = null;
        File logFile = null;
        FileWriter csvOut = null;
        FileWriter bcpOut = null;
        FileWriter logOut = null;

        // get rid of spaces and colons in directory name
        directory = directory.replaceAll("\\s*", "");
        directory = directory.replaceAll(":", "");

        // create export csv directory
        File csvDir = new File(directory);
        try {
            csvDir.mkdirs();
        } catch (SecurityException e) {
            throw FarragoResource.instance().ExportSchemaCreateDirFailed.ex(
                directory,
                e.getMessage(),
                e);
        }

        // create export log file
        String logFileName =
            directory + File.separator + LOGFILE_PREFIX
            + getTimestampString() + ".log";
        logFile = new File(logFileName);
        try {
            logOut = new FileWriter(logFile, false);
        } catch (IOException e) {
            throw FarragoResource.instance().ExportSchemaCreateFileWriterFailed
            .ex(
                logFileName,
                e.getMessage(),
                e);
        }

        Iterator<String> tableIter = tableNames.iterator();
        Statement stmt = conn.createStatement();
        ResultSet tblData;
        ResultSetMetaData tblMeta;

        try {
            logOut.write(
                QUOTE + "TableName" + QUOTE + TAB + QUOTE
                + "StartTime" + QUOTE + TAB + QUOTE + "Status" + QUOTE
                + TAB + QUOTE + "EndTime" + QUOTE + TAB + QUOTE + "Reason"
                + QUOTE + NEWLINE);
        } catch (IOException ie) {
            try {
                logOut.flush();
                logOut.close();
                logFile.delete();
            } catch (IOException ie2) {
                throw FarragoResource.instance().ExportSchemaFileWriterError.ex(
                    logFileName,
                    ie.getMessage() + ie2.getMessage(),
                    ie);
            }
            throw FarragoResource.instance().ExportSchemaFileWriterError.ex(
                logFileName,
                ie.getMessage(),
                ie);
        }

        // loop through the tables and output data to csv/bcp files
        while (tableIter.hasNext()) {
            String tblName = tableIter.next();

            try {
                logOut.write(tblName + TAB + getTimestampString() + TAB);

                if (catalog != null) {
                    tblData =
                        stmt.executeQuery(
                            "select * from " + QUOTE + catalog + QUOTE + "."
                            + QUOTE + schema + QUOTE + "." + QUOTE + tblName
                            + QUOTE);
                } else {
                    tblData =
                        stmt.executeQuery(
                            "select * from " + QUOTE + schema + QUOTE + "."
                            + QUOTE + tblName + QUOTE);
                }

                tblMeta = tblData.getMetaData();
            } catch (SQLException se) {
                try {
                    logOut.write(
                        "FAIL" + TAB + getTimestampString() + TAB
                        + se.toString() + NEWLINE);
                    logOut.flush();
                } catch (IOException ie) {
                    throw FarragoResource.instance().ExportSchemaFileWriterError
                    .ex(
                        logFileName,
                        ie.getMessage(),
                        ie);
                }
                continue;
            } catch (IOException ie) {
                try {
                    logOut.flush();
                    logOut.close();
                } catch (IOException ie2) {
                    throw FarragoResource.instance().ExportSchemaFileWriterError
                    .ex(
                        logFileName,
                        ie.getMessage() + ie2.getMessage(),
                        ie);
                }
                throw FarragoResource.instance().ExportSchemaFileWriterError.ex(
                    logFileName,
                    ie.getMessage(),
                    ie);
            }

            String csvName = directory + File.separator + tblName + ".txt";
            String bcpName = directory + File.separator + tblName + ".bcp";
            boolean tableFailed = false;
            try {
                csvFile = new File(csvName);
                csvOut = new FileWriter(csvFile, false);
                int numCols = tblMeta.getColumnCount();
                if (with_bcp) {
                    // write BCP header
                    bcpFile = new File(bcpName);
                    bcpOut = new FileWriter(bcpFile, false);

                    // version using BroadBase
                    bcpOut.write("6.0" + NEWLINE);
                    bcpOut.write(numCols + NEWLINE);
                }

                for (int i = 1; i <= numCols; i++) {
                    // write column names to CSV, tab separated, quoted
                    csvOut.write(QUOTE + tblMeta.getColumnName(i));
                    if (i != numCols) {
                        csvOut.write(QUOTE + TAB);
                    } else {
                        csvOut.write(QUOTE + NEWLINE);
                    }

                    // write bcp file
                    if (with_bcp) {
                        bcpOut.write(getBcpLine(i, tblMeta));
                    }
                }

                if (with_bcp) {
                    bcpOut.flush();
                }

                // write the csv file
                while (tblData.next()) {
                    for (int i = 1; i <= numCols; i++) {
                        String field = tblData.getString(i);
                        if (field == null) {
                            if (i != numCols) {
                                csvOut.write(TAB);
                            } else {
                                csvOut.write(NEWLINE);
                            }
                        } else {
                            csvOut.write(QUOTE);

                            // quote the quotes
                            csvOut.write(quote(field));

                            if (i != numCols) {
                                csvOut.write(QUOTE + TAB);
                            } else {
                                csvOut.write(QUOTE + NEWLINE);
                            }
                        }
                    }
                }

                // log success
                try {
                    logOut.write(
                        "PASS" + TAB + getTimestampString() + TAB
                        + "None" + NEWLINE);
                    logOut.flush();
                } catch (IOException ie) {
                    try {
                        logOut.flush();
                        logOut.close();
                    } catch (IOException ie2) {
                        throw FarragoResource.instance()
                        .ExportSchemaFileWriterError.ex(
                            logFileName,
                            ie.getMessage() + ie2.getMessage(),
                            ie);
                    }
                    throw FarragoResource.instance().ExportSchemaFileWriterError
                    .ex(
                        logFileName,
                        ie.getMessage(),
                        ie);
                }
            } catch (SQLException se) {
                tableFailed = true;
                try {
                    logOut.write(
                        "FAIL" + TAB + getTimestampString() + TAB
                        + se.toString() + NEWLINE);
                    logOut.flush();
                } catch (IOException ie) {
                    try {
                        logOut.flush();
                        logOut.close();
                    } catch (IOException ie2) {
                        throw FarragoResource.instance()
                        .ExportSchemaFileWriterError.ex(
                            logFileName,
                            ie.getMessage() + ie2.getMessage(),
                            ie);
                    }
                    throw FarragoResource.instance().ExportSchemaFileWriterError
                    .ex(
                        logFileName,
                        ie.getMessage(),
                        ie);
                }
                continue;
            } catch (IOException ie) {
                tableFailed = true;
                try {
                    logOut.write(
                        "FAIL" + TAB + getTimestampString() + TAB
                        + ie.toString() + NEWLINE);
                    logOut.flush();
                } catch (IOException ie2) {
                    try {
                        logOut.flush();
                        logOut.close();
                    } catch (IOException ie3) {
                        throw FarragoResource.instance()
                        .ExportSchemaFileWriterError.ex(
                            logFileName,
                            ie2.getMessage() + ie3.getMessage(),
                            ie2);
                    }
                    throw FarragoResource.instance().ExportSchemaFileWriterError
                    .ex(
                        logFileName,
                        ie2.getMessage(),
                        ie2);
                }
                continue;
            } finally {
                tblData.close();
                if (csvOut != null) {
                    try {
                        csvOut.flush();
                        csvOut.close();
                    } catch (IOException ie) {
                        throw FarragoResource.instance()
                        .ExportSchemaFileWriterError.ex(
                            csvName,
                            ie.getMessage(),
                            ie);
                    }
                }
                if (bcpOut != null) {
                    try {
                        bcpOut.flush();
                        bcpOut.close();
                    } catch (IOException ie) {
                        throw FarragoResource.instance()
                        .ExportSchemaFileWriterError.ex(
                            bcpName,
                            ie.getMessage(),
                            ie);
                    }
                }

                // delete partial files if table export failed
                if (tableFailed) {
                    if (csvFile != null) {
                        csvFile.delete();
                    }
                    if (bcpFile != null) {
                        bcpFile.delete();
                    }
                }
            }
        }
        try {
            logOut.flush();
            logOut.close();
        } catch (IOException ie) {
            throw FarragoResource.instance().ExportSchemaFileWriterError.ex(
                logFileName,
                ie.getMessage(),
                ie);
        }
        stmt.close();
    }

    /**
     * Helper function which returns a line in BCP file format for a specified
     * column in a table. Note: this uses BCP types that aren't standard such as
     * SQLDATE and SQLTIME, SQLTIMESTAMP, SQLVARBINARY, and the extra columns
     * specifying precision and scale for SQLDECIMAL
     *
     * @param colNum column number to return BCP control data for
     * @param rsmd metadata for the table
     */
    public static String getBcpLine(int colNum, ResultSetMetaData rsmd)
        throws SQLException
    {
        String literalSep;
        if (colNum == rsmd.getColumnCount()) {
            literalSep = "\"\\r\\n\"";
        } else {
            literalSep = "\"\\t\"";
        }
        String colStr = String.valueOf(colNum);
        int colJdbcType = rsmd.getColumnType(colNum);

        switch (colJdbcType) {
        case Types.BIGINT:
            return
                colStr + TAB + "SQLBIGINT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.BINARY:
            return
                colStr + TAB + "SQLBINARY" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.BOOLEAN:
            return
                colStr + TAB + "SQLBIT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.CHAR:
            return
                colStr + TAB + "SQLCHAR" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.DATE:
            return
                colStr + TAB + "SQLDATE" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.DECIMAL:
            return
                colStr + TAB + "SQLDECIMAL" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + TAB
                + String.valueOf(rsmd.getPrecision(colNum)) + TAB
                + String.valueOf(rsmd.getScale(colNum)) + NEWLINE;
        case Types.DOUBLE:
            return
                colStr + TAB + "SQLFLT8" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.FLOAT:
            return
                colStr + TAB + "SQLFLT4" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.INTEGER:
            return
                colStr + TAB + "SQLINT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.NUMERIC:
            return
                colStr + TAB + "SQLNUMERIC" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + TAB
                + String.valueOf(rsmd.getPrecision(colNum)) + TAB
                + String.valueOf(rsmd.getScale(colNum)) + NEWLINE;
        case Types.REAL:
            return
                colStr + TAB + "SQLREAL" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.SMALLINT:
            return
                colStr + TAB + "SQLSMALLINT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.TIME:
            return
                colStr + TAB + "SQLTIME" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.TIMESTAMP:
            return
                colStr + TAB + "SQLTIMESTAMP" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.TINYINT:
            return
                colStr + TAB + "SQLTINYINT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.VARBINARY:
            return
                colStr + TAB + "SQLBINARY" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        case Types.VARCHAR:
        default:
            return
                colStr + TAB + "SQLVARCHAR" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + rsmd.getColumnName(colNum) + NEWLINE;
        }
    }

    private static String quote(String value)
    {
        return value.replaceAll("\"", "\"\"");
    }

    private static String getTimestampString()
    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        return formatter.format(new java.util.Date());
    }
}

// End FarragoExportSchemaUDR.java
