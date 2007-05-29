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
import java.util.logging.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.session.*;
import net.sf.farrago.runtime.*;

/**
 * FarragoExportSchemaUDR provides system procedures to export tables from a
 * local or foreign schema into CSV files.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class FarragoExportSchemaUDR
{
    private static Logger tracer = FarragoTrace.getSyslibTracer();

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

    private static final int FULL_EXPORT = 0;
    private static final int INCR_EXPORT = 1;
    private static final int MERGE_EXPORT = 2;
    private static final String [] EXPORT_TYPES =
    {
        "Full",
        "Incremental",
        "Merge"
    };

    //~ Methods ----------------------------------------------------------------

    /**
     * Exports tables within a schema to flat files with BCP files
     *
     * @param catalog name of the catalog where schema resides, if null,
     * default catalog
     * @param schema name of local schema
     * @param exclude if true, tables matching either the table_list of the
     * table_pattern will be excluded. if false, tables will be included
     * @param tableList comma separated list of tables or null value if
     * table_pattern is being used
     * @param tablePattern table name pattern where '_' represents any single
     * character
     * @param directory the directory in which to place the exported CSV and
     * BCP files
     * @param withBcp indicates whether BCP files should be created. If true,
     * BCP files will be created. If false, they will not be created
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     * @param fieldDelimiter used to delimit column fields in the flat file
     * if null, defaults to tab separated
     * @param fileExtension the file extension for the created flat file, if
     * null, defaults to .txt
     * @param dateFormat format for DATE fields ({@link SimpleDateFormat})
     * @param timeFormat format for TIME fields ({@link SimpleDateFormat})
     * @param timestampFormat format for TIMESTAMP fields ({@link
     * SimpleDateFormat})
     */
    public static void exportSchemaToFile(
        String catalog,
        String schema,
        boolean exclude,
        String tableList,
        String tablePattern,
        String directory,
        boolean withBcp,
        boolean deleteFailedFiles,
        String fieldDelimiter,
        String fileExtension,
        String dateFormat,
        String timeFormat,
        String timestampFormat)
        throws SQLException
    {
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        HashSet<String> tableNames = getLocalTableNames(
            catalog,
            schema,
            exclude,
            tableList,
            tablePattern,
            conn);

        // create Csv files
        toCsv(
            FULL_EXPORT,
            catalog,
            schema,
            null,  // lastModified
            null,  // columnName
            null,  // incrCatalog
            null,  // incrSchema
            directory,
            withBcp,
            true,
            deleteFailedFiles,
            fieldDelimiter,
            fileExtension,
            dateFormat,
            timeFormat,
            timestampFormat,
            tableNames,
            null,  // querySql
            conn);
    }

    /**
     * Exports tables within a schema to flat files with BCP files
     *
     * @param catalog name of the catalog where schema resides, if null,
     * default catalog
     * @param schema name of local schema
     * @param exclude if true, tables matching either the table_list of the
     * table_pattern will be excluded. if false, tables will be included
     * @param tableList comma separated list of tables or null value if
     * table_pattern is being used
     * @param tablePattern table name pattern where '_' represents any single
     * character
     * @param directory the directory in which to place the exported CSV and
     * BCP files
     * @param withBcp indicates whether BCP files should be created. If true,
     * BCP files will be created. If false, they will not be created
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     * @param fieldDelimiter used to delimit column fields in the flat file
     * if null, defaults to tab separated
     * @param fileExtension the file extension for the created flat file, if
     * null, defaults to .txt
     */
    public static void exportSchemaToFile(
        String catalog,
        String schema,
        boolean exclude,
        String tableList,
        String tablePattern,
        String directory,
        boolean withBcp,
        boolean deleteFailedFiles,
        String fieldDelimiter,
        String fileExtension)
        throws SQLException
    {
        exportSchemaToFile(
            catalog,
            schema,
            exclude,
            tableList,
            tablePattern,
            directory,
            withBcp,
            deleteFailedFiles,
            fieldDelimiter,
            fileExtension,
            null,     // dateFormat
            null,     // timeFormat
            null);     // timestampFormat

    }


    /**
     * Exports tables within a schema to flat files with BCP files
     *
     * Older version of the export local schema UDP without file extension,
     * field delimiter and datetime format parameters.  To be eventually
     * either retired, or changed to output csv files instead of tab separated
     *
     * @param catalog name of the catalog where schema resides, if null,
     * default catalog
     * @param schema name of local schema
     * @param exclude if true, tables matching either the table_list of the
     * table_pattern will be excluded. if false, tables will be included
     * @param tableList comma separated list of tables or null value if
     * table_pattern is being used
     * @param tablePattern table name pattern where '_' represents any single
     * character
     * @param directory the directory in which to place the exported CSV and
     * BCP files
     * @param withBcp indicates whether BCP files should be created. If true,
     * BCP files will be created. If false, they will not be created
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     */
    public static void exportSchemaToCsv(
        String catalog,
        String schema,
        boolean exclude,
        String tableList,
        String tablePattern,
        String directory,
        boolean withBcp,
        boolean deleteFailedFiles)
        throws SQLException
    {
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        HashSet<String> tableNames = getLocalTableNames(
            catalog,
            schema,
            exclude,
            tableList,
            tablePattern,
            conn);

        // create Csv files
        toCsv(
            FULL_EXPORT,
            catalog,
            schema,
            null,  // lastModified
            null,  // columnName
            null,  // incrCatalog
            null,  // incrSchema
            directory,
            withBcp,
            true,
            deleteFailedFiles,
            null,  // fieldDelimiter
            null,  //fileExtension
            null,  // dateFormat
            null,  // timeFormat
            null,  // timestampFormat
            tableNames,
            null,  // querySql
            conn);
    }

    /**
     * Standard version of local schema export which always creates
     * bcp files and deletes any leftover files from failed table exports
     */
    public static void exportSchemaToCsv(
        String catalog,
        String schema,
        boolean exclude,
        String tableList,
        String tablePattern,
        String directory)
        throws SQLException
    {
        exportSchemaToCsv(
            catalog,
            schema,
            exclude,
            tableList,
            tablePattern,
            directory,
            true,  // withBcp
            true); // deleteFailedFiles
    }

    /**
     * Exports tables within a foreign schema to CSV/BCP files
     *
     * @param foreignServer name of the foreign server
     * @param foreignSchema name of the foreign schema
     * @param exclude if true, tables matching either the tableList of the
     * tablePattern will be excluded. if false, tables will be included
     * @param tableList comma separated list of tables or null value if
     * tablePattern is being used
     * @param tablePattern table name pattern where '_' represents any single
     * character and '%' represents any sequence of zero or more characters. Set
     * to null value if tableList is being used
     * @param directory the directory in which to place the exported CSV and
     * BCP files
     * @param withBcp indicates whether bcp files should be created. if true,
     * bcp files will be created. If false, they will not be created
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     */
    public static void exportForeignSchemaToCsv(
        String foreignServer,
        String foreignSchema,
        boolean exclude,
        String tableList,
        String tablePattern,
        String directory,
        boolean withBcp,
        boolean deleteFailedFiles)
        throws SQLException
    {
        exportForeignSchemaHelper(
            FULL_EXPORT,
            foreignServer,
            foreignSchema,
            exclude,
            tableList,
            tablePattern,
            null,   // lastModified
            null,   // lastModifiedColumn
            directory,
            withBcp,
            deleteFailedFiles);
    }


    /**
     * Standard version of foreign schema export which always creates
     * bcp files and deletes any leftover files from failed table exports
     */
    public static void exportForeignSchemaToCsv(
        String foreignServer,
        String foreignSchema,
        boolean exclude,
        String tableList,
        String tablePattern,
        String directory)
        throws SQLException
    {
        exportForeignSchemaToCsv(
            foreignServer,
            foreignSchema,
            exclude,
            tableList,
            tablePattern,
            directory,
            true, // withBcp
            true); // deleteFailedFiles
    }

    /**
     * Exports tables within a schema to CSV/BCP files for rows modified
     * after a specified timestamp
     *
     * @param catalog name of the catalog where schema resides, if null,
     * default catalog
     * @param schema name of local schema
     * @param exclude if true, tables matching either the tableList of the
     * tablePattern will be excluded. if false, tables will be included
     * @param tableList comma separated list of tables or null value if
     * tablePattern is being used
     * @param tablePattern table name pattern where '_' represents any single
     * character
     * @param lastModified if specified, only rows which were modified after
     * this timestamp will be written to the csv file; lastModifiedColumn
     * must be valid for this field to be used
     * @param lastModifiedColumn name of the column for last modified
     * timestamp, if not specified then all rows will be written
     * @param directory the directory in which to place the exported CSV and
     * BCP files
     * @param withBcp indicates whether BCP files should be created. If true,
     * BCP files will be created. If false, they will not be created
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     */
    public static void exportSchemaIncrementalToCsv(
        String catalog,
        String schema,
        boolean exclude,
        String tableList,
        String tablePattern,
        Timestamp lastModified,
        String lastModifiedColumn,
        String directory,
        boolean withBcp,
        boolean deleteFailedFiles)
        throws SQLException
    {
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        HashSet<String> tableNames = getLocalTableNames(
            catalog,
            schema,
            exclude,
            tableList,
            tablePattern,
            conn);


        if ((lastModified == null) || (lastModifiedColumn == null)) {
            throw FarragoResource.instance()
                .ExportSchemaSpecifyLastModified.ex();
        }

        // create Csv files
        toCsv(
            INCR_EXPORT,
            catalog,
            schema,
            lastModified,
            lastModifiedColumn,
            null,      // incrCatalog
            null,      // incrSchema
            directory,
            withBcp,
            true,
            deleteFailedFiles,
            null,      // fieldDelimiter
            null,      // fileExtension
            null,      // dateFormat
            null,      // timeFormat
            null,      // timestampFormat
            tableNames,
            null,      // querySql
            conn);
    }

    /**
     * Standard version of incremental local schema export which
     * always creates bcp files and deletes any leftover files from failed
     * table exports
     */
    public static void exportSchemaIncrementalToCsv(
        String catalog,
        String schema,
        boolean exclude,
        String tableList,
        String tablePattern,
        Timestamp lastModified,
        String lastModifiedColumn,
        String directory)
        throws SQLException
    {
        exportSchemaIncrementalToCsv(
            catalog,
            schema,
            exclude,
            tableList,
            tablePattern,
            lastModified,
            lastModifiedColumn,
            directory,
            true,   // withBcp
            true);  // deleteFailedFiles
    }

    /**
     * Exports tables within a foreign schema to CSV/BCP files for rows
     * modified after a specified timestamp
     *
     * @param foreignServer name of the foreign server
     * @param foreignSchema name of the foreign schema
     * @param exclude if true, tables matching either the tableList of the
     * tablePattern will be excluded. if false, tables will be included
     * @param tableList comma separated list of tables or null value if
     * tablePattern is being used
     * @param tablePattern table name pattern where '_' represents any single
     * character and '%' represents any sequence of zero or more characters.
     * Set to null value if tableList is being used
     * @param lastModified only rows which were modified after
     * this timestamp will be returned
     * @param lastModifiedColumn name of the column for last modified
     * timestamp; if not valid for a table, all rows will be returned
     * @param directory the directory in which to place the exported CSV and
     * BCP files
     * @param withBcp indicates whether bcp files should be created. if true,
     * bcp files will be created. If false, they will not be created
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     */
    public static void exportForeignSchemaIncrementalToCsv(
        String foreignServer,
        String foreignSchema,
        boolean exclude,
        String tableList,
        String tablePattern,
        Timestamp lastModified,
        String lastModifiedColumn,
        String directory,
        boolean withBcp,
        boolean deleteFailedFiles)
        throws SQLException
    {
        exportForeignSchemaHelper(
            INCR_EXPORT,
            foreignServer,
            foreignSchema,
            exclude,
            tableList,
            tablePattern,
            lastModified,
            lastModifiedColumn,
            directory,
            withBcp,
            deleteFailedFiles);
    }

    /**
     * Standard version of incremental foreign schema export which
     * always creates bcp files and deletes any leftover files from failed
     * table exports
     */
    public static void exportForeignSchemaIncrementalToCsv(
        String foreignServer,
        String foreignSchema,
        boolean exclude,
        String tableList,
        String tablePattern,
        Timestamp lastModified,
        String lastModifiedColumn,
        String directory)
        throws SQLException
    {
        exportForeignSchemaIncrementalToCsv(
            foreignServer,
            foreignSchema,
            exclude,
            tableList,
            tablePattern,
            lastModified,
            lastModifiedColumn,
            directory,
            true,  // withBcp
            true); // deleteFailedFiles
    }

    /**
     * Used to combine original data and incremental data.  The data from
     * the original schema which has been deleted will not be updated.  Only
     * updates and new records from the incremental schema will be.  The
     * tables in the schemas must have the same structure.
     *
     * @param origCatalog name of catalog where original data tables reside
     * @param origSchema name of schema where original data tables reside
     * @param incrCatalog name of catalog where incremental data tables reside
     * @param incrSchema name of schema where incremental data tables reside
     * @param idColumn name of the id column used to join tables
     * @param directory the directory in which to place the exported CSV and
     * BCP files
     * @param withBcp indicates whether bcp files should be created. if true,
     * bcp files will be created. If false, they will not be created
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     */
    public static void exportMergedSchemas(
        String origCatalog,
        String origSchema,
        String incrCatalog,
        String incrSchema,
        boolean exclude,
        String tableList,
        String tablePattern,
        String idColumn,
        String directory,
        boolean withBcp,
        boolean deleteFailedFiles)
        throws SQLException
    {
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        // get tables from first schema with original data
        HashSet<String> origTblNames = getLocalTableNames(
            origCatalog,
            origSchema,
            exclude,
            tableList,
            tablePattern,
            conn);

        // get tables from the 2nd schema with incremental data
        HashSet<String> incrTblNames = getLocalTableNames(
            incrCatalog,
            incrSchema,
            exclude,
            tableList,
            tablePattern,
            conn);

        // check that they match
        if (!incrTblNames.equals(origTblNames)) {
            throw FarragoResource.instance()
                .ExportSchemaMergeTablesDiffer.ex();
        }

        // call toCsv
        toCsv(
            MERGE_EXPORT,
            origCatalog,
            origSchema,
            null,   // last modified timestamp
            idColumn,
            incrCatalog,
            incrSchema,
            directory,
            withBcp,
            true,
            deleteFailedFiles,
            null,   // fieldDelimiter
            null,   // fileExtension
            null,   // dateFormat
            null,   // timeFormat
            null,   // timestampFormat
            incrTblNames,
            null,   // querySql
            conn);
    }

    /**
     * Standard version of local merge schema export which always
     * creates bcp files and deletes any leftover files from failed table
     * exports
     */
    public static void exportMergedSchemas(
        String origCatalog,
        String origSchema,
        String incrCatalog,
        String incrSchema,
        boolean exclude,
        String tableList,
        String tablePattern,
        String idColumn,
        String directory)
        throws SQLException
    {
        exportMergedSchemas(
            origCatalog,
            origSchema,
            incrCatalog,
            incrSchema,
            exclude,
            tableList,
            tablePattern,
            idColumn,
            directory,
            true,   // withBcp
            true);  // deleteFailedFiles
    }

    /**
     * Helper function which gets the set of local table names to be exported
     *
     * @param catalog name of catalog
     * @param schema name of local schema
     * @param exclude if true, tables matching either the tableList of the
     * tablePattern will be excluded. if false, tables will be included
     * @param tableList comma separated list of tables or null value if
     * tablePattern is being used
     * @param tablePattern table name pattern where '_' represents any single
     * character
     * @param conn connection to the database
     * @return HashSet of types String with the names of all the tables to be
     * exported
     */
    private static HashSet<String> getLocalTableNames(
        String catalog,
        String schema,
        boolean exclude,
        String tableList,
        String tablePattern,
        Connection conn)
        throws SQLException
    {
        ResultSet rs = null;
        HashSet<String> tableNames = new HashSet<String>();
        HashSet<String> tempTables = null;

        try {
            // get default catalog if catalog isn't set
            if (catalog == null) {
                catalog = conn.getCatalog();
            }

            // query db for the table names
            if ((tableList != null) && (tablePattern != null)) {
                throw FarragoResource.instance()
                .ExportSchemaSpecifyListOrPattern.ex();
            } else if ((tableList == null) && (tablePattern != null)) {
                // use tablePattern to retrieve table names
                rs =
                    conn.getMetaData().getTables(
                        catalog,
                        schema,
                        tablePattern,
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
                    // get table names matching tablePattern
                    while (rs.next()) {
                        tableNames.add(rs.getString(3));
                    }
                }
            } else {
                // either tableList is being used or there is no filtering
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

                // using tableList, verify table names from tableList
                if (tableList != null) {
                    StringTokenizer strTok =
                        new StringTokenizer(
                            tableList,
                            ",");
                    StringBuilder incorrectTables = null;
                    while (strTok.hasMoreTokens()) {
                        String tblInList = strTok.nextToken().trim();
                        if (tableNames.contains(tblInList)) {
                            if (exclude) {
                                tableNames.remove(tblInList);
                            } else {
                                if (tempTables == null) {
                                    tempTables = new HashSet<String>();
                                }
                                tempTables.add(tblInList);
                            }
                        } else {
                            // a table in the tableList is incorrect
                            if (incorrectTables == null) {
                                incorrectTables = new StringBuilder();
                                incorrectTables.append(tblInList);
                            } else {
                                incorrectTables.append(", ");
                                incorrectTables.append(tblInList);
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
                        tableNames = tempTables;
                    }
                }
            }

            // no tables to export
            if (tableNames.isEmpty()) {
                throw FarragoResource.instance().ExportSchemaNoTables.ex(
                    catalog,
                    schema,
                    String.valueOf(exclude),
                    tableList,
                    tablePattern);
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }

        return tableNames;
    }

    /**
     * Helper function for full and incremental exports of tables within a
     * foreign schema
     *
     * @param expType type of export
     * @param foreignServer name of the foreign server
     * @param foreignSchema name of the foreign schema
     * @param exclude if true, tables matching either the tableList of the
     * tablePattern will be excluded. if false, tables will be included
     * @param tableList comma separated list of tables or null value if
     * tablePattern is being used
     * @param tablePattern table name pattern where '_' represents any single
     * character and '%' represents any sequence of zero or more characters.
     * Set to null value if tableList is being used
     * @param lastModified if specified, only rows which were modified after
     * this timestamp will be returned; lastModifiedColumn must be valid
     * for this field to be used
     * @param lastModifiedColumn name of the column for last modified
     * timestamp, if not specified all rows will be returned
     * @param directory the directory in which to place the exported CSV and
     * BCP files
     * @param withBcp indicates whether bcp files should be created. if true,
     * bcp files will be created. If false, they will not be created
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     */
    private static void exportForeignSchemaHelper(
        int expType,
        String foreignServer,
        String foreignSchema,
        boolean exclude,
        String tableList,
        String tablePattern,
        Timestamp lastModified,
        String lastModifiedColumn,
        String directory,
        boolean withBcp,
        boolean deleteFailedFiles)
        throws SQLException
    {
        String tmpLocalSchema =
            "_TMP_LOCAL_SCHEMA"
            + UUID.randomUUID().toString();

        boolean tmpSchemaExists = false;

        if ((tableList != null) && (tablePattern != null)) {
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

            String importSql = buildImportForeignSchemaSql(
                foreignServer,
                foreignSchema,
                tableList,
                tablePattern,
                exclude,
                tmpLocalSchema);

            // import foreign schema into temp schema
            try {
                stmt.executeUpdate(importSql);
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
            if (!rs.next()) {
                // no tables to export
                throw FarragoResource.instance().ExportSchemaNoTablesImported
                .ex(
                    foreignServer,
                    foreignSchema,
                    String.valueOf(exclude),
                    tableList,
                    tablePattern,
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
            toCsv(
                expType,
                null,
                tmpLocalSchema,
                lastModified,
                lastModifiedColumn,
                null,  // incrCatalog
                null,  // incrSchema
                directory,
                withBcp,
                true,
                deleteFailedFiles,
                null,  // fieldDelimiter
                null,  // fileExtension
                null,  // dateFormat
                null,  // timeFormat
                null,  // timestampFormat
                tableNames,
                null,  // querySql
                conn);

            // drop temp schema
            try {
                stmt.executeUpdate(
                    "drop schema " + QUOTE + tmpLocalSchema
                    + QUOTE + " cascade");
            } catch (SQLException se) {
                throw FarragoResource.instance()
                    .ExportSchemaDropTempSchemaError.ex(
                        tmpLocalSchema,
                        se.getMessage());
            }

            tmpSchemaExists = false;

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
     * @param expType type of export
     * @param catalog name of catalog
     * @param schema name of local schema
     * @param lastModified only used for incremental export, rows which were
     * modified after this timestamp will be written to the csv file
     * lastModifiedColumn must be valid for this field to be used
     * @param columnName for incremental export, name of the last modified
     * timestamp column, for merge export, name of the id column
     * @param incrCatalog only valid for merge export; name of catalog for
     * incremental data
     * @param incrSchema only valid for merge export; name of schema for
     * incremental data
     * @param directory location to write CSV and BCP files
     * @param withBcp if true creates BCP files, if false, doesn't
     * @param withData if true creates data files, if false, doesn't
     * @param deleteFailedFiles if true, csv and bcp files for tables which
     * fail during export will be deleted, otherwise they will remain
     * @param dateFormat format for DATE fields ({@link SimpleDateFormat})
     * @param timeFormat format for TIME fields ({@link SimpleDateFormat})
     * @param timestampFormat format for TIMESTAMP fields ({@link
     * SimpleDateFormat})
     * @param tableNames HashSet with names of the table to export
     * @param querySql SQL query to execute (if non-null, all table-related
     * parameters should be null; and vice versa)
     * @param conn connection to the dtaabase
     */
    private static void toCsv(
        int expType,
        String catalog,
        String schema,
        Timestamp lastModified,
        String columnName,
        String incrCatalog,
        String incrSchema,
        String directory,
        boolean withBcp,
        boolean withData,
        boolean deleteFailedFiles,
        String fieldDelimiter,
        String fileExtension,
        String dateFormat,
        String timeFormat,
        String timestampFormat,
        HashSet<String> tableNames,
        String querySql,
        Connection conn)
        throws SQLException
    {
        File csvFile = null;
        File bcpFile = null;
        File logFile = null;
        Writer csvOut = null;
        Writer bcpOut = null;
        Writer logOut = null;

        boolean tracing = tracer.isLoggable(Level.FINE);

        // Expand stuff like ${FARRAGO_HOME}.
        directory = FarragoProperties.instance().expandProperties(directory);

        // create export csv directory
        File csvDir = new File(directory);
        if (querySql != null) {
            // If we're exporting a query, then directory is
            // actually pathWithoutExtension, in which case
            // we need to strip off the base filename.
            csvDir = csvDir.getParentFile();
        }
        try {
            csvDir.mkdirs();
        } catch (Throwable e) {
            throw FarragoResource.instance().ExportSchemaCreateDirFailed.ex(
                csvDir.toString(),
                e.getMessage(),
                e);
        }
        if (!csvDir.exists() || !csvDir.isDirectory()) {
            throw FarragoResource.instance().ExportSchemaCreateDirFailed.ex(
                csvDir.toString(), "mkdir");
        }

        // create export log file
        File logDir = null;
        FarragoSessionVariables sessionVariables =
            FarragoUdrRuntime.getSession().getSessionVariables();
        String logDirVarName = FarragoSessionVariables.LOG_DIR;
        if (sessionVariables.containsVariable(logDirVarName)) {
            String logDirString = sessionVariables.get(logDirVarName);
            if (logDirString != null) {
                logDir = new File(logDirString);
            }
        }

        if (logDir == null) {
            // logDir isn't set so use the same directory as csv
            logDir = csvDir;
        }

        logFile = new File(
            logDir,
            LOGFILE_PREFIX + EXPORT_TYPES[expType]
            + "_" + getTimestampString() + ".log");
        String logFileName = logFile.toString();
        try {
            logOut = new FileWriter(logFile, false);
        } catch (IOException e) {
            throw FarragoResource.instance().ExportSchemaCreateFileWriterFailed
            .ex(
                logFileName,
                e.getMessage(),
                e);
        }
        logOut = new BufferedWriter(logOut);

        Iterator<String> tableIter;
        if (tableNames != null) {
            assert(querySql == null);
            tableIter = tableNames.iterator();
        } else {
            assert(querySql != null);
            tableIter = Collections.singleton("{QUERY}").iterator();
        }
        Statement stmt = conn.createStatement();
        ResultSet tblData;
        ResultSetMetaData tblMeta;

        try {
            logOut.write(
                QUOTE + "TableName" + QUOTE + TAB + QUOTE
                + "StartTime" + QUOTE + TAB + QUOTE + "ExportType" + QUOTE
                + TAB + QUOTE + "Status" + QUOTE + TAB + QUOTE + "EndTime"
                + QUOTE + TAB + QUOTE + "Reason" + QUOTE + NEWLINE);
        } catch (IOException ie) {
            try {
                logOut.flush();
                logOut.close();
                logFile.delete();
            } catch (IOException ie2) {
                throw FarragoResource.instance()
                    .ExportSchemaFileWriterError.ex(
                        logFileName,
                        ie.getMessage() + ie2.getMessage(),
                        ie);
            }
            throw FarragoResource.instance().ExportSchemaFileWriterError.ex(
                logFileName,
                ie.getMessage(),
                ie);
        }

        // field delimiter for data file defaults to tab
        if ((fieldDelimiter == null) || (fieldDelimiter.contains("\\t"))) {
            fieldDelimiter = TAB;
        }
        // file extension for data file defaults to .txt
        if (fileExtension == null) {
            fileExtension = ".txt";
        }

        // loop through the tables and output data to csv/bcp files
        while (tableIter.hasNext()) {
            String tblName = tableIter.next();

            try {
                logOut.write(tblName + TAB + getTimestampString() + TAB);

                // if column doesn't exist in table, does a full export
                // for incremental, for merge table won't be merged
                // but will be logged in log file
                int export = checkColumnName(
                    expType,
                    catalog,
                    schema,
                    incrCatalog,
                    incrSchema,
                    tblName,
                    columnName,
                    conn);

                if (tableNames != null) {
                    querySql = buildQuerySql(
                        export,
                        catalog,
                        schema,
                        tblName,
                        lastModified,
                        columnName,
                        incrCatalog,
                        incrSchema);
                }

                logOut.write(EXPORT_TYPES[export] + TAB);

                // TODO jvs 21-Oct-2006:  For case of !withData,
                // don't bother executing query; just prepare and
                // use ResultSetMetaData

                tblData = stmt.executeQuery(querySql);
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

            String csvName;
            String bcpName;
            if (tableNames == null) {
                csvName = directory + fileExtension;
                bcpName = directory + ".bcp";
            } else {
                csvName = directory + File.separator + tblName + fileExtension;
                bcpName = directory + File.separator + tblName + ".bcp";
            }
            boolean tableFailed = false;
            try {
                if (withData) {
                    csvFile = new File(csvName);
                    csvOut = new OutputStreamWriter(
                        new FileOutputStream(csvFile), "ISO-8859-1");
                    csvOut = new BufferedWriter(csvOut);
                }
                int numCols = tblMeta.getColumnCount();
                if (withBcp) {
                    // write BCP header
                    bcpFile = new File(bcpName);
                    bcpOut = new OutputStreamWriter(
                        new FileOutputStream(bcpFile), "ISO-8859-1");
                    bcpOut = new BufferedWriter(bcpOut);

                    // version using BroadBase
                    bcpOut.write("6.0" + NEWLINE);
                    bcpOut.write(numCols + NEWLINE);
                }

                for (int i = 1; i <= numCols; i++) {
                    if (withData) {
                        // write column names to CSV, delimited and quoted
                        csvOut.write(QUOTE + tblMeta.getColumnName(i));
                        if (i != numCols) {
                            csvOut.write(QUOTE + fieldDelimiter);
                        } else {
                            csvOut.write(QUOTE + NEWLINE);
                        }
                    }

                    // write bcp file
                    if (withBcp) {
                        bcpOut.write(getBcpLine(i, tblMeta, fieldDelimiter));
                    }
                }

                if (withBcp) {
                    bcpOut.flush();
                }

                long nRows = 0;

                // write the csv file
                while (withData && tblData.next()) {
                    for (int i = 1; i <= numCols; i++) {
                        String field;

                        // format date/time/timestamp fields
                        int columnType = tblMeta.getColumnType(i);

                        if ((columnType == Types.DATE) && (dateFormat != null))
                        {
                            field = FarragoConvertDatetimeUDR.date_to_char(
                                dateFormat, tblData.getDate(i), true);
                        } else if ((columnType == Types.TIME) &&
                            (timeFormat != null))
                        {
                            field = FarragoConvertDatetimeUDR.time_to_char(
                                timeFormat, tblData.getTime(i), true);
                        } else if ((columnType == Types.TIMESTAMP) &&
                            (timestampFormat != null))
                        {
                            field =
                                FarragoConvertDatetimeUDR.timestamp_to_char(
                                    timestampFormat, tblData.getTimestamp(i),
                                    true);
                        } else {
                            // everything else
                            field = tblData.getString(i);
                        }

                        if (field == null) {
                            if (i != numCols) {
                                csvOut.write(fieldDelimiter);
                            } else {
                                csvOut.write(NEWLINE);
                            }
                        } else {
                            csvOut.write(QUOTE);

                            // quote the quotes
                            csvOut.write(quote(field));

                            if (i != numCols) {
                                csvOut.write(QUOTE + fieldDelimiter);
                            } else {
                                csvOut.write(QUOTE + NEWLINE);
                            }
                        }
                    }

                    if (tracing) {
                        if ((nRows % 100) == 0) {
                            // When trace is on, be nice to the poor little
                            // Windows users, otherwise they can't see anything
                            // until the very end because the file size only
                            // gets updated on explicit flush.  Do this on row
                            // 1 so that they'll know something's happening if
                            // it takes a long time to compute the rest.
                            // Normally, don't do any explicit flushing because
                            // it's bad for performance.
                            csvOut.flush();
                            tracer.fine("Exported row #" + nRows);
                        }
                    }
                    ++nRows;
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
                    throw FarragoResource.instance()
                        .ExportSchemaFileWriterError.ex(
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
                    throw FarragoResource.instance()
                        .ExportSchemaFileWriterError.ex(
                            logFileName,
                            ie.getMessage(),
                            ie);
                }
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
                if (tableFailed && deleteFailedFiles) {
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

        if (tableNames == null) {
            // For a successful single-query export, delete when done
            // to reduce clutter.
            logFile.delete();
        }
    }

    // TODO jvs 25-Sept-2006: Versions of two methods below in UDX form so that
    // querySql doesn't have to be quoted.  Or, better, enhance procedure
    // support to take CURSOR parameters.

    /**
     * Exports results of a single query to CSV/BCP files.
     * (Actually currently tab-separated .txt rather than CSV; see
     * FRG-197).
     *
     * @param querySql query whose result is to be executed
     * @param pathWithoutExtension location to write CSV and BCP files; this
     * should be a directory-qualified filename without an extension
     * (correct extension will be appended automatically)
     * @param withBcp if true creates BCP files, if false, doesn't
     * @param deleteFailedFiles if true, csv and bcp files will be deleted
     * if export fails, otherwise they may remain
     * rowcount
     *
     * @deprecated Use widest version instead.
     */
    public static void exportQueryToFile(
        String querySql,
        String pathWithoutExtension,
        boolean withBcp,
        boolean deleteFailedFiles)
        throws SQLException
    {
        exportQueryToFile(
            querySql,
            pathWithoutExtension,
            withBcp,
            deleteFailedFiles,
            null,               // fieldDelimiter
            null,               // fileExtension
            null,               // dateFormat
            null,               // timeFormat
            null                // timestampFormat
            );
    }

    /**
     * Exports results of a single query to CSV/BCP files.
     *
     * @param querySql query whose result is to be executed
     * @param pathWithoutExtension location to write CSV and BCP files; this
     * should be a directory-qualified filename without an extension
     * (correct extension will be appended automatically)
     * @param withBcp if true creates BCP files, if false, doesn't
     * @param deleteFailedFiles if true, csv and bcp files will be deleted
     * if export fails, otherwise they may remain
     * rowcount
     * @param fieldDelimiter used to delimit column fields in the flat file
     * if null, defaults to tab separated
     * @param fileExtension the file extension for the created flat file, if
     * null, defaults to .txt
     * @param dateFormat format for DATE fields ({@link SimpleDateFormat})
     * @param timeFormat format for TIME fields ({@link SimpleDateFormat})
     * @param timestampFormat format for TIMESTAMP fields ({@link
     * SimpleDateFormat})
     *
     * @deprecated Use widest version instead.
     */
    public static void exportQueryToFile(
        String querySql,
        String pathWithoutExtension,
        boolean withBcp,
        boolean deleteFailedFiles,
        String fieldDelimiter,
        String fileExtension,
        String dateFormat,
        String timeFormat,
        String timestampFormat)
        throws SQLException
    {
        exportQueryToFile(
            querySql,
            pathWithoutExtension,
            withBcp,
            true,
            deleteFailedFiles,
            null,               // fieldDelimiter
            null,               // fileExtension
            null,               // dateFormat
            null,               // timeFormat
            null                // timestampFormat
            );
    }

    /**
     * Exports results of a single query to CSV/BCP files.
     *
     * @param querySql query whose result is to be executed
     * @param pathWithoutExtension location to write CSV and BCP files; this
     * should be a directory-qualified filename without an extension
     * (correct extension will be appended automatically)
     * @param withBcp if true creates BCP file, if false, doesn't
     * @param withData if true creates data file, if false, doesn't
     * @param deleteFailedFiles if true, csv and bcp files will be deleted
     * if export fails, otherwise they may remain
     * rowcount
     * @param fieldDelimiter used to delimit column fields in the flat file
     * if null, defaults to tab separated
     * @param fileExtension the file extension for the created flat file, if
     * null, defaults to .txt
     * @param dateFormat format for DATE fields ({@link SimpleDateFormat})
     * @param timeFormat format for TIME fields ({@link SimpleDateFormat})
     * @param timestampFormat format for TIMESTAMP fields ({@link
     * SimpleDateFormat})
     */
    public static void exportQueryToFile(
        String querySql,
        String pathWithoutExtension,
        boolean withBcp,
        boolean withData,
        boolean deleteFailedFiles,
        String fieldDelimiter,
        String fileExtension,
        String dateFormat,
        String timeFormat,
        String timestampFormat)
        throws SQLException
    {
        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        // TODO jvs 5-Oct-2006:  implement dateFormat, timeFormat,
        // and timestampFormat

        toCsv(
            FULL_EXPORT,
            null,               // catalog
            null,               // schema
            null,               // lastModified
            null,               // columnName
            null,               // incrCatalog
            null,               // incrSchema
            pathWithoutExtension,
            withBcp,
            withData,
            deleteFailedFiles,
            fieldDelimiter,
            fileExtension,
            dateFormat,
            timeFormat,
            timestampFormat,
            null,               // tableNames
            querySql,
            conn);
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
    public static String getBcpLine(
        int colNum, ResultSetMetaData rsmd, String fieldDelim)
        throws SQLException
    {
        String literalSep;
        if (colNum == rsmd.getColumnCount()) {
            literalSep = "\"\\r\\n\"";
        } else if (fieldDelim == TAB) {
            literalSep = "\"\\t\"";
        } else {
            literalSep = "\"" + fieldDelim + "\"";
        }
        String colStr = String.valueOf(colNum);
        int colJdbcType = rsmd.getColumnType(colNum);
        String colName = rsmd.getColumnName(colNum);

        // if spaces exist within column name, quote it
        if (colName.contains(" ")) {
            colName = QUOTE + colName + QUOTE;
        }

        switch (colJdbcType) {
        case Types.BIGINT:
            return
                colStr + TAB + "SQLBIGINT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.BINARY:
            return
                colStr + TAB + "SQLBINARY" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.BOOLEAN:
            return
                colStr + TAB + "SQLBIT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.CHAR:
            return
                colStr + TAB + "SQLCHAR" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.DATE:
            return
                colStr + TAB + "SQLDATE" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.DECIMAL:
            return
                colStr + TAB + "SQLDECIMAL" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + TAB
                + String.valueOf(rsmd.getPrecision(colNum)) + TAB
                + String.valueOf(rsmd.getScale(colNum)) + NEWLINE;
        case Types.DOUBLE:
            return
                colStr + TAB + "SQLFLT8" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.FLOAT:
            return
                colStr + TAB + "SQLFLT4" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.INTEGER:
            return
                colStr + TAB + "SQLINT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.NUMERIC:
            return
                colStr + TAB + "SQLNUMERIC" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + TAB
                + String.valueOf(rsmd.getPrecision(colNum)) + TAB
                + String.valueOf(rsmd.getScale(colNum)) + NEWLINE;
        case Types.REAL:
            return
                colStr + TAB + "SQLREAL" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.SMALLINT:
            return
                colStr + TAB + "SQLSMALLINT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.TIME:
            return
                colStr + TAB + "SQLTIME" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.TIMESTAMP:
            return
                colStr + TAB + "SQLTIMESTAMP" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.TINYINT:
            return
                colStr + TAB + "SQLTINYINT" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.VARBINARY:
            return
                colStr + TAB + "SQLBINARY" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        case Types.VARCHAR:
        default:
            return
                colStr + TAB + "SQLVARCHAR" + TAB + "0" + TAB
                + rsmd.getColumnDisplaySize(colNum) + TAB + literalSep
                + TAB + colStr + TAB + colName + NEWLINE;
        }
    }


    /**
     * Helper function which escapes the quotes by quoting them
     *
     * @param value string in which to replace quotes with quoted quotes
     * @return string with all quotes escaped
     */
    private static String quote(String value)
    {
        return value.replaceAll("\"", "\"\"");
    }

    /**
     * Helper function which returns the current timestamp as a string
     *
     * @return current date and time as a string
     */
    private static String getTimestampString()
    {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        return formatter.format(new java.util.Date());
    }

    /**
     * Helper function to build sql to query tables
     */
    private static String buildQuerySql(
        int expType,
        String catalog,
        String schema,
        String tblName,
        Timestamp lastModifiedTs,
        String colName,
        String incrCatalog,
        String incrSchema)
    {
        String oldQualifiedTable;

        if (catalog != null) {
            oldQualifiedTable = QUOTE + catalog + QUOTE + "." + QUOTE + schema
                + QUOTE + "." + QUOTE + tblName + QUOTE;
        } else {
            oldQualifiedTable = QUOTE + schema + QUOTE + "." + QUOTE + tblName
                + QUOTE;
        }

        StringBuilder querySql = new StringBuilder("select * from "
            + oldQualifiedTable);

        switch (expType) {
        case FULL_EXPORT:
            // does nothing, sql complete
            break;
        case INCR_EXPORT:
            // appends where clause to filter out incremental data here
            querySql.append(" where " + QUOTE + colName + QUOTE
                + " >= TIMESTAMP'" + lastModifiedTs.toString() + "'");
            break;
        case MERGE_EXPORT:
            // gets all records which haven't been changed in the old schema
            // and all the new and updated records from the new/incr schema
            String incrQualifiedTable;
            String tempTableName1 = QUOTE + UUID.randomUUID().toString()
                + QUOTE;
            String tempTableName2 = QUOTE + UUID.randomUUID().toString()
                + QUOTE;

            if (incrCatalog != null) {
                incrQualifiedTable = QUOTE + incrCatalog + QUOTE + "." + QUOTE
                    + incrSchema + QUOTE + "." + QUOTE + tblName + QUOTE;
            } else {
                incrQualifiedTable = QUOTE + incrSchema + QUOTE + "." + QUOTE
                    + tblName + QUOTE;
            }

            // select out one set of columns only
            querySql.insert(7, tempTableName1 + ".");

            querySql.append(" " + tempTableName1 + " inner join ( select "
                 + QUOTE + colName + QUOTE + " from " + oldQualifiedTable
                + " except select " + QUOTE + colName + QUOTE + " from "
                + incrQualifiedTable + ") " + tempTableName2 + " on "
                + tempTableName1 + "." + colName + "=" + tempTableName2
                + "." + QUOTE + colName + QUOTE + ")");

            // put incremental first in union all so resulting column names
            // will be the same as the columns in incremental table
            querySql.insert(0, "select * from " + incrQualifiedTable +
                " union all (");
            break;
        default:
            // should never get here
            throw FarragoResource.instance().ExportSchemaInvalidExpType.ex(
                String.valueOf(expType));

        }
        return querySql.toString();
    }

    /**
     * Helper function to build the import foreign schema SQL used in exporting
     * from a foreign schema
     */
    private static String buildImportForeignSchemaSql(
        String foreignServer,
        String foreignSchema,
        String tableList,
        String tablePattern,
        boolean exclude,
        String tempSchema)
    {
        StringBuilder importSql = new StringBuilder();

        importSql.append(
            "import foreign schema " + QUOTE + foreignSchema + QUOTE + " ");

        // if there is filtering
        if (!((tableList == null) && (tablePattern == null))) {
            if (exclude) {
                importSql.append("except ");
            } else {
                importSql.append("limit to ");
            }

            if (tableList != null) {
                // remove all spaces in table list
                // this means we can't have table names with spaces
                tableList = tableList.replaceAll("\\s*", "");
                importSql.append(
                    "(" + QUOTE
                    + tableList.replaceAll(",", QUOTE + "," + QUOTE)
                    + QUOTE
                    + ") ");
            } else {
                importSql.append(
                    "table_name like '" + tablePattern
                    + "' ");
            }
        }
        importSql.append(
            "from server " + QUOTE + foreignServer + QUOTE + " into "
            + QUOTE + tempSchema + QUOTE);

        return importSql.toString();
    }

    /**
     * Checks the column names passed in are valid for a table; last modified
     * column for incremental export and id column for merge export
     *
     * @return export type to attempt, same as the export type passed in if
     * column exists, full export otherwise
     */
    private static int checkColumnName(int expType, String catalog,
        String schema, String incrCatalog, String incrSchema, String tableName,
        String columnName, Connection conn)
        throws SQLException
    {
        ResultSet rs;
        int exportType = expType;

        switch (expType) {
        case FULL_EXPORT:
            // does nothing, export type doesn't change for full export
            break;
        case INCR_EXPORT:
            rs = conn.getMetaData().getColumns(
                catalog,
                schema,
                tableName,
                columnName);
            if (!rs.next()) {
                // last modified column doesn't exist in table
                exportType = FULL_EXPORT;
            }
            rs.close();
            break;
        case MERGE_EXPORT:
            rs = conn.getMetaData().getColumns(
                catalog,
                schema,
                tableName,
                columnName);
            if (!rs.next()) {
                // id column doesn't exist in first table
                rs.close();
                throw new SQLException("ID column not found in table: "
                    + "catalog=" + catalog + ",schema=" + schema + ",table="
                    + tableName + ",column=" + columnName);
            }
            rs.close();
            // Merge export does a column check for the 2nd table
            rs = conn.getMetaData().getColumns(
                incrCatalog,
                incrSchema,
                tableName,
                columnName);
            if (!(rs.next())) {
                // id column doesn't exist in 2nd table
                rs.close();
                break;
            }
        default:
            // should never get here
            throw FarragoResource.instance().ExportSchemaInvalidExpType.ex(
                String.valueOf(expType));
        }

        return exportType;
    }
}

// End FarragoExportSchemaUDR.java
