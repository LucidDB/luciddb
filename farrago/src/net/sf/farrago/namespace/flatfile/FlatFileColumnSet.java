/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package net.sf.farrago.namespace.flatfile;

import java.io.*;

import java.text.*;

import java.util.*;

import net.sf.farrago.fem.config.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.query.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * FlatFileColumnSet provides a flatfile implementation of the {@link
 * FarragoMedColumnSet} interface.
 *
 * @author John Pham
 * @version $Id$
 */
class FlatFileColumnSet
    extends MedAbstractColumnSet
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PROP_FILENAME = "FILENAME";
    public static final String PROP_LOG_FILENAME = "LOG_FILENAME";

    private static final String TIMESTAMP_PREFIX = "_";
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd_HH_mm_ss";

    //~ Instance fields --------------------------------------------------------

    FlatFileParams params;
    String filePath;
    String logFilePath;
    FlatFileParams.SchemaType schemaType;
    long numRows;

    //~ Constructors -----------------------------------------------------------

    FlatFileColumnSet(
        String [] localName,
        RelDataType rowType,
        FlatFileParams params,
        Properties tableProps,
        long numRows,
        FlatFileParams.SchemaType schemaType)
    {
        super(localName, null, rowType, null, null);

        this.params = params;
        this.numRows = numRows;
        filePath =
            makeFilePath(
                localName,
                tableProps.getProperty(PROP_FILENAME, null));
        logFilePath =
            makeLogFilePath(
                tableProps.getProperty(PROP_LOG_FILENAME, null));
        this.schemaType = schemaType;
    }

    //~ Methods ----------------------------------------------------------------

    public FlatFileParams getParams()
    {
        return params;
    }

    public String getFilePath()
    {
        return filePath;
    }

    public String getLogFilePath()
    {
        return logFilePath;
    }

    // implement RelOptTable
    public double getRowCount()
    {
        if (numRows < 0) {
            return super.getRowCount();
        }
        return numRows;
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        // Implement the flat file scan as physical relations. The scan
        // relies on a calculator to convert text into typed data. This
        // calculator may either be integrated into the flat file scan
        // as a Fennel calc, or may be a separate relation, probably a
        // Java calc.
        //
        // In addition, if custom datetime formats are specified, they are
        // always implemented in a separate CalcRel (because the Fennel
        // calc only understands ISO formats).

        FlatFileProgramWriter pw =
            new FlatFileProgramWriter(
                cluster.getRexBuilder(),
                getPreparingStmt(),
                params,
                rowType);

        if (schemaType == FlatFileParams.SchemaType.QUERY) {
            RexProgram program = pw.getProgram();
            return newCalcRel(
                cluster,
                newFennelRel(
                    cluster,
                    connection,
                    FlatFileParams.SchemaType.QUERY_TEXT,
                    program.getInputRowType()),
                program);
        } else {
            return newFennelRel(cluster, connection, schemaType, rowType);
        }
    }

    /**
     * Constructs a new FlatFileFennelRel
     */
    private FennelRel newFennelRel(
        RelOptCluster cluster,
        RelOptConnection connection,
        FlatFileParams.SchemaType schemaType,
        RelDataType rowType)
    {
        return new FlatFileFennelRel(
            this,
            cluster,
            connection,
            schemaType,
            params,
            rowType);
    }

    /**
     * Constructs a new CalcRel
     */
    private CalcRel newCalcRel(
        RelOptCluster cluster,
        FennelRel child,
        RexProgram program)
    {
        return new CalcRel(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child,
            program.getOutputRowType(),
            program,
            Collections.EMPTY_LIST);
    }

    /**
     * Constructs the full path to the file for a table, based upon the server
     * directory, filename option (if specified), and the server data file
     * extension. If the filename is not specified, the local table name is used
     * instead.
     *
     * @param localName name of the table within the catalog
     * @param filename name of the file, specified in parameters
     *
     * @return full path to the data file for the table
     */
    private String makeFilePath(String [] localName, String filename)
    {
        String name = filename;
        if (name == null) {
            name = localName[localName.length - 1];
        }
        String extension = params.getFileExtenstion();
        return (params.getDirectory() + name + extension);
    }

    /**
     * Constructs the full path to the log file for a table. The path is
     * constructed from the server's log directory option, and the table's log
     * filename option. If the log directory is not specified, then the current
     * directory is used. If the log filename is not specified, then the log
     * filename will be based upon the table's filename.
     *
     * <p>Log files names are appended with a timestamp and have a .ERR
     * extension rather than the data file extension.
     */
    private String makeLogFilePath(String logFilename)
    {
        String name = logFilename;
        if (name == null) {
            // NOTE: file path must be set before calling this function
            Util.pre(filePath != null, "filePath != null");
            File file = new File(filePath); // DIR/FILE.EXT
            String root = file.getName(); // FILE.EXT
            int dot = root.lastIndexOf(FlatFileParams.FILE_EXTENSION_PREFIX);
            if (dot > 0) {
                root = root.substring(0, dot); // FILE
            }
            SimpleDateFormat formatter = new SimpleDateFormat(TIMESTAMP_FORMAT);
            String timeStamp = formatter.format(new java.util.Date());
            name =
                (root + TIMESTAMP_PREFIX + timeStamp
                    + FlatFileParams.FILE_EXTENSION_PREFIX
                    + FlatFileParams.LOG_FILE_EXTENSION);
        }
        return params.getLogDirectory() + name;
    }
}

// End FlatFileColumnSet.java
