/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
// Portions Copyright (C) 2003-2007 John V. Sichi
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
import java.util.logging.*;

import javax.sql.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FlatFileDataServer provides an implementation of the {@link
 * FarragoMedDataServer} interface.
 *
 * @author John Pham
 * @version $Id$
 */
class FlatFileDataServer
    extends MedAbstractDataServer
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getClassTracer(FlatFileDataServer.class);

    private static int DESCRIBE_COLUMN_LENGTH = 2048;
    private static String DESCRIBE_COLUMN_NAME = "FIELD_SIZES";
    private static String QUALIFIED_NAME_SEPARATOR = ".";
    private static String SQL_QUOTE_CHARACTER = "\"";

    //~ Instance fields --------------------------------------------------------

    private MedAbstractDataWrapper wrapper;
    FlatFileParams params;

    //~ Constructors -----------------------------------------------------------

    FlatFileDataServer(
        MedAbstractDataWrapper wrapper,
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
        this.wrapper = wrapper;
    }

    //~ Methods ----------------------------------------------------------------

    void initialize()
        throws SQLException
    {
        params = new FlatFileParams(getProperties());
        params.decode();

        // throw an error if directory doesn't exist
        File dir = new File(params.getDirectory());
        if (!dir.exists() && !params.getDirectory().equals("")) {
            throw FarragoResource.instance().InvalidDirectory.ex(
                params.getDirectory());
        }

        // throw an error when using fixed position parsing mode
        // with incompatible parameters
        if (params.getFieldDelimiter() == 0) {
            if ((params.getQuoteChar() != 0)
                || (params.getEscapeChar() != 0))
            {
                throw FarragoResource.instance().FlatFileInvalidFixedPosParams
                .ex();
            }
        }

        if (params.getMapped() && (params.getWithHeader() == false)) {
            throw FarragoResource.instance().FlatFileMappedRequiresWithHeader
            .ex();
        }

        if (params.getMapped() && (params.getLenient() == false)) {
            throw FarragoResource.instance().FlatFileMappedRequiresLenient.ex();
        }
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        // scan directory and files for metadata (Phase II)
        return new FlatFileNameDirectory(
            this,
            FarragoMedMetadataQuery.OTN_SCHEMA);
    }

    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map<String, Properties> columnPropMap)
        throws SQLException
    {
        String schemaName = getSchemaName(localName);
        FlatFileParams.SchemaType schemaType =
            FlatFileParams.getSchemaType(schemaName, true);

        String filename =
            tableProps.getProperty(
                FlatFileColumnSet.PROP_FILENAME);
        if (filename == null) {
            filename = getTableName(localName);
        }

        String dataFilePath =
            params.getDirectory() + filename
            + params.getFileExtenstion();
        File dataFile = new File(dataFilePath);

        // Estimate number of rows in a file
        long numRows = -1;
        try {
            if (schemaType == FlatFileParams.SchemaType.QUERY) {
                String [] foreignName =
                    {
                        this.getProperties().getProperty("NAME"),
                        FlatFileParams.SchemaType.QUERY.getSchemaName(),
                        filename
                    };
                if (params.getNumRowsScan() > 0) {
                    long avgRowSize = sampleAndCreateBcp(foreignName, null);
                    // Estimated number of rows == file length / avg row length
                    if (avgRowSize > 0) {
                        numRows = dataFile.length() / avgRowSize;
                    }
                }
            }
        } catch (Exception e) {
            // REVIEW jvs 17-Oct-2008:  swallowing exceptions without
            // explanation is a bad idea
        }

        if (rowType == null) {
            // scan control file/data file for metadata (Phase II)
            // check data file exists
            if (!dataFile.exists()) {
                return null;
            }
            String ctrlFilePath =
                params.getDirectory() + filename
                + params.getControlFileExtenstion();
            FlatFileBCPFile bcpFile =
                new FlatFileBCPFile(ctrlFilePath, typeFactory);
            rowType =
                deriveRowType(
                    typeFactory,
                    schemaType,
                    localName,
                    filename,
                    bcpFile);
        }
        if (rowType == null) {
            return null;
        }
        return new FlatFileColumnSet(
            localName,
            rowType,
            params,
            tableProps,
            numRows,
            schemaType);
    }

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param)
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        super.closeAllocation();
    }

    MedAbstractDataWrapper getWrapper()
    {
        return wrapper;
    }

    RelDataType createRowType(
        FarragoTypeFactory typeFactory,
        RelDataType [] types,
        String [] names)
    {
        return typeFactory.createStructType(types, names);
    }

    /**
     * Derives the row type of a table when other type information is not
     * available. Also derives the row type of internal queries.
     *
     * @throws SQLException
     */
    private RelDataType deriveRowType(
        FarragoTypeFactory typeFactory,
        FlatFileParams.SchemaType schemaType,
        String [] localName,
        String filename,
        FlatFileBCPFile bcpFile)
        throws SQLException
    {
        List<RelDataType> fieldTypes = new ArrayList<RelDataType>();
        List<String> fieldNames = new ArrayList<String>();
        String [] foreignName =
        {
            this.getProperties().getProperty("NAME"),
            FlatFileParams.SchemaType.QUERY.getSchemaName(),
            filename
        };

        // Cannot describe or sample a fixed position data file
        if (params.getFieldDelimiter() == 0) {
            switch (schemaType) {
            case DESCRIBE:
            case SAMPLE:
                throw FarragoResource.instance().FlatFileNoFixedPosSample.ex(
                    filename);
            }
        }

        switch (schemaType) {
        case DESCRIBE:
            fieldTypes.add(
                FlatFileBCPFile.forceSingleByte(
                    typeFactory,
                    typeFactory.createSqlType(
                        SqlTypeName.VARCHAR,
                        DESCRIBE_COLUMN_LENGTH)));
            fieldNames.add(DESCRIBE_COLUMN_NAME);
            break;
        case SAMPLE:
            List<Integer> fieldSizes = getFieldSizes(foreignName);
            int i = 1;
            for (Integer size : fieldSizes) {
                RelDataType type =
                    typeFactory.createSqlType(
                        SqlTypeName.VARCHAR,
                        size.intValue());
                RelDataType nullableType =
                    typeFactory.createTypeWithNullability(type, true);
                nullableType = FlatFileBCPFile.forceSingleByte(
                    typeFactory,
                    nullableType);
                fieldTypes.add(nullableType);
                fieldNames.add("COL" + i++);
            }
            break;
        case QUERY:
            synchronized (FlatFileBCPFile.class) {
                if (!bcpFile.exists()) {
                    if (sampleAndCreateBcp(foreignName, bcpFile) == -1) {
                        return null;
                    }
                }
                if (bcpFile.parse()) {
                    return createRowType(
                        typeFactory,
                        bcpFile.types,
                        bcpFile.colNames);
                }

                // couldn't parse control file
                return null;
            }
        default:
            return null;
        }
        return typeFactory.createStructType(fieldTypes, fieldNames);
    }

    /**
     * Returns the sizes of a flat file's fields, based upon an internal
     * describe query.
     */
    private List<Integer> getFieldSizes(String [] localName)
        throws SQLException
    {
        // Attempt to issue a loopback query into Farrago to
        // get the number of rows to produce.
        DataSource loopbackDataSource = getLoopbackDataSource();
        Connection connection = null;
        if (loopbackDataSource != null) {
            try {
                connection = loopbackDataSource.getConnection();
                Statement stmt = connection.createStatement();
                String sql = getDescribeQuery(localName);
                ResultSet resultSet = stmt.executeQuery(sql);
                if (resultSet.next()) {
                    String result = resultSet.getString(1);
                    StringTokenizer parser = new StringTokenizer(result);
                    List<Integer> sizes = new ArrayList<Integer>();

                    while (parser.hasMoreTokens()) {
                        String token = parser.nextToken();
                        Integer columnSize;
                        try {
                            columnSize = Integer.valueOf(token);
                        } catch (NumberFormatException e) {
                            throw Util.newInternal(
                                "failed to parse sample desc: '" + result
                                + "'");
                        }
                        sizes.add(columnSize);
                    }
                    return sizes;
                }
            } finally {
                // It's OK not to clean up stmt and resultSet;
                // connection.close() will do that for us.
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException ignore) {
                        tracer.severe("could not close connection");
                    }
                }
            }
        }
        return null;
    }

    /**
     * Creates the given control file based on an internal sample query.
     */
    public long sampleAndCreateBcp(
        String [] localName,
        FlatFileBCPFile bcpFile)
        throws SQLException
    {
        // Attempt to issue a loopback query into Farrago to
        // get sample data back
        DataSource loopbackDataSource = getLoopbackDataSource();
        Connection connection = null;
        if (loopbackDataSource != null) {
            try {
                connection = loopbackDataSource.getConnection();
                Statement stmt = connection.createStatement();
                String sql = getSampleQuery(localName);
                ResultSet resultSet = stmt.executeQuery(sql);
                ResultSetMetaData rsmeta = resultSet.getMetaData();

                String [] cols = new String[rsmeta.getColumnCount()];
                String [] numRows =
                { Integer.toString(rsmeta.getColumnCount()) };

                if (bcpFile != null) {
                    if (!bcpFile.create()) { // write version
                        throw FarragoResource.instance().FileWriteFailed.ex(
                            bcpFile.fileName);
                    }
                    if (!bcpFile.write(numRows, null)) { // write numCols
                        throw FarragoResource.instance().FileWriteFailed.ex(
                            bcpFile.fileName);
                    }
                }
                boolean skipNext = params.getWithHeader();
                long sumRows = 0;
                long numRowsScan = 1;
                while (resultSet.next()) {
                    numRowsScan++;
                    for (int j = 0; j < cols.length; j++) {
                        cols[j] = resultSet.getString(j + 1);
                    }
                    for (String col : cols) {
                        if (col != null) {
                            sumRows += col.length();
                        }
                    }
                    // add one per column for delimiter size
                    sumRows += cols.length;

                    if (bcpFile != null) {
                        if (skipNext) {
                            skipNext = false;
                            bcpFile.update(cols, true);
                        } else {
                            bcpFile.update(cols, false);
                        }
                    }
                }
                if (bcpFile != null) {
                    if (!bcpFile.write(cols, params)) {
                        throw FarragoResource.instance().FileWriteFailed.
                            ex(bcpFile.fileName);
                    }
                }
                return sumRows / numRowsScan;
            } finally {
                // It's OK not to clean up stmt and resultSet;
                // connection.close() will do that for us.
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException ignore) {
                        tracer.severe("could not close connection");
                    }
                }
            }
        }
        return -1;
    }

    /**
     * Constructs an internal query to describe the results of sampling
     */
    private String getDescribeQuery(String [] localName)
    {
        assert (localName.length == 3);
        String [] newName =
            setSchemaName(
                localName,
                FlatFileParams.SchemaType.DESCRIBE.getSchemaName());
        return "select * from " + getQualifiedName(newName);
    }

    /**
     * Constructs an internal query to sample
     */
    private String getSampleQuery(String [] localName)
    {
        assert (localName.length == 3);
        String [] newName =
            setSchemaName(
                localName,
                FlatFileParams.SchemaType.SAMPLE.getSchemaName());
        return "select * from " + getQualifiedName(newName);
    }

    /**
     * Constructs a qualified (multi-part) name
     */
    private String getQualifiedName(String [] localName)
    {
        String qual = quoteName(localName[0]);
        for (int i = 1; i < localName.length; i++) {
            qual += QUALIFIED_NAME_SEPARATOR + quoteName(localName[i]);
        }
        return qual;
    }

    /**
     * Constructs a quoted name
     */
    private String quoteName(String name)
    {
        return SQL_QUOTE_CHARACTER + name + SQL_QUOTE_CHARACTER;
    }

    /**
     * Returns the last name of localName. TODO: move this into a better place
     */
    private String getTableName(String [] localName)
    {
        assert (localName.length > 0);
        return localName[localName.length - 1];
    }

    /**
     * Returns the second to last name of localName. TODO: move this into a
     * better place
     */
    private String getSchemaName(String [] localName)
    {
        assert (localName.length > 1);
        return localName[localName.length - 2];
    }

    /**
     * Sets the second to last name of localName. TODO: move this into a better
     * place
     */
    private String [] setSchemaName(String [] localName, String schemaName)
    {
        String [] newName = localName.clone();
        assert (newName.length > 1);
        newName[newName.length - 2] = schemaName;
        return newName;
    }
}

// End FlatFileDataServer.java
