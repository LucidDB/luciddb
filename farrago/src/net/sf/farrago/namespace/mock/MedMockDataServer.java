/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package net.sf.farrago.namespace.mock;

import java.sql.*;

import java.util.*;

import javax.sql.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * MedMockDataServer provides a mock implementation of the {@link
 * FarragoMedDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockDataServer
    extends MedAbstractDataServer
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String PROP_SIMULATE_BAD = "SIMULATE_BAD_CONNECTION";
    public static final String PROP_SCHEMA_NAME = "FOREIGN_SCHEMA_NAME";
    public static final String PROP_TABLE_NAME = "FOREIGN_TABLE_NAME";
    public static final String PROP_ROW_COUNT = "ROW_COUNT";
    public static final String PROP_ROW_COUNT_SQL = "ROW_COUNT_SQL";
    public static final String PROP_UDX_SPECIFIC_NAME = "UDX_SPECIFIC_NAME";
    public static final String PROP_EXECUTOR_IMPL = "EXECUTOR_IMPL";
    public static final String PROPVAL_JAVA = "JAVA";
    public static final String PROPVAL_FENNEL = "FENNEL";
    public static final String PROP_EXTRACT_COLUMNS = "EXTRACT_COLUMNS";
    public static final boolean DEFAULT_EXTRACT_COLUMNS = true;

    //~ Instance fields --------------------------------------------------------

    private MedAbstractDataWrapper wrapper;
    protected boolean extractColumns;

    //~ Constructors -----------------------------------------------------------

    MedMockDataServer(
        MedAbstractDataWrapper wrapper,
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
        this.wrapper = wrapper;

        extractColumns =
            getBooleanProperty(
                props,
                PROP_EXTRACT_COLUMNS,
                DEFAULT_EXTRACT_COLUMNS);
    }

    //~ Methods ----------------------------------------------------------------

    void initialize()
        throws SQLException
    {
        boolean simulateBadConnection =
            getBooleanProperty(
                wrapper.getProperties(),
                PROP_SIMULATE_BAD,
                false);
        if (simulateBadConnection) {
            throw new SQLException("Let's pretend something bad happened.");
        }
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        if (getForeignSchemaName() == null) {
            return null;
        }
        return new MedMockNameDirectory(
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
        if (rowType == null) {
            rowType = createMockRowType(typeFactory);
        }

        assert (rowType.getFieldList().size() == 1);
        RelDataType type = rowType.getFields()[0].getType();
        assert (!type.isNullable());
        assert (typeFactory.getClassForPrimitive(type) != null);

        // TODO jvs 5-Aug-2005:  clean up usage of server properties
        // as defaults

        long nRows = -1;
        String rowCountSql = tableProps.getProperty(PROP_ROW_COUNT_SQL);
        if (rowCountSql != null) {
            // Attempt to issue a loopback query into Farrago to
            // get the number of rows to produce.
            DataSource loopbackDataSource = getLoopbackDataSource();
            Connection connection = null;
            if (loopbackDataSource != null) {
                try {
                    connection = loopbackDataSource.getConnection();
                    Statement stmt = connection.createStatement();
                    ResultSet resultSet = stmt.executeQuery(rowCountSql);
                    if (resultSet.next()) {
                        nRows = resultSet.getLong(1);
                    }
                } finally {
                    // It's OK not to clean up stmt and resultSet;
                    // connection.close() will do that for us.
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        }

        if (nRows == -1) {
            nRows =
                getLongProperty(
                    tableProps,
                    PROP_ROW_COUNT,
                    getLongProperty(
                        getProperties(),
                        PROP_ROW_COUNT,
                        10));
        }

        String executorImpl =
            tableProps.getProperty(
                PROP_EXECUTOR_IMPL,
                getProperties().getProperty(
                    PROP_EXECUTOR_IMPL,
                    PROPVAL_JAVA));
        assert (executorImpl.equals(PROPVAL_JAVA)
            || executorImpl.equals(PROPVAL_FENNEL));

        String udxSpecificName = tableProps.getProperty(PROP_UDX_SPECIFIC_NAME);

        if (udxSpecificName != null) {
            assert (executorImpl.equals(PROPVAL_JAVA));
        }

        checkNameMatch(
            getForeignSchemaName(),
            tableProps.getProperty(PROP_SCHEMA_NAME));

        checkNameMatch(
            getForeignTableName(),
            tableProps.getProperty(PROP_TABLE_NAME));

        return new MedMockColumnSet(
            this,
            localName,
            rowType,
            nRows,
            executorImpl,
            udxSpecificName);
    }

    private void checkNameMatch(String expectedName, String actualName)
    {
        if ((expectedName != null) && (actualName != null)) {
            if (!expectedName.equals(actualName)) {
                throw FarragoResource.instance().MockForeignObjectNotFound.ex(
                    wrapper.getRepos().getLocalizedObjectName(
                        actualName));
            }
        }
    }

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param)
        throws SQLException
    {
        if (param instanceof Integer) {
            // Double the input.
            return 2 * (Integer) param;
        } else {
            return null;
        }
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

    String getForeignSchemaName()
    {
        return getProperties().getProperty(PROP_SCHEMA_NAME);
    }

    String getForeignTableName()
    {
        return getProperties().getProperty(PROP_TABLE_NAME);
    }

    MedAbstractDataWrapper getWrapper()
    {
        return wrapper;
    }

    RelDataType createMockRowType(FarragoTypeFactory typeFactory)
    {
        return typeFactory.createStructType(
            new RelDataType[] { createMockColumnType(typeFactory), },
            new String[] { MedMockNameDirectory.COLUMN_NAME });
    }

    RelDataType createMockColumnType(FarragoTypeFactory typeFactory)
    {
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
    }
}

// End MedMockDataServer.java
