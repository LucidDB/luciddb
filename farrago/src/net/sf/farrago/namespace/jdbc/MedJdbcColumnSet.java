/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.namespace.jdbc;

import java.sql.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.namespace.impl.*;

import org.apache.commons.dbcp.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * MedJdbcColumnSet implements the {@link
 * net.sf.farrago.namespace.FarragoMedColumnSet} interface for foreign JDBC
 * tables.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedJdbcColumnSet
    extends MedAbstractColumnSet
{
    //~ Instance fields --------------------------------------------------------

    final MedJdbcNameDirectory directory;
    final SqlSelect select;
    final SqlDialect dialect;
    RelDataType origRowType;
    RelDataType srcRowType;
    RelDataType currRowType;

    //~ Constructors -----------------------------------------------------------

    public MedJdbcColumnSet(
        MedJdbcNameDirectory directory,
        String [] foreignName,
        String [] localName,
        SqlSelect select,
        SqlDialect dialect,
        RelDataType rowType,
        RelDataType origRowType,
        RelDataType srcRowType)
    {
        super(localName, foreignName, origRowType, null, null);
        this.directory = directory;
        this.select = select;
        this.dialect = dialect;
        this.srcRowType = srcRowType;
        this.origRowType = origRowType;
        this.currRowType = rowType;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptTable
    public double getRowCount()
    {
        // TODO:  use getStatistics?
        return super.getRowCount();
    }

    /**
     * @return the directory from which this columnset originates
     */
    public MedJdbcNameDirectory getDirectory()
    {
        return directory;
    }

    /**
     * @return the dialect of SQL used to access the remote DBMS
     */
    public SqlDialect getDialect()
    {
        return dialect;
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        // FRG-183
        RelNode rel = null;
        try {
            rel = optimizeLoopbackLink(cluster, connection);
        } catch (SQLException ex) {
            // REVIEW jvs 14-Aug-2006: Suppress it so that the optimization
            // attempt doesn't cause something to fail that would have worked
            // otherwise.  But maybe we should trace it?
        }

        if (rel != null) {
            return rel;
        }

        // TODO jvs 30-May-2009:  include unique key info
        rel =
            new MedJdbcQueryRel(
                directory.server,
                this,
                cluster,
                currRowType,
                connection,
                dialect,
                select);
        if (directory.server.lenient) {
            return toLenientRel(
                cluster,
                rel,
                origRowType,
                srcRowType);
        }
        return rel;
    }

    private RelNode optimizeLoopbackLink(
        RelOptCluster cluster,
        RelOptConnection connection)
        throws SQLException
    {
        String [] schemaQualifiedName = getForeignName();

        // Schema name should always be present in foreign name.
        if (schemaQualifiedName.length < 2) {
            return null;
        }

        // OK, we're ready to construct the local name of the real
        // underlying table.
        String [] actualName = new String[3];
        actualName[0] = null;
        actualName[1] = schemaQualifiedName[schemaQualifiedName.length - 2];
        actualName[2] = schemaQualifiedName[schemaQualifiedName.length - 1];
        return optimizeLoopbackLink(cluster, connection, actualName);
    }

    protected RelNode optimizeLoopbackLink(
        RelOptCluster cluster,
        RelOptConnection connection,
        String [] actualName)
        throws SQLException
    {
        if (directory == null) {
            return null;
        }
        if (directory.server == null) {
            return null;
        }
        if ((directory.server.schemaName != null)
            && !directory.server.useSchemaNameAsForeignQualifier)
        {
            // Schema name should never be specified for a connection to
            // Farrago; if it is, bail.
            return null;
        }

        Connection loopbackConnection = directory.server.getConnection();
        if (!(loopbackConnection instanceof FarragoJdbcEngineConnection)) {
            Connection conn = loopbackConnection;
            while ((conn != null) && (conn instanceof DelegatingConnection)) {
                conn = ((DelegatingConnection) conn).getDelegate();
            }
            if (!(conn instanceof FarragoJdbcEngineConnection)) {
                return null;
            }
        }

        String catalogName = directory.server.catalogName;
        if (catalogName == null) {
            // No catalog name specified, so try to query the connection for
            // it.
            catalogName = loopbackConnection.getCatalog();
            if (catalogName == null) {
                return null;
            }
        }

        if (actualName[0] == null) {
            actualName[0] = catalogName;
        }

        // REVIEW jvs 14-Aug-2006:  Security security security.
        RelOptTable realTable =
            getPreparingStmt().getTableForMember(actualName);
        if (realTable == null) {
            return null;
        }
        return realTable.toRel(cluster, connection);
    }
}

// End MedJdbcColumnSet.java
