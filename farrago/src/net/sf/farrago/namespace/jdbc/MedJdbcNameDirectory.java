/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.namespace.jdbc;

import net.sf.saffron.sql.*;
import net.sf.saffron.core.*;
import net.sf.saffron.util.*;
import net.sf.saffron.ext.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.FarragoMetadataFactory;

import java.sql.*;
import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

/**
 * MedJdbcNameDirectory implements the FarragoNameDirectory
 * interface by mapping the metadata provided by any JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedJdbcNameDirectory implements FarragoNameDirectory
{
    final MedJdbcForeignDataWrapper dataWrapper;

    /**
     * Instantiate a MedJdbcNameDirectory.
     *
     * @param dataWrapper MedJdbcForeignDataWrapper from which
     * this directory was opened
     */
    MedJdbcNameDirectory(
        MedJdbcForeignDataWrapper dataWrapper)
    {
        this.dataWrapper = dataWrapper;
    }

    // implement FarragoNameDirectory
    public FarragoNamedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String [] foreignName,
        String [] localName)
        throws SQLException
    {
        return lookupColumnSetAndImposeType(
            typeFactory,foreignName,localName,null);
    }
    
    FarragoNamedColumnSet lookupColumnSetAndImposeType(
        FarragoTypeFactory typeFactory,
        String [] foreignName,
        String [] localName,
        SaffronType rowType)
        throws SQLException
    {
        SqlDialect dialect = new SqlDialect(dataWrapper.databaseMetaData);
        SqlOperatorTable opTab = SqlOperatorTable.instance();
        if (dataWrapper.schemaName != null) {
            assert(foreignName.length == 2);
            assert(foreignName[0].equals(dataWrapper.schemaName));
            foreignName = new String [] 
                {
                    foreignName[1]
                };
        }
        SqlSelect select = opTab.selectOperator.createCall(
            false,
            new SqlNodeList(
                Collections.singletonList(
                    new SqlIdentifier("*"))),
            new SqlIdentifier(foreignName,null),
            null,
            null,
            null,
            null);

        if (rowType == null) {
            String sql = select.toString(dialect);
        
            PreparedStatement ps =
                dataWrapper.connection.prepareStatement(sql);
            ResultSet rs = null;
            try {
                ResultSetMetaData md = null;
                try {
                    md = ps.getMetaData();
                } catch (SQLException ex) {
                    // Some drivers can't return metadata before execution.
                    // Fall through to recovery below.
                }
                if (md == null) {
                    rs = ps.executeQuery();
                    md = rs.getMetaData();
                }
                rowType = typeFactory.createResultSetType(md);
            } finally {
                if (rs != null) {
                    rs.close();
                }
                ps.close();
            }
        } else {
            // REVIEW:  should we at least check to see if the inferred
            // row type is compatible with the enforced row type?
        }
        
        return new MedJdbcColumnSet(
            this,
            typeFactory,
            foreignName,
            localName,
            select,
            dialect,
            rowType);
    }

    // implement FarragoNameDirectory
    public FarragoNameDirectory lookupSubdirectory(String [] foreignName)
        throws SQLException
    {
        // TODO?
        return null;
    }

    // implement FarragoNameDirectory
    public Iterator getContentsAsCwm(FarragoMetadataFactory factory)
        throws SQLException
    {
        // TODO
        return null;
    }
}

// End MedJdbcNameDirectory.java
