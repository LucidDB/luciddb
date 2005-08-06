/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.namespace.jdbc;

import java.sql.*;
import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.*;


/**
 * MedJdbcNameDirectory implements the FarragoMedNameDirectory
 * interface by mapping the metadata provided by any JDBC driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedJdbcNameDirectory extends MedAbstractNameDirectory
{
    //~ Instance fields -------------------------------------------------------

    final MedJdbcDataServer server;

    //~ Constructors ----------------------------------------------------------

    MedJdbcNameDirectory(MedJdbcDataServer server)
    {
        this.server = server;
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoMedNameDirectory
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String [] foreignName,
        String [] localName)
        throws SQLException
    {
        return lookupColumnSetAndImposeType(typeFactory, foreignName,
            localName, null);
    }

    FarragoMedColumnSet lookupColumnSetAndImposeType(
        FarragoTypeFactory typeFactory,
        String [] foreignName,
        String [] localName,
        RelDataType rowType)
        throws SQLException
    {
        SqlDialect dialect = new SqlDialect(server.databaseMetaData);
        SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();
        if (server.schemaName != null) {
            assert (foreignName.length == 2);

            // TODO jvs 11-June-2004: this should be a real error, not an
            // assert
            assert (foreignName[0].equals(server.schemaName));
            foreignName = new String [] { foreignName[1] };
        }
        SqlSelect select =
            opTab.selectOperator.createCall(
                null,
                new SqlNodeList(
                    Collections.singletonList(
                        new SqlIdentifier("*", SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                new SqlIdentifier(foreignName, SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);

        if (rowType == null) {
            String sql = select.toSqlString(dialect);
            sql = normalizeQueryString(sql);

            PreparedStatement ps = null;
            try {
                ps = server.connection.prepareStatement(sql);
            } catch (Exception ex) {
                // Some drivers don't support prepareStatement
            }
            Statement stmt = null;
            ResultSet rs = null;
            try {
                ResultSetMetaData md = null;
                try {
                    if (ps != null) {
                        md = ps.getMetaData();
                    }
                } catch (SQLException ex) {
                    // Some drivers can't return metadata before execution.
                    // Fall through to recovery below.
                }
                if (md == null) {
                    if (ps != null) {
                        rs = ps.executeQuery();
                    } else {
                        stmt = server.connection.createStatement();
                        rs = stmt.executeQuery(sql);
                    }
                    md = rs.getMetaData();
                }
                rowType = typeFactory.createResultSetType(md);
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (ps != null) {
                    ps.close();
                }
            }
        } else {
            // REVIEW:  should we at least check to see if the inferred
            // row type is compatible with the enforced row type?
        }

        return new MedJdbcColumnSet(this, foreignName, localName, select,
            dialect, rowType);
    }

    String normalizeQueryString(String sql)
    {
        // some drivers don't like multi-line SQL, so convert all
        // whitespace into plain spaces
        return sql.replaceAll("\\s", " ");
    }

    // TODO:  lookupSubdirectory, queryMetadata
}


// End MedJdbcNameDirectory.java
