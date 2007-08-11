/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;


/**
 * MedMockNameDirectory provides a mock implementation of the {@link
 * FarragoMedNameDirectory} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockNameDirectory
    extends MedAbstractNameDirectory
{
    //~ Static fields/initializers ---------------------------------------------

    static final String COLUMN_NAME = "MOCK_COLUMN";

    //~ Instance fields --------------------------------------------------------

    final MedMockDataServer server;

    String scope;

    //~ Constructors -----------------------------------------------------------

    MedMockNameDirectory(MedMockDataServer server, String scope)
    {
        this.server = server;
        this.scope = scope;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedNameDirectory
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String foreignName,
        String [] localName)
        throws SQLException
    {
        if (!scope.equals(FarragoMedMetadataQuery.OTN_TABLE)) {
            return null;
        }
        if (!foreignName.equals(server.getForeignTableName())) {
            return null;
        }

        return server.newColumnSet(
            localName,
            server.getProperties(),
            typeFactory,
            server.createMockRowType(typeFactory),
            Collections.EMPTY_MAP);
    }

    // implement FarragoMedNameDirectory
    public FarragoMedNameDirectory lookupSubdirectory(String foreignName)
        throws SQLException
    {
        if (scope.equals(FarragoMedMetadataQuery.OTN_SCHEMA)) {
            if (foreignName.equals(server.getForeignSchemaName())) {
                return new MedMockNameDirectory(
                    server,
                    FarragoMedMetadataQuery.OTN_TABLE);
            }
        }
        return null;
    }

    // implement FarragoMedNameDirectory
    public boolean queryMetadata(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        // NOTE:  We take advantage of the fact that we're permitted
        // to ignore query filters and return everything.

        if (scope.equals(FarragoMedMetadataQuery.OTN_SCHEMA)) {
            sink.writeObjectDescriptor(
                server.getForeignSchemaName(),
                FarragoMedMetadataQuery.OTN_SCHEMA,
                "Mock schema",
                new Properties());
        } else {
            sink.writeObjectDescriptor(
                server.getForeignTableName(),
                FarragoMedMetadataQuery.OTN_TABLE,
                "Mock table",
                new Properties());
            if (server.extractColumns) {
                sink.writeColumnDescriptor(
                    server.getForeignTableName(),
                    COLUMN_NAME,
                    0,
                    server.createMockColumnType(sink.getTypeFactory()),
                    "Mock column",
                    "0",
                    new Properties());
            }
        }
        return true;
    }
}

// End MedMockNameDirectory.java
