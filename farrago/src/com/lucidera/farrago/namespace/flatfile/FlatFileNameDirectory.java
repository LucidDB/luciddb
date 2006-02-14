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

import java.io.File;
import java.io.FilenameFilter;
import java.sql.*;
import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;

/**
 * FlatFileNameDirectory provides an implementation of the
 * {@link FarragoMedNameDirectory} interface.
 *
 * @author Sunny Choi
 * @version $Id$
 */
class FlatFileNameDirectory extends MedAbstractNameDirectory
{

    //~ Instance fields -------------------------------------------------------

    final FlatFileDataServer server;
    String scope;


    //~ Constructors ----------------------------------------------------------

    FlatFileNameDirectory(FlatFileDataServer server, String scope)
    {
        this.server = server;
        this.scope = scope;
    }


    //~ Methods ---------------------------------------------------------------

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

        return server.newColumnSet(
            localName,
            server.getProperties(),
            typeFactory,
            null,
            Collections.EMPTY_MAP);
    }

    // implement FarragoMedNameDirectory
    public FarragoMedNameDirectory lookupSubdirectory(String foreignName)
        throws SQLException
    {
        if (scope.equals(FarragoMedMetadataQuery.OTN_SCHEMA)) {
            FlatFileParams.SchemaType schemaType =
                FlatFileParams.getSchemaType(foreignName, false);
            if (schemaType != null) {
                return new FlatFileNameDirectory(server,
                    FarragoMedMetadataQuery.OTN_TABLE);
            } else {
                return null;
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
        if (scope.equals(FarragoMedMetadataQuery.OTN_SCHEMA)) {
            boolean wantSchemas = query.getResultObjectTypes().contains(
                FarragoMedMetadataQuery.OTN_SCHEMA);
            if (wantSchemas) {
                sink.writeObjectDescriptor(
                    FlatFileParams.SchemaType.QUERY.getSchemaName(),
                    FarragoMedMetadataQuery.OTN_SCHEMA,
                    null,
                    Collections.EMPTY_MAP);
            }
        } else {
            boolean wantTables = query.getResultObjectTypes().contains(
                FarragoMedMetadataQuery.OTN_TABLE);
            if (wantTables) {
                if (!queryTables(query, sink)) {
                    return false;
                }
            }
            boolean wantColumns = query.getResultObjectTypes().contains(
                FarragoMedMetadataQuery.OTN_COLUMN);
            if (wantColumns) {
                if (!queryColumns(query, sink)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean queryTables(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        File dir = new File(server.params.getDirectory());
        String[] files = dir.list(new FlatFileFilter());

        for (int i=0; i<files.length; i++) {
            String tableName = files[i].substring(0, files[i].indexOf("."));
            sink.writeObjectDescriptor(
                tableName,
                FarragoMedMetadataQuery.OTN_TABLE,
                null,
                Collections.EMPTY_MAP);
        }
        return true;
    }

    private boolean queryColumns(
        FarragoMedMetadataQuery query,
        FarragoMedMetadataSink sink)
        throws SQLException
    {
        File dir = new File(server.params.getDirectory());
        String[] fileNames = dir.list(new FlatFileFilter());
        synchronized(FlatFileBCPFile.class) {
            for (int i=0; i<fileNames.length; i++) {
                String tableName =
                    fileNames[i].substring(0, fileNames[i].indexOf("."));
                String bcpFileName = server.params.getDirectory() +
                    tableName + server.params.getControlFileExtenstion();
                File testFile = new File(bcpFileName);
                FlatFileBCPFile bcpFile = new FlatFileBCPFile(bcpFileName,
                    sink.getTypeFactory());
                if (!testFile.exists()) {
                    String[] localName = {
                        server.getProperties().getProperty("NAME"),
                        FlatFileParams.SchemaType.QUERY.getSchemaName(),
                        tableName};
                    server.sampleAndCreateBcp(localName, bcpFile);
                }
            }
            String bcpExt = server.params.getControlFileExtenstion();
            String[] files = dir.list(new BCPFileFilter());

            for (int i=0; i<files.length; i++) {
                String tableName = files[i].
                    substring(0, files[i].indexOf("."));
                String bcpFilePath =
                    server.params.getDirectory() + tableName + bcpExt;
                FlatFileBCPFile bcpFile =
                    new FlatFileBCPFile(bcpFilePath, sink.getTypeFactory());
                if (bcpFile.parse()) {
                    RelDataType[] types = bcpFile.types;
                    String[] colNames = bcpFile.colNames;
                    int ordinal = 0;
                    for (int j=0; j<types.length; j++) {
                        RelDataType typeWithNull =
                            sink.getTypeFactory().createTypeWithNullability(
                                types[j], true);
                        sink.writeColumnDescriptor(
                            tableName,
                            colNames[j],
                            ordinal,
                            typeWithNull,
                            null,
                            null,
                            Collections.EMPTY_MAP);
                        ordinal++;
                    }
                }
            }
        }
        return true;
    }

    class FlatFileFilter implements FilenameFilter
    {
        public boolean accept(File dir, String name) {
            String fileExt = server.params.getFileExtenstion();
            return (name.endsWith(fileExt));
        }
    }

    class BCPFileFilter implements FilenameFilter
    {
        public boolean accept(File dir, String name) {
            String bcpExt = server.params.getControlFileExtenstion();
            return (name.endsWith(bcpExt));
        }
    }
}

// End FlatFileNameDirectory.java
