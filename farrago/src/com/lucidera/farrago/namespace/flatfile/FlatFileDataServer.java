/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.farrago.namespace.flatfile;

import java.sql.*;
import java.util.*;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.type.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;

/**
 * FlatFileDataServer provides an implementation of the {@link
 * FarragoMedDataServer} interface.
 *
 * @author John V. Pham
 * @version $Id$
 */
class FlatFileDataServer extends MedAbstractDataServer
{
    //~ Static fields/initializers --------------------------------------------

    private MedAbstractDataWrapper wrapper;
    FlatFileParams params;

    //~ Constructors ----------------------------------------------------------

    FlatFileDataServer(
        MedAbstractDataWrapper wrapper,
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
        this.wrapper = wrapper;
    }

    //~ Methods ---------------------------------------------------------------

    void initialize()
        throws SQLException
    {
        params = new FlatFileParams(getProperties());
        params.decode();

        // TODO: validate, e.g. throw an error if directory doesn't exist
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
        Map columnPropMap)
        throws SQLException
    {
        if (rowType == null) {
            // scan control file/data file for metadata (Phase II)
            String name = tableProps.getProperty(
                FlatFileColumnSet.PROP_FILENAME);
            if (name == null) {
                return null;
            }
            String extension = params.getControlFileExtenstion();
            String ctrlFilePath = params.getDirectory() + name + extension;
            FlatFileBCPFile bcpFile =
                new FlatFileBCPFile(ctrlFilePath, typeFactory);
            if (bcpFile.parse()) {
                rowType = createRowType(
                    typeFactory, bcpFile.types, bcpFile.colNames);
            }
        }
        return new FlatFileColumnSet(localName, rowType, params, tableProps);
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
        RelDataType[] types,
        String[] names)
    {
        return typeFactory.createStructType(types, names);
    }
}


// End FlatFileDataServer.java

