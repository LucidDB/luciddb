/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.namespace.impl;

import java.sql.*;

import java.util.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;


/**
 * MedAbstractLocalDataServer is an abstract base class for implementations of
 * the {@link FarragoMedLocalDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractLocalDataServer
    extends MedAbstractDataServer
    implements FarragoMedLocalDataServer
{
    //~ Instance fields --------------------------------------------------------

    private FennelDbHandle fennelDbHandle;

    //~ Constructors -----------------------------------------------------------

    protected MedAbstractLocalDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the Fennel database handle to use for accessing local storage
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedLocalDataServer
    public void setFennelDbHandle(FennelDbHandle fennelDbHandle)
    {
        this.fennelDbHandle = fennelDbHandle;
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex)
        throws SQLException
    {
        // by default, no special validation rules
    }

    // implement FarragoMedLocalDataServer
    public void validateTableDefinition(
        FemLocalTable table,
        FemLocalIndex generatedPrimaryKeyIndex,
        boolean creation)
        throws SQLException
    {
        validateTableDefinition(table, generatedPrimaryKeyIndex);
    }

    // implement FarragoMedLocalDataServer
    public boolean supportsAlterTableAddColumn()
    {
        // Assume not; subclasses have to override this
        // to enable support.
        return false;
    }
}

// End MedAbstractLocalDataServer.java
