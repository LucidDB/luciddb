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

package net.sf.farrago.namespace.impl;

import net.sf.farrago.namespace.*;

import net.sf.saffron.core.*;

import java.util.*;
import java.sql.*;

/**
 * MedAbstractDataServer is an abstract base class for
 * implementations of the {@link FarragoMedDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractDataServer
    extends MedAbstractBase
    implements FarragoMedDataServer
{
    private String serverMofId;
    
    private Properties props;

    protected MedAbstractDataServer(
        String serverMofId,
        Properties props)
    {
        this.serverMofId = serverMofId;
        this.props = props;
    }

    /**
     * @return the MofId of the catalog definition for this server
     */
    public String getServerMofId()
    {
        return serverMofId;
    }

    /**
     * @return the options specified by CREATE SERVER
     */
    public Properties getProperties()
    {
        return props;
    }
    
    // implement FarragoMedDataServer
    public void registerRules(SaffronPlanner planner)
    {
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
    }
}

// End MedAbstractDataServer.java
