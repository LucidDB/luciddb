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

package net.sf.farrago.namespace.ftrs;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;

import net.sf.saffron.core.*;
import net.sf.saffron.util.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.convert.*;

import java.sql.*;
import java.util.*;

/**
 * FtrsDataServer implements the {@link FarragoMedDataServer} interface
 * for FTRS data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsDataServer extends MedAbstractDataServer
{
    FtrsDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId,props);
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        SaffronType rowType,
        Map columnPropMap)
        throws SQLException
    {
        return new FtrsTable(localName,rowType,tableProps,columnPropMap);
    }

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param) throws SQLException
    {
        return null;
    }

    // implement FarragoMedDataServer
    public void registerRules(SaffronPlanner planner)
    {
        super.registerRules(planner);
        planner.addRule(new FtrsTableProjectionRule());
        planner.addRule(new FtrsTableModificationRule());
        planner.addRule(new FtrsScanToSearchRule());
        planner.addRule(new FtrsIndexJoinRule());
        planner.addRule(new FtrsRemoveRedundantSortRule());
    }
}

// End FtrsDataServer.java
