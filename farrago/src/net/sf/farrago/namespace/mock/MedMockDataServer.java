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

package net.sf.farrago.namespace.mock;

import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.util.*;
import net.sf.farrago.type.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;

import net.sf.saffron.sql.*;
import net.sf.saffron.core.*;
import net.sf.saffron.util.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.rel.jdbc.*;

import java.sql.*;
import java.util.*;
import java.lang.reflect.*;

/**
 * MedMockDataServer provides a mock implementation of the {@link
 * FarragoMedDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockDataServer extends MedAbstractDataServer
{
    public static final String PROP_ROW_COUNT = "ROW_COUNT";
    
    public static final String PROP_EXECUTOR_IMPL = "EXECUTOR_IMPL";
    
    public static final String PROPVAL_JAVA = "JAVA";
    
    public static final String PROPVAL_FENNEL = "FENNEL";
    
    MedMockDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId,props);
    }

    void initialize()
        throws SQLException
    {
    }

    // implement FarragoMedDataServer
    public FarragoMedNameDirectory getNameDirectory()
        throws SQLException
    {
        return new MedMockNameDirectory(this);
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
        assert(rowType.getFieldCount() == 1);
        FarragoAtomicType type = (FarragoAtomicType)
            rowType.getFields()[0].getType();
        assert(!type.isNullable());
        assert(type.hasClassForPrimitive());
        long nRows = getLongProperty(tableProps,PROP_ROW_COUNT,10);
        String executorImpl = tableProps.getProperty(
            PROP_EXECUTOR_IMPL,PROPVAL_JAVA);
        assert(executorImpl.equals(PROPVAL_JAVA)
               || executorImpl.equals(PROPVAL_FENNEL));
        return new MedMockColumnSet(
            localName,
            rowType,
            nRows,
            executorImpl);
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
    }
    
    // implement FarragoAllocation
    public void closeAllocation()
    {
        super.closeAllocation();
    }
}

// End MedMockDataServer.java
