/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2004-2004 John V. Sichi.
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

import java.sql.*;
import java.util.*;

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.type.*;

import org.eigenbase.reltype.*;
import org.eigenbase.relopt.*;

/**
 * MedMockLocalDataServer provides a mock implementation of the
 * {@link FarragoMedLocalDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockLocalDataServer
    extends MedMockDataServer
    implements FarragoMedLocalDataServer
{
    MedMockLocalDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
    }
    
    // implement FarragoMedLocalDataServer
    public void setFennelDbHandle(FennelDbHandle fennelDbHandle)
    {
        // ignore
    }

    // implement FarragoMedLocalDataServer
    public long createIndex(CwmSqlindex index)
        throws SQLException
    {
        // mock roots are meaningless
        return 0;
    }

    // implement FarragoMedLocalDataServer
    public void dropIndex(
        CwmSqlindex index,
        long rootPageId,
        boolean truncate)
        throws SQLException
    {
        // ignore
    }
    
    // implement FarragoMedDataServer
    public void registerRules(RelOptPlanner planner)
    {
        super.registerRules(planner);
        planner.addRule(new MedMockTableModificationRule());
    }

    // override MedMockDataServer
    public FarragoMedColumnSet newColumnSet(
        String [] localName,
        Properties tableProps,
        FarragoTypeFactory typeFactory,
        RelDataType rowType,
        Map columnPropMap)
        throws SQLException
    {
        long nRows = getLongProperty(tableProps, PROP_ROW_COUNT, 0);
        String executorImpl =
            tableProps.getProperty(PROP_EXECUTOR_IMPL, PROPVAL_JAVA);
        assert (executorImpl.equals(PROPVAL_JAVA)
            || executorImpl.equals(PROPVAL_FENNEL));
        return new MedMockColumnSet(localName, rowType, nRows, executorImpl);
    }
}

// End MedMockLocalDataServer.java
