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
package net.sf.farrago.namespace.mock;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * MedMockDataServer provides a mock implementation of the {@link
 * FarragoMedDataServer} interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MedMockDataServer extends MedAbstractDataServer
{
    //~ Static fields/initializers --------------------------------------------

    public static final String PROP_ROW_COUNT = "ROW_COUNT";
    public static final String PROP_EXECUTOR_IMPL = "EXECUTOR_IMPL";
    public static final String PROPVAL_JAVA = "JAVA";
    public static final String PROPVAL_FENNEL = "FENNEL";

    //~ Constructors ----------------------------------------------------------

    MedMockDataServer(
        String serverMofId,
        Properties props)
    {
        super(serverMofId, props);
    }

    //~ Methods ---------------------------------------------------------------

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
        RelDataType rowType,
        Map columnPropMap)
        throws SQLException
    {
        assert (rowType.getFieldList().size() == 1);
        RelDataType type = rowType.getFields()[0].getType();
        assert (!type.isNullable());
        assert (typeFactory.getClassForPrimitive(type) != null);
        long nRows = getLongProperty(tableProps, PROP_ROW_COUNT, 10);
        String executorImpl =
            tableProps.getProperty(PROP_EXECUTOR_IMPL, PROPVAL_JAVA);
        assert (executorImpl.equals(PROPVAL_JAVA)
            || executorImpl.equals(PROPVAL_FENNEL));
        return new MedMockColumnSet(localName, rowType, nRows, executorImpl);
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
}


// End MedMockDataServer.java
