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
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;

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
class FtrsDataServer extends MedAbstractLocalDataServer
{
    private FarragoCatalog catalog;
    
    private FarragoTypeFactory indexTypeFactory;
    
    FtrsDataServer(
        String serverMofId,
        Properties props,
        FarragoCatalog catalog)
    {
        super(serverMofId,props);
        this.catalog = catalog;
        indexTypeFactory = new FarragoTypeFactoryImpl(catalog);
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

    // implement FarragoMedLocalDataServer
    public long createIndex(
        CwmSqlindex index)
    {
        FemCmdCreateIndex cmd = catalog.newFemCmdCreateIndex();
        if (!catalog.isFennelEnabled()) {
            return 0;
        }
        
        initIndexCmd(cmd,index);
        return getFennelDbHandle().executeCmd(cmd);
    }

    // implement FarragoMedLocalDataServer
    public void dropIndex(
        CwmSqlindex index,
        long rootPageId,
        boolean truncate)
    {
        FemCmdDropIndex cmd;
        if (truncate) {
            cmd = catalog.newFemCmdTruncateIndex();
        } else {
            cmd = catalog.newFemCmdDropIndex();
        }
        if (!catalog.isFennelEnabled()) {
            return;
        }
        initIndexCmd(cmd,index);
        cmd.setRootPageId(rootPageId);
        getFennelDbHandle().executeCmd(cmd);
    }
    
    private void initIndexCmd(
        FemIndexCmd cmd,CwmSqlindex index)
    {
        cmd.setDbHandle(getFennelDbHandle().getFemDbHandle(catalog));
        cmd.setTupleDesc(
            FtrsUtil.getCoverageTupleDescriptor(
                indexTypeFactory,
                index));
        cmd.setKeyProj(
            FtrsUtil.getDistinctKeyProjection(catalog,index));
        cmd.setSegmentId(getIndexSegmentId(index));
        cmd.setIndexId(JmiUtil.getObjectId(index));
    }

    /**
     * Get the SegmentId of the segment storing an index.
     *
     * @param index the index of interest
     *
     * @return containing SegmentId
     */
    static long getIndexSegmentId(
        CwmSqlindex index)
    {
        // TODO:  share symbolic enum with Fennel rather than hard-coding
        // values here
        if (FarragoCatalog.isTemporary(index)) {
            return 2;
        } else {
            return 1;
        }
    }
}

// End FtrsDataServer.java
