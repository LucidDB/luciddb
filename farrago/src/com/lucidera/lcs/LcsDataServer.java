/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.lucidera.lcs;

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * LcsDataServer implements the {@link FarragoMedDataServer} interface
 * for LucidDB column-store data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class LcsDataServer extends MedAbstractLocalDataServer
{
    //~ Instance fields -------------------------------------------------------

    private FarragoRepos repos;
    private FarragoTypeFactory indexTypeFactory;

    //~ Constructors ----------------------------------------------------------

    LcsDataServer(
        String serverMofId,
        Properties props,
        FarragoRepos repos)
    {
        super(serverMofId, props);
        this.repos = repos;
        indexTypeFactory = new FarragoTypeFactoryImpl(repos);
    }

    //~ Methods ---------------------------------------------------------------

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
        RelDataType rowType,
        Map columnPropMap)
        throws SQLException
    {
        return new LcsTable(localName, rowType, tableProps, columnPropMap);
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

        // TODO jvs 26-Oct-2005:  planner rules specific to
        // column-store go here, e.g.
        // planner.addRule(new LcsTableProjectionRule());
    }

    // implement FarragoMedLocalDataServer
    public long createIndex(FemLocalIndex index)
    {
        // REVIEW jvs 26-Oct-2005:  I copied this over from FTRS;
        // can it be shared?
        repos.beginTransientTxn();
        try {
            FemCmdCreateIndex cmd = repos.newFemCmdCreateIndex();
            initIndexCmd(cmd, index);
            return getFennelDbHandle().executeCmd(cmd);
        } finally {
            repos.endTransientTxn();
        }
    }

    // implement FarragoMedLocalDataServer
    public void dropIndex(
        FemLocalIndex index,
        long rootPageId,
        boolean truncate)
    {
        // REVIEW jvs 26-Oct-2005:  I copied this over from FTRS;
        // can it be shared?
        repos.beginTransientTxn();
        try {
            FemCmdDropIndex cmd;
            if (truncate) {
                cmd = repos.newFemCmdTruncateIndex();
            } else {
                cmd = repos.newFemCmdDropIndex();
            }
            initIndexCmd(cmd, index);
            cmd.setRootPageId(rootPageId);
            getFennelDbHandle().executeCmd(cmd);
        } finally {
            repos.endTransientTxn();
        }
    }

    private void initIndexCmd(
        FemIndexCmd cmd,
        FemLocalIndex index)
    {
        // TODO jvs 26-Oct-2005:  rework this example copied from FTRS
        cmd.setDbHandle(getFennelDbHandle().getFemDbHandle(repos));
        /*
        FtrsIndexGuide indexGuide = new LcsIndexGuide(
            indexTypeFactory,
            FarragoCatalogUtil.getIndexTable(index));
        cmd.setTupleDesc(
            indexGuide.getCoverageTupleDescriptor(index));
        cmd.setKeyProj(indexGuide.getDistinctKeyProjection(index));
        cmd.setSegmentId(getIndexSegmentId(index));
        cmd.setIndexId(JmiUtil.getObjectId(index));
        */
    }

    /**
     * Gets the SegmentId of the segment storing an index.
     *
     * @param index the index of interest
     *
     * @return containing SegmentId
     */
    static long getIndexSegmentId(FemLocalIndex index)
    {
        // REVIEW jvs 26-Oct-2005:  I copied this over from FTRS;
        // can it be shared?
        if (FarragoCatalogUtil.isIndexTemporary(index)) {
            return 2;
        } else {
            return 1;
        }
    }
}


// End LcsDataServer.java
