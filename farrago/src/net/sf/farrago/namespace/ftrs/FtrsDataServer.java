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

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
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
 * FtrsDataServer implements the {@link FarragoMedDataServer} interface
 * for FTRS data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsDataServer extends MedAbstractLocalDataServer
{
    //~ Instance fields -------------------------------------------------------

    private FarragoRepos repos;
    private FarragoTypeFactory indexTypeFactory;

    //~ Constructors ----------------------------------------------------------

    FtrsDataServer(
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
        return new FtrsTable(localName, rowType, tableProps, columnPropMap);
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
        planner.addRule(new FtrsTableProjectionRule());
        planner.addRule(new FtrsTableModificationRule());
        planner.addRule(new FtrsScanToSearchRule());
        planner.addRule(new FtrsIndexJoinRule());
        planner.addRule(new FtrsRemoveRedundantSortRule());
    }

    // implement FarragoMedLocalDataServer
    public long createIndex(CwmSqlindex index)
    {
        repos.beginTransientTxn();
        try {
            FemCmdCreateIndex cmd = repos.newFemCmdCreateIndex();
            if (!repos.isFennelEnabled()) {
                return 0;
            }

            initIndexCmd(cmd, index);
            return getFennelDbHandle().executeCmd(cmd);
        } finally {
            repos.endTransientTxn();
        }
    }

    // implement FarragoMedLocalDataServer
    public void dropIndex(
        CwmSqlindex index,
        long rootPageId,
        boolean truncate)
    {
        repos.beginTransientTxn();
        try {
            FemCmdDropIndex cmd;
            if (truncate) {
                cmd = repos.newFemCmdTruncateIndex();
            } else {
                cmd = repos.newFemCmdDropIndex();
            }
            if (!repos.isFennelEnabled()) {
                return;
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
        CwmSqlindex index)
    {
        cmd.setDbHandle(getFennelDbHandle().getFemDbHandle(repos));
        cmd.setTupleDesc(
            FtrsUtil.getCoverageTupleDescriptor(indexTypeFactory, index));
        cmd.setKeyProj(FtrsUtil.getDistinctKeyProjection(repos, index));
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
    static long getIndexSegmentId(CwmSqlindex index)
    {
        // TODO:  share symbolic enum with Fennel rather than hard-coding
        // values here
        if (FarragoRepos.isTemporary(index)) {
            return 2;
        } else {
            return 1;
        }
    }
}


// End FtrsDataServer.java
