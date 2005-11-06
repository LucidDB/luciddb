/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.util.*;

import java.util.*;
import java.sql.*;

/**
 * MedAbstractFennelDataServer refines {@link MedAbstractLocalDataServer}
 * with abstract support for using Fennel's btree indexing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractFennelDataServer
    extends MedAbstractLocalDataServer
{
    protected FarragoRepos repos;
    
    protected MedAbstractFennelDataServer(
        String serverMofId,
        Properties props,
        FarragoRepos repos)
    {
        super(serverMofId, props);
        this.repos = repos;
    }

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param)
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedLocalDataServer
    public long createIndex(FemLocalIndex index)
    {
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
        cmd.setDbHandle(getFennelDbHandle().getFemDbHandle(repos));
        cmd.setSegmentId(getIndexSegmentId(index));
        cmd.setIndexId(JmiUtil.getObjectId(index));
        prepareIndexCmd(cmd, index);
    }
    
    /**
     * Prepares an index command based on the catalog definition
     * of the index.  The parameterization details of command
     * preparation are subclass-dependent.
     *
     * @param cmd command to be initialized
     *
     * @param index catalog definition of index
     */
    protected abstract void prepareIndexCmd(
        FemIndexCmd cmd,
        FemLocalIndex index);

    /**
     * Gets the SegmentId of the segment storing an index.
     *
     * @param index the index of interest
     *
     * @return containing SegmentId
     */
    public static long getIndexSegmentId(FemLocalIndex index)
    {
        // TODO:  share symbolic enum with Fennel rather than hard-coding
        // values here
        if (FarragoCatalogUtil.isIndexTemporary(index)) {
            return 2;
        } else {
            return 1;
        }
    }
}

// End MedAbstractFennelDataServer.java
