/*
// $Id$
// Firewater is a scaleout column store DBMS.
// Copyright (C) 2009-2009 John V. Sichi
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
package net.sf.firewater;

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.impl.*;
import net.sf.farrago.namespace.jdbc.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fwm.*;
import net.sf.farrago.fwm.distributed.*;

import org.apache.commons.dbcp.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.fun.*;

/**
 * FirewaterColumnSet implements {@link FarragoMedColumnSet} for Firewater
 * tables.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterColumnSet extends MedJdbcColumnSet
{
    private final FirewaterPartitioning partitioning;
    public final String dm_key;

    public FirewaterColumnSet(
        MedJdbcNameDirectory directory,
        String [] foreignName,
        String [] localName,
        SqlSelect select,
        SqlDialect dialect,
        RelDataType rowType,
        FirewaterPartitioning partitioning,
        String dm_key)
    {
        super(
            directory, foreignName, localName, select, dialect,
            rowType, rowType, rowType);

        this.partitioning = partitioning;
        this.dm_key = dm_key;
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        FarragoRepos repos = getPreparingStmt().getRepos();
        FirewaterPartitioning partitioning =
            FirewaterDataServer.getPartitioning(
                repos,
                (FemLocalTable) getCwmColumnSet());

        if (!partitioning.equals(FirewaterPartitioning.HASH)) {
            // REVIEW jvs 20-Mar-2010:  This is a workaround
            // which preloads the data servers; if we don't do
            // that now, we run into assertions later.  There
            // must be a better way.
            Collection<FemDataServer> servers =
                repos.allOfType(FemDataServer.class);
            boolean distributed = false;
            for (FemDataServer server : servers) {
                String wrapperName = server.getWrapper().getName();
                if (!wrapperName.startsWith("SYS_FIREWATER")) {
                    continue;
                }
                if (wrapperName.contains("DISTRIBUTED")) {
                    continue;
                }
                if (!wrapperName.equals("SYS_FIREWATER_EMBEDDED_WRAPPER")) {
                    distributed = true;
                }
                String catalogName =
                    FirewaterDdlHandler.getCatalogNameForServer(server);
                createRelForSpecificNode(
                    cluster,
                    connection,
                    server,
                    catalogName);
            }
            // TODO jvs 6-May-2010: need to get firewater_replica schema
            // created automatically on all storage nodes
            if (distributed) {
                String [] replicaName = new String[3];
                replicaName[0] = "FIREWATER_REPLICA";
                replicaName[1] = getForeignName()[1];
                replicaName[2] = getForeignName()[2];
                return generateForeignSql(
                    cluster,
                    connection,
                    replicaName,
                    getDirectory().getServer());
            } else {
                return new FirewaterReplicatedTableRel(
                    cluster, this, connection);
            }
        }

        // TODO jvs 20-Mar-2010:  Defer this, and return
        // a new FirewaterPartitionedTableRel instead.  This will
        // allow us to efficiently join a partitioned table with
        // another partitioned table in the case where the
        // partitioning key is the same.
        Collection c = FirewaterSessionFactory.getFwmPackage(
            getPreparingStmt().getRepos()).
            getDistributed().getFwmPartition().refAllOfClass();
        int nPartitions = c.size();
        RelNode [] inputs = new RelNode[nPartitions];
        int i = 0;
        for (Object o : c) {
            // TODO jvs 17-May-2009:  remote URL; also need
            // to make partition order deterministic
            FwmPartition partition = (FwmPartition) o;
            FemDataServer node =
                FirewaterDdlHandler.getNodeForPartition(partition);
            RelNode rel = createRelForSpecificNode(
                cluster, connection, node, partition.getName());
            inputs[i] = rel;
            ++i;
        }
        return new UnionRel(cluster, inputs, true);
    }

    RelNode createRelForSpecificNode(
        RelOptCluster cluster,
        RelOptConnection connection,
        FemDataServer node,
        String catalogName)
    {
        // REVIEW jvs 19-May-2009: see comments in
        // FarragoPreparingStmt.loadDataServerFromCache; and maybe we should be
        // making that public and calling it here?
        FarragoMedDataServer dataServer =
            getPreparingStmt().getStmtValidator().getDataWrapperCache().
            loadServerFromCatalog(node);
        String [] actualName = new String[3];
        actualName[0] = catalogName;
        actualName[1] = getForeignName()[1];
        actualName[2] = getForeignName()[2];
        RelNode rel = null;
        try {
            if (node.getWrapper().getName().equals(
                    "SYS_FIREWATER_EMBEDDED_WRAPPER"))
            {
                rel = optimizeLoopbackLink(
                    cluster, connection, actualName);
            }
        } catch (SQLException ex) {
            // fall through to generateForeignSql below
        }
        if (rel == null) {
            rel = generateForeignSql(
                cluster, connection, actualName, dataServer);
        }
        return rel;
    }
    
    private RelNode generateForeignSql(
        RelOptCluster cluster,
        RelOptConnection connection,
        String [] actualName,
        FarragoMedDataServer dataServer)
    {
        SqlSelect select =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                new SqlNodeList(
                    Collections.singletonList(
                        new SqlIdentifier("*", SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                new SqlIdentifier(actualName, SqlParserPos.ZERO),
                null,
                null,
                null,
                null,
                null,
                SqlParserPos.ZERO);
        assert (dataServer instanceof MedJdbcDataServer)
            : dataServer.getClass().getName();
        MedJdbcDataServer jdbcDataServer = (MedJdbcDataServer) dataServer;
        MedJdbcNameDirectory nameDirectory =
            new MedJdbcNameDirectory(jdbcDataServer);
        MedJdbcColumnSet columnSet =
            new MedJdbcColumnSet(
                nameDirectory,
                actualName,
                getLocalName(),
                select,
                getDialect(),
                getRowType(),
                getRowType(),
                getRowType());
        RelNode rel =
            new MedJdbcQueryRel(
                jdbcDataServer,
                columnSet,
                cluster,
                getRowType(),
                connection,
                getDialect(),
                select);
        return rel;
    }
}

// End FirewaterColumnSet.java
