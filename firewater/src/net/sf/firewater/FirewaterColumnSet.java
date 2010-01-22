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

    public FirewaterColumnSet(
        MedJdbcNameDirectory directory,
        String [] foreignName,
        String [] localName,
        SqlSelect select,
        SqlDialect dialect,
        RelDataType rowType,
        FirewaterPartitioning partitioning)
    {
        super(
            directory, foreignName, localName, select, dialect,
            rowType, rowType, rowType);

        this.partitioning = partitioning;
    }

    // implement RelOptTable
    public RelNode toRel(
        RelOptCluster cluster,
        RelOptConnection connection)
    {
        FirewaterPartitioning partitioning =
            FirewaterDataServer.getPartitioning(
                getPreparingStmt().getRepos(),
                (FemLocalTable) getCwmColumnSet());

        if (!partitioning.equals(FirewaterPartitioning.HASH)) {
            return new FirewaterReplicatedTableRel(
                cluster, this, connection);
        }
        
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
            // REVIEW jvs 19-May-2009:  see comments in
            // FarragoPreparingStmt.loadDataServerFromCache; and maybe
            // we should be making that public and calling it here?
            FarragoMedDataServer dataServer =
                getPreparingStmt().getStmtValidator().getDataWrapperCache().
                loadServerFromCatalog(node);
            String [] partitionName = new String[3];
            partitionName[0] = partition.getName();
            partitionName[1] = getForeignName()[1];
            partitionName[2] = getForeignName()[2];
            RelNode rel = null;
            try {
                if (node.getWrapper().getName().equals(
                        "SYS_FIREWATER_EMBEDDED_WRAPPER"))
                {
                    rel = optimizeLoopbackLink(
                        cluster, connection, partitionName);
                }
            } catch (SQLException ex) {
                // fall through to generateForeignSql below
            }
            if (rel == null) {
                rel = generateForeignSql(
                    cluster, connection, partitionName, dataServer);
            }
            inputs[i] = rel;
            ++i;
        }
        return new UnionRel(cluster, inputs, true);
    }

    private RelNode generateForeignSql(
        RelOptCluster cluster,
        RelOptConnection connection,
        String [] partitionName,
        FarragoMedDataServer dataServer)
    {
        SqlSelect select =
            SqlStdOperatorTable.selectOperator.createCall(
                null,
                new SqlNodeList(
                    Collections.singletonList(
                        new SqlIdentifier("*", SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
                new SqlIdentifier(partitionName, SqlParserPos.ZERO),
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
                partitionName,
                getLocalName(),
                select,
                getDialect(),
                getRowType(),
                getRowType(),
                getRowType());
        RelNode rel =
            new MedJdbcQueryRel(
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
