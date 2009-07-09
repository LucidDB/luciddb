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

import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.util.*;
import org.eigenbase.sql.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.session.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.ddl.gen.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fwm.distributed.*;

import net.sf.firewater.jdbc.*;

/**
 * FirewaterDdlHandler implements the {@Link FarragoSessionDdlHandler} pattern
 * by plugging in Firewater behavior: distributing the DDL across storage
 * nodes.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FirewaterDdlHandler extends DdlHandler
{
    private static List<Pair<Boolean,String>> schemaSql
        = new ArrayList<Pair<Boolean,String>>();

    private static List<Pair<Boolean,String>> tableSql =
        new ArrayList<Pair<Boolean,String>>();

    private static List<Pair<Boolean,String>> indexSql =
        new ArrayList<Pair<Boolean,String>>();

    private static List<Pair<Boolean,String>> labelSql =
        new ArrayList<Pair<Boolean,String>>();

    private static List<Pair<String,String>> serverSpecificSql
        = new ArrayList<Pair<String,String>>();

    private DdlRelationalHandler relationalHandler;

    public FirewaterDdlHandler(FarragoSessionDdlValidator validator)
    {
        super(validator);
        relationalHandler = new DdlRelationalHandler(
            new DdlMedHandler(validator));
    }

    public static void onEndOfTransaction(
        FarragoRepos repos,
        FarragoSessionTxnEnd eot)
    {
        // This is a crude way of dealing with cascades: go in a specific
        // object order which guarantees containees get dropped before
        // containers.  A real topological sort will be necessary for arbitrary
        // dependencies such as for UDR's.  Also, need the reverse order for
        // compound CREATE (e.g. schema with objects, or table with indexes).
        executeRemoteSql(repos, indexSql, eot, false, false);
        executeRemoteSql(repos, tableSql, eot, false, false);
        executeRemoteSql(repos, schemaSql, eot, true, false);
        executeRemoteSql(repos, labelSql, eot, true, true);
        executeServerSpecificSql(repos, serverSpecificSql, eot);
    }

    private static void executeRemoteSql(
        FarragoRepos repos,
        List<Pair<Boolean,String>> sqlList,
        FarragoSessionTxnEnd eot,
        boolean allServers,
        boolean skipLocal)
    {
        // TODO jvs 17-May-2009:  repos txn for partition iteration
        if (eot == FarragoSessionTxnEnd.COMMIT) {
            for (Pair<Boolean,String> pair : sqlList) {
                boolean isPartitioned = pair.left;
                String sql = pair.right;
                if (isPartitioned) {
                    Collection<FwmPartition> partitions =
                        repos.allOfClass(FwmPartition.class);
                    for (FwmPartition partition : partitions) {
                        executeRemoteSql(
                            repos,
                            sql,
                            partition.getName(),
                            getNodeForPartition(partition));
                    }
                }
                if (allServers || !isPartitioned) {
                    boolean executedLocal = skipLocal;
                    Collection<FemDataServer> servers =
                        repos.allOfType(FemDataServer.class);
                    for (FemDataServer server : servers) {
                        String wrapperName = server.getWrapper().getName();
                        if (wrapperName.equals(
                                "SYS_FIREWATER_REMOTE_WRAPPER"))
                        {
                            executeRemoteSql(
                                repos,
                                sql,
                                null,
                                server);
                        } else if (!executedLocal) {
                            if (wrapperName.equals(
                                    "SYS_FIREWATER_EMBEDDED_WRAPPER")
                                || wrapperName.equals(
                                    "SYS_FIREWATER_FAKEREMOTE_WRAPPER"))
                            {
                                executeRemoteSql(
                                    repos,
                                    sql,
                                    "LOCAL_REPLICAS",
                                    server);
                                executedLocal = true;
                            }
                        }
                    }
                }
            }
        }
        sqlList.clear();
    }

    private static void executeServerSpecificSql(
        FarragoRepos repos,
        List<Pair<String,String>> sqlList,
        FarragoSessionTxnEnd eot)
    {
        // TODO jvs 17-May-2009:  repos txn for partition iteration
        if (eot == FarragoSessionTxnEnd.COMMIT) {
            for (Pair<String,String> pair : sqlList) {
                String serverMofId = pair.left;
                String sql = pair.right;
                FemDataServer node = (FemDataServer)
                    repos.getMdrRepos().getByMofId(serverMofId);
                executeRemoteSql(
                    repos,
                    sql,
                    null,
                    node);
            }
        }
        sqlList.clear();
    }

    private static void executeRemoteSql(
        FarragoRepos repos,
        String sql,
        String partitionName,
        FemDataServer node)
    {
        Properties nodeProps =
            FarragoCatalogUtil.getStorageOptionsAsProperties(repos, node);
        String url = nodeProps.getProperty("URL");

        // TODO jvs 19-May-2009:  delegate this to wrapper
        Driver driver;
        if (url == null) {
            driver = new FirewaterEmbeddedStorageDriver();
            url = "jdbc:firewater_storage:embedded:";
        } else {
            driver = new FirewaterRemoteStorageDriver();
        }
        Properties props = new Properties();
        props.setProperty("user", "sa");
        props.setProperty("password", "");
        Connection conn = null;
        try {
            conn = driver.connect(url, props);
            Statement stmt = conn.createStatement();
            if (partitionName != null) {
                stmt.execute("set catalog '\"" + partitionName + "\"'");
            }
            stmt.execute(sql);
        } catch (SQLException ex) {
            // FIXME
            throw Util.newInternal(ex);
        } finally {
            Util.squelchConnection(conn);
        }
    }

    private void distributeStmt(
        GeneratedDdlStmt ddlStmt,
        List<Pair<Boolean,String>> sqlList,
        boolean isPartitioned)
    {
        StringBuffer sb = new StringBuffer();
        for (String frag : ddlStmt.getStatementList()) {
            sb.append(frag);
        }
        String sql = sb.toString();
        assert(!sql.equals(""));
        sqlList.add(new Pair<Boolean,String>(isPartitioned, sql));
        setLastSql(sql);
    }

    private void setLastSql(String sql)
    {
        // TODO jvs 18-May-2009:  come up with something better
        System.setProperty("firewater.test.lastSql", sql);
    }

    private void distributeCreation(
        CwmModelElement element,
        List<Pair<Boolean,String>> sqlList,
        boolean isPartitioned)
    {
        DdlGenerator ddlGen = new FarragoDdlGenerator(
            repos.getModelView());
        ddlGen.setSchemaQualified(true);
        GeneratedDdlStmt stmt = new GeneratedDdlStmt(false);
        ddlGen.generateCreate(element, stmt);
        distributeStmt(stmt, sqlList, isPartitioned);
    }

    private void distributeDrop(
        CwmModelElement element,
        List<Pair<Boolean,String>> sqlList,
        boolean isPartitioned)
    {
        DdlGenerator ddlGen = new FarragoDdlGenerator(
            repos.getModelView());
        ddlGen.setSchemaQualified(true);
        if (element instanceof FemLabel) {
            // TODO jvs 12-May-2009:  fix the DDL generator instead
            // to avoid the qualifier in this case
            ddlGen.setSchemaQualified(false);
        }
        ddlGen.setDropCascade(true);
        if (element instanceof FemLocalIndex) {
            // TODO jvs 12-May-2009:  fix the DDL generator instead
            // to avoid the CASCADE in this case
            ddlGen.setDropCascade(false);
        }
        GeneratedDdlStmt stmt = new GeneratedDdlStmt(false);
        ddlGen.generateDrop(element, stmt);
        distributeStmt(stmt, sqlList, isPartitioned);
    }

    // implement FarragoSessionDdlHandler
    public void validateDefinition(FwmPartition partition)
    {
        FemDataServer node = getNodeForPartition(partition);
        String wrapperName = node.getWrapper().getName();
        Properties nodeProps =
            FarragoCatalogUtil.getStorageOptionsAsProperties(repos, node);
        String url = nodeProps.getProperty("URL");
        boolean fail = false;
        if ((url != null) && !(url.startsWith("jdbc:firewater_storage:"))) {
            fail = true;
        }
        if (!wrapperName.equals("SYS_FIREWATER_EMBEDDED_WRAPPER")
            && !wrapperName.equals("SYS_FIREWATER_REMOTE_WRAPPER")
            && !wrapperName.equals("SYS_FIREWATER_FAKEREMOTE_WRAPPER"))
        {
            fail = true;
        }
        if (fail) {
            throw FirewaterSessionFactory.res.FirewaterUrlRequired.ex(
                repos.getLocalizedObjectName(node));
        }
    }

    // implement FarragoSessionDdlHandler
    public void validateModification(FwmPartition partition)
    {
        validateDefinition(partition);
    }

    // implement FarragoSessionDdlHandler
    public void validateDrop(FwmPartitionReplica replica)
    {
        if (!validator.isDeletedObject(replica.getPartition())) {
            throw FirewaterSessionFactory.res.CascadeToReplica.ex(
                repos.getLocalizedObjectName(replica.getPartition()));
        }
    }

    // implement FarragoSessionDdlHandler
    public void executeCreation(FwmPartition partition)
    {
        String sql = "create catalog "
            + SqlUtil.eigenbaseDialect.quoteIdentifier(
                partition.getName());
        setLastSql(sql);
        serverSpecificSql.add(
            new Pair<String,String>(
                getNodeForPartition(partition).refMofId(),
                sql));
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FwmPartition partition)
    {
        String sql =
            "drop catalog "
            + SqlUtil.eigenbaseDialect.quoteIdentifier(
                partition.getName())
            + " cascade";
        setLastSql(sql);
        serverSpecificSql.add(
            new Pair<String,String>(
                getNodeForPartition(partition).refMofId(),
                sql));
    }

    public static FemDataServer getNodeForPartition(FwmPartition partition)
    {
        assert(partition.getReplica().size() == 1);
        FwmPartitionReplica replica =
            partition.getReplica().iterator().next();
        return replica.getNode();
    }

    // implement FarragoSessionDdlHandler
    public void executeCreation(FemLocalSchema schema)
    {
        distributeCreation(schema, schemaSql, true);
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLocalSchema schema)
    {
        distributeDrop(schema, schemaSql, true);
    }

    private boolean decidePartitioning(FemLocalTable table)
    {
        FirewaterPartitioning partitioning =
            FirewaterDataServer.getPartitioning(repos, table);
        return partitioning.equals(FirewaterPartitioning.HASH);
    }

    // implement FarragoSessionDdlHandler
    public void executeCreation(FemLocalTable table)
    {
        distributeCreation(table, tableSql, decidePartitioning(table));
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLocalTable table)
    {
        distributeDrop(table, tableSql, decidePartitioning(table));
    }

    private boolean isSystemIndex(FemLocalIndex index)
    {
        return index.getName().startsWith("SYS$");
    }

    // implement FarragoSessionDdlHandler
    public void executeCreation(FemLocalIndex index)
    {
        relationalHandler.executeCreation(index);

        if (isSystemIndex(index)) {
            return;
        }
        distributeCreation(
            index,
            indexSql,
            decidePartitioning(
                (FemLocalTable) index.getSpannedClass()));
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLocalIndex index)
    {
        relationalHandler.executeDrop(index);

        if (isSystemIndex(index)) {
            return;
        }
        distributeDrop(
            index,
            indexSql,
            decidePartitioning(
                (FemLocalTable) index.getSpannedClass()));
    }

    // implement FarragoSessionDdlHandler
    public void executeCreation(FemLabel label)
    {
        distributeCreation(label, labelSql, false);
        relationalHandler.executeCreation(label);
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLabel label)
    {
        distributeDrop(label, labelSql, false);
    }
}

// End FirewaterDdlHandler.java
