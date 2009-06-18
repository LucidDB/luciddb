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
    private static List<String> schemaSql = new ArrayList<String>();

    private static List<String> tableSql = new ArrayList<String>();

    private static List<String> indexSql = new ArrayList<String>();

    private static List<String> labelSql = new ArrayList<String>();

    private static List<Pair<String,String>> catalogSql
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
        executeRemoteSql(repos, indexSql, eot);
        executeRemoteSql(repos, tableSql, eot);
        executeRemoteSql(repos, schemaSql, eot);
        executeRemoteSql(repos, labelSql, eot);
        executeCatalogSql(repos, catalogSql, eot);
    }

    private static void executeRemoteSql(
        FarragoRepos repos,
        List<String> sqlList,
        FarragoSessionTxnEnd eot)
    {
        // TODO jvs 17-May-2009:  repos txn for partition iteration
        if (eot == FarragoSessionTxnEnd.COMMIT) {
            for (String sql : sqlList) {
                for (FwmPartition partition
                         : repos.allOfClass(FwmPartition.class))
                {
                    executeRemoteSql(
                        repos,
                        sql,
                        partition,
                        getNodeForPartition(partition));
                }
            }
        }
        sqlList.clear();
    }

    private static void executeCatalogSql(
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
        FwmPartition partition,
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
            if (partition != null) {
                // REVIEW jvs 17-May-2009:  naming scheme for catalogs
                // which represent partitions
                String catalogName = partition.getName();
                stmt.execute("set catalog '\"" + catalogName + "\"'");
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
        boolean execLocal,
        List<String> sqlList)
    {
        StringBuffer sb = new StringBuffer();
        for (String frag : ddlStmt.getStatementList()) {
            sb.append(frag);
        }
        String sql = sb.toString();
        assert(!sql.equals(""));
        if (execLocal) {
            sqlList.add(sql);
        }
        setLastSql(sql);
    }

    private void setLastSql(String sql)
    {
        // TODO jvs 18-May-2009:  come up with something better
        System.setProperty("firewater.test.lastSql", sql);
    }

    private void distributeCreation(
        CwmModelElement element,
        boolean execLocal,
        List<String> sqlList)
    {
        DdlGenerator ddlGen = new FarragoDdlGenerator(
            repos.getModelView());
        ddlGen.setSchemaQualified(true);
        GeneratedDdlStmt stmt = new GeneratedDdlStmt(false);
        ddlGen.generateCreate(element, stmt);
        distributeStmt(stmt, execLocal, sqlList);
    }

    private void distributeDrop(
        CwmModelElement element,
        boolean execLocal,
        List<String> sqlList)
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
        distributeStmt(stmt, execLocal, sqlList);
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
        catalogSql.add(
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
        catalogSql.add(
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
        distributeCreation(schema, true, schemaSql);
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLocalSchema schema)
    {
        distributeDrop(schema, true, schemaSql);
    }

    // implement FarragoSessionDdlHandler
    public void executeCreation(FemLocalTable table)
    {
        distributeCreation(table, true, tableSql);
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLocalTable table)
    {
        distributeDrop(table, true, tableSql);
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
        distributeCreation(index, true, indexSql);
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLocalIndex index)
    {
        relationalHandler.executeDrop(index);

        if (isSystemIndex(index)) {
            return;
        }
        distributeDrop(index, true, indexSql);
    }

    // implement FarragoSessionDdlHandler
    public void executeCreation(FemLabel label)
    {
        distributeCreation(label, false, labelSql);
    }

    // implement FarragoSessionDdlHandler
    public void executeDrop(FemLabel label)
    {
        // TODO jvs 28-May-2009:  only once per server, not
        // once per partition!
        distributeDrop(label, false, labelSql);
    }
}

// End FirewaterDdlHandler.java
