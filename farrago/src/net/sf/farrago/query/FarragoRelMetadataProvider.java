/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.query;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.rel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.stat.*;


/**
 * FarragoRelMetadataProvider implements Farrago specific metadata. Initially,
 * it provides costing information available through statistics. Other potential
 * uses of this class are:
 *
 * <ol>
 * <li>Provide cost information for Farrago rels</li>
 * <li>Provides a uniform cost model for all rels of concern to Farrago</li>
 * </ol>
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoRelMetadataProvider
    extends ReflectiveRelMetadataProvider
{
    //~ Instance fields --------------------------------------------------------

    private FarragoRepos repos;

    private FarragoColumnMetadata columnMd;

    //~ Constructors -----------------------------------------------------------

    /**
     * Initializes a provider with access to the Farrago catalog. The provider
     * reads statistics stored in the catalog.
     *
     * @param repos the Farrago catalog
     */
    public FarragoRelMetadataProvider(FarragoRepos repos)
    {
        this.repos = repos;
        columnMd = new FarragoColumnMetadata();

        mapParameterTypes(
            "getPopulationSize",
            Collections.singletonList((Class) BitSet.class));

        List<Class> args = new ArrayList<Class>();
        args.add((Class) BitSet.class);
        args.add((Class) RexNode.class);
        mapParameterTypes("getDistinctRowCount", args);

        mapParameterTypes(
            "getUniqueKeys",
            Collections.singletonList((Class) Boolean.TYPE));

        args = new ArrayList<Class>();
        args.add((Class) BitSet.class);
        args.add((Class) Boolean.TYPE);
        mapParameterTypes("areColumnsUnique", args);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Retrieves statistics for a relational expression, if they exist in the
     * catalog.
     *
     * @param rel the relational expression
     *
     * @return the statistics object, or null
     */
    public RelStatSource getStatistics(RelNode rel)
    {
        return getStatistics(rel, repos);
    }

    private static RelStatSource getStatistics(RelNode rel, FarragoRepos repos)
    {
        RelOptTable table = rel.getTable();
        if (table == null) {
            return null;
        }
        return getStatistics(
            table,
            repos,
            FennelRelUtil.getPreparingStmt(rel).getSession()
                         .getSessionLabelCreationTimestamp());
    }

    private static RelStatSource getStatistics(
        RelOptTable table,
        FarragoRepos repos,
        Timestamp labelTimestamp)
    {
        String [] qualifiedName = table.getQualifiedName();
        assert qualifiedName.length == 3
            : "qualified name did not have three parts";
        String catalogName = qualifiedName[0];
        String schemaName = qualifiedName[1];
        String tableName = qualifiedName[2];

        CwmCatalog catalog = repos.getCatalog(catalogName);
        if (catalog == null) {
            return null;
        }
        FemLocalSchema schema =
            FarragoCatalogUtil.getSchemaByName(catalog, schemaName);
        if (schema == null) {
            return null;
        }
        FemAbstractColumnSet columnSet =
            FarragoCatalogUtil.getModelElementByNameAndType(
                schema.getOwnedElement(),
                tableName,
                FemAbstractColumnSet.class);
        if (columnSet == null) {
            return null;
        }

        RelStatSource result =
            new FarragoTableStatistics(
                repos,
                columnSet,
                labelTimestamp);
        return result;
    }

    /**
     * Retrieves the row count of a Farrago expression or null, using statistics
     * stored in the catalog
     *
     * @param rel the relational expression
     * @param repos repository
     *
     * @return the row count, or null if stats aren't available
     */
    public static Double getRowCountStat(RelNode rel, FarragoRepos repos)
    {
        Double result = null;
        RelStatSource source = getStatistics(rel, repos);
        if (source != null) {
            result = source.getRowCount();
        }
        return result;
    }

    /**
     * Retrieves the row count of a relational table using statistics stored in
     * the catalog
     *
     * @param table the relational table
     * @param repos repository
     *
     * @return the row count, or null if stats aren't available
     */
    public static Double getRowCountStat(
        RelOptTable table,
        FarragoRepos repos)
    {
        return getRowCountStat(table, repos, null);
    }

    /**
     * Retrieves the row count of a relational table for a specific label, using
     * statistics stored in the catalog.
     *
     * @param table the relational table
     * @param repos repository
     * @param labelTimestamp creation timestamp of the label that determines
     * which stats to retrieve; null if there is no label setting
     *
     * @return the row count, or null if stats aren't available
     */
    public static Double getRowCountStat(
        RelOptTable table,
        FarragoRepos repos,
        Timestamp labelTimestamp)
    {
        Double result = null;
        RelStatSource source = getStatistics(table, repos, labelTimestamp);
        if (source != null) {
            result = source.getRowCount();
        }
        return result;
    }

    /**
     * Retrieves the row count of a Farrago expression or null
     *
     * @param rel the relational expression
     *
     * @return the row count, or null if the row count was not available
     */
    public Double getRowCount(RelNode rel)
    {
        return getRowCountStat(rel, repos);
    }

    public Set<BitSet> getUniqueKeys(RelNode rel, boolean ignoreNulls)
    {
        return columnMd.getUniqueKeys(rel, repos, ignoreNulls);
    }

    public Boolean areColumnsUnique(
        RelNode rel,
        BitSet columns,
        boolean ignoreNulls)
    {
        return columnMd.areColumnsUnique(rel, columns, repos, ignoreNulls);
    }

    public Double getPopulationSize(RelNode rel, BitSet groupKey)
    {
        return columnMd.getPopulationSize(rel, groupKey);
    }

    public Double getDistinctRowCount(
        RelNode rel,
        BitSet groupKey,
        RexNode predicate)
    {
        return columnMd.getDistinctRowCount(rel, groupKey, predicate);
    }

    public Boolean canRestart(RelNode rel)
    {
        // TODO jvs 4-Nov-2006:  Override this to ignore children
        // and return true in cases where we know buffering
        // is already being done.

        for (RelNode child : rel.getInputs()) {
            if (!FarragoRelMetadataQuery.canRestart(child)) {
                return false;
            }
        }
        return true;
    }
}

// End FarragoRelMetadataProvider.java
