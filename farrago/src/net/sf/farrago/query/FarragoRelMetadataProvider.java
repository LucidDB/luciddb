/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;

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
            "areColumnsUnique",
            Collections.singletonList((Class) BitSet.class));
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

        String [] qualifiedName = table.getQualifiedName();
        assert (qualifiedName.length == 3) : "qualified name did not have three parts";
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

        RelStatSource result = new FarragoTableStatistics(repos, columnSet);
        return result;
    }

    /**
     * Retrieves the row count of a Farrago expression or null, using
     * statistics stored in the catalog
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

    public Set<BitSet> getUniqueKeys(RelNode rel)
    {
        return columnMd.getUniqueKeys(rel, repos);
    }

    public Boolean areColumnsUnique(RelNode rel, BitSet columns)
    {
        return columnMd.areColumnsUnique(rel, columns, repos);
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
