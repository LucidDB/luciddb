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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.impl.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.stat.*;

/**
 * FarragoRelMetadataProvider implements Farrago specific metadata. 
 * Initially, it provides costing information available through statistics.
 * Other potential uses of this class are:
 * 
 * <ol>
 *   <li>Provide cost information for Farrago rels</li>
 *   <li>Provides a uniform cost model for all rels of concern to 
 *      Farrago</li>
 * </ol>
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoRelMetadataProvider extends ReflectiveRelMetadataProvider
{
    private FarragoRepos repos;
    
    /**
     * Initializes a provider with access to the Farrago catalog. The 
     * provider reads statistics stored in the catalog.
     * 
     * @param repos the Farrago catalog
     */
    public FarragoRelMetadataProvider(FarragoRepos repos)
    {
        this.repos = repos;
    }
    
    /**
     * Retrieves statistics for a relational expression, if they exist 
     * in the catalog.
     * 
     * @param rel the relational expression
     * 
     * @return the statistics object, or null
     */
    public RelStatSource getStatistics(RelNode rel)
    {
        RelOptTable table = rel.getTable();
        if (table == null) {
            return null;
        }
        
        String[] qualifiedName = table.getQualifiedName();
        assert (qualifiedName.length == 3) 
            : "qualified name did not have three parts";
        String catalogName = qualifiedName[0];
        String schemaName = qualifiedName[1];
        String tableName = qualifiedName[2];
        
        CwmCatalog catalog = repos.getCatalog(catalogName);
        if (catalog == null) {
            return null;
        }
        FemLocalSchema schema = 
            FarragoCatalogUtil.getSchemaByName(repos, catalog, schemaName);
        if (schema == null) {
            return null;
        }
        FemAbstractColumnSet columnSet = (FemAbstractColumnSet) 
            FarragoCatalogUtil.getModelElementByNameAndType(
                schema.getOwnedElement(), tableName, 
                repos.getFemPackage().getSql2003().getFemAbstractColumnSet());
        if (columnSet == null) {
            return null;
        }
 
        RelStatSource result = new FarragoTableStatistics(repos, columnSet);
        return result;
    }
    
    /**
     * Retrieves the row count of a Farrago expression or null
     * 
     * @param rel the relational expression
     * 
     * @return the row count, or null if the row count was not available
     */
    public Double getRowCount(RelNode rel) {
        Double result = null;
        RelStatSource source = getStatistics(rel);
        if (source != null) {
            result = source.getRowCount();
        }
        return result;
    }
    
    public Set<BitSet> getUniqueKeys(RelNode rel)
    {      
        // this method only handles table level relnodes
        if (rel.getTable() == null) {
            return null;
        }
        
        MedAbstractColumnSet table = (MedAbstractColumnSet) rel.getTable();
        Set<BitSet> retSet = new HashSet<BitSet>();
        
        // first retrieve the columns from the primary key
        FemPrimaryKeyConstraint primKey =
            FarragoCatalogUtil.getPrimaryKey(table.getCwmColumnSet());
        if (primKey != null) {
            addKeyCols(primKey.getFeature(), false, retSet);
        }
        
        // then, loop through each unique constraint, looking for unique
        // constraints where all columns in the constraint are non-null
        List<FemUniqueKeyConstraint> uniqueConstraints = 
            FarragoCatalogUtil.getUniqueKeyConstraints(
                table.getCwmColumnSet());
        for (FemUniqueKeyConstraint uniqueConstraint : uniqueConstraints) {
            addKeyCols(uniqueConstraint.getFeature(), true, retSet);
        }
        
        return retSet;
    }
    
    /**
     * Forms bitmaps representing the columns in a constraint and adds them
     * to a set
     * 
     * @param keyCols list of columns that make up a constraint
     * @param checkNulls if true, don't add the columns of the constraint if
     * the columns allow nulls
     * @param keyList the set where the bitmaps will be added
     */
    private void addKeyCols(
        List<FemAbstractColumn> keyCols, boolean checkNulls,
        Set<BitSet> keyList)
    {
        BitSet colMask = new BitSet();
        boolean nullFound = false;
        for (FemAbstractColumn keyCol : keyCols) {          
            if (checkNulls &&
                FarragoCatalogUtil.isColumnNullable(repos, keyCol))
            {
                nullFound = true;
                break;
            }
            colMask.set(keyCol.getOrdinal());
        }
        if (!nullFound) {
            keyList.add(colMask);
        }
    }
    
    public Double getPopulationSize(TableAccessRel rel, BitSet groupKey)
    {
        double population = 1.0;
        
        // if columns are part of a unique key, then just return the rowcount
        if (RelMdUtil.areColumnsUnique(rel, groupKey)) {
            return RelMetadataQuery.getRowCount(rel);
        }
        
        // if no stats are available, return null
        RelStatSource tabStats = RelMetadataQuery.getStatistics(rel);
        if (tabStats == null) {
            return null;
        }
        
        // multiply by the cardinality of each column
        for (int col = groupKey.nextSetBit(0); col >= 0;
            col = groupKey.nextSetBit(col + 1))
        {
            RelStatColumnStatistics colStats =
                tabStats.getColumnStatistics(col, null);
            if (colStats == null) {
                return null;
            }
            Double colCard = colStats.getCardinality();
            if (colCard == null) {
                return null;
            }
            population *= colCard;
        }
        
        // cap the number of distinct values
        return RelMdUtil.numDistinctVals(
            population, RelMetadataQuery.getRowCount(rel));
    }
}

// End FarragoRelMetadataProvider.java
