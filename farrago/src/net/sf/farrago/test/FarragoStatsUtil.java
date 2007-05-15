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
package net.sf.farrago.test;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.sql.validate.*;


/**
 * Utility class for manipulating statistics stored in the catalog
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoStatsUtil
{

    //~ Static fields/initializers ---------------------------------------------

    public static final int DEFAULT_HISTOGRAM_BAR_COUNT = 100;

    //~ Methods ----------------------------------------------------------------

    public static void setTableRowCount(
        FarragoSession session,
        String catalogName,
        String schemaName,
        String tableName,
        long rowCount)
        throws SqlValidatorException
    {
        FarragoRepos repos = session.getRepos();

        FarragoReposTxnContext txn = repos.newTxnContext();
        try {
            txn.beginWriteTxn();
            
            FemAbstractColumnSet columnSet =
                lookupColumnSet(
                    session,
                    repos,
                    catalogName,
                    schemaName,
                    tableName);
            FarragoCatalogUtil.updateRowCount(columnSet, rowCount);

            txn.commit();
        } finally {
            txn.rollback();
        }
    }
    
    public static CwmCatalog lookupCatalog(
        FarragoSession session,
        FarragoRepos repos,
        String catalogName)
        throws SqlValidatorException
    {
        if ((catalogName == null) || (catalogName.length() == 0)) {
            catalogName = session.getSessionVariables().catalogName;
        }
        CwmCatalog catalog = repos.getCatalog(catalogName);
        if (catalog == null) {
            throw FarragoResource.instance().ValidatorUnknownObject.ex(
                catalogName);
        }
        return catalog;
    }

    public static FemLocalSchema lookupSchema(
        FarragoSession session,
        CwmCatalog catalog,
        String schemaName)
        throws SqlValidatorException
    {
        if ((schemaName == null) || (schemaName.length() == 0)) {
            schemaName = session.getSessionVariables().schemaName;
        }
        FemLocalSchema schema =
            FarragoCatalogUtil.getSchemaByName(catalog, schemaName);
        if (schema == null) {
            throw FarragoResource.instance().ValidatorUnknownObject.ex(
                schemaName);
        }
        return schema;
    }

    public static FemAbstractColumnSet lookupColumnSet(
        FemLocalSchema schema,
        String tableName)
        throws SqlValidatorException
    {
        FemAbstractColumnSet columnSet = null;
        if (tableName != null) {
            columnSet =
                FarragoCatalogUtil.getModelElementByNameAndType(
                    schema.getOwnedElement(),
                    tableName,
                    FemAbstractColumnSet.class);
        }
        if (columnSet == null) {
            throw FarragoResource.instance().ValidatorUnknownObject.ex(
                tableName);
        }
        return columnSet;
    }
    
    /**
     * Maps a table, given its name components, to its corresponding
     * FemAbstractColumnSet
     * 
     * @param session currrent session
     * @param repos repository associated with the session
     * @param catalogName catalog name for the table
     * @param schemaName schema name for the table
     * @param tableName table name
     * 
     * @return FemAbstractColumnSet corresponding to the desired table
     * 
     * @throws SqlValidatorException
     */
    public static FemAbstractColumnSet lookupColumnSet(
        FarragoSession session,
        FarragoRepos repos,
        String catalogName,
        String schemaName,
        String tableName)
        throws SqlValidatorException
    {
        CwmCatalog catalog = lookupCatalog(session, repos, catalogName);
        FemLocalSchema schema = lookupSchema(session, catalog, schemaName);
        return lookupColumnSet(schema, tableName);
    }

    public static void setIndexPageCount(
        FarragoSession session,
        String catalogName,
        String schemaName,
        String indexName,
        long pageCount)
        throws SqlValidatorException
    {
        FarragoRepos repos = session.getRepos();

        FarragoReposTxnContext txn = repos.newTxnContext();
        try {
            txn.beginWriteTxn();

            CwmCatalog catalog = lookupCatalog(session, repos, catalogName);
            FemLocalSchema schema = lookupSchema(session, catalog, schemaName);
            FemLocalIndex index = lookupIndex(schema, indexName);
            FarragoCatalogUtil.updatePageCount(index, pageCount);

            txn.commit();
        } finally {
            txn.rollback();
        }
    }

    /**
     * Creates a column histogram
     */
    public static void createColumnHistogram(
        FarragoSession session,
        String catalogName,
        String schemaName,
        String tableName,
        String columnName,
        long distinctValues,
        int samplePercent,
        long sampleDistinctValues,
        int distributionType,
        String valueDigits)
        throws SqlValidatorException
    {
        FarragoRepos repos = session.getRepos();

        FarragoReposTxnContext txn = repos.newTxnContext();
        try {
            txn.beginWriteTxn();

            FemAbstractColumnSet columnSet =
                lookupColumnSet(
                    session,
                    repos,
                    catalogName,
                    schemaName,
                    tableName);
            FemAbstractColumn column = lookupColumn(columnSet, columnName);

            long rowCount = columnSet.getRowCount();
            long sampleRows = (rowCount * samplePercent) / 100;
            assert (distinctValues <= rowCount);
            assert (sampleDistinctValues <= distinctValues);
            assert (sampleDistinctValues <= sampleRows);
            int barCount = 0;
            long rowsPerBar = 0;
            long rowsLastBar = 0;
            if (sampleRows <= DEFAULT_HISTOGRAM_BAR_COUNT) {
                barCount = (int) sampleRows;
                rowsPerBar = rowsLastBar = 1;
            } else {
                barCount = DEFAULT_HISTOGRAM_BAR_COUNT;
                rowsPerBar = sampleRows / barCount;
                if ((sampleRows % barCount) != 0) {
                    rowsPerBar++;
                }
                rowsLastBar = sampleRows - ((barCount - 1) * rowsPerBar);
                if (rowsLastBar < 0) {
                    rowsLastBar = 0;
                }
            }
            List<FemColumnHistogramBar> bars =
                createColumnHistogramBars(
                    repos,
                    column,
                    sampleDistinctValues,
                    barCount,
                    rowsPerBar,
                    rowsLastBar,
                    distributionType,
                    valueDigits);

            FarragoCatalogUtil.updateHistogram(
                repos,
                column,
                distinctValues,
                samplePercent,
                barCount,
                rowsPerBar,
                rowsLastBar,
                bars);

            txn.commit();
        } finally {
            txn.rollback();
        }
    }

    private static List<FemColumnHistogramBar> createColumnHistogramBars(
        FarragoRepos repos,
        FemAbstractColumn column,
        long distinctValues,
        int barCount,
        long rowsPerBar,
        long rowsLastBar,
        int distributionType,
        String valueDigits)
    {
        FemColumnHistogram origHistogram = column.getHistogram();
        int origBarCount = 0;
        List<FemColumnHistogramBar> origBars = null;
        if (origHistogram != null) {
            origBarCount = origHistogram.getBarCount();
            origBars = origHistogram.getBar();
        }
        
        List<FemColumnHistogramBar> bars =
            new LinkedList<FemColumnHistogramBar>();
        List<Long> valueCounts =
            createValueCounts(barCount, distinctValues, distributionType);
        List<String> values =
            createValues(barCount, valueDigits, distributionType, valueCounts);
        for (int i = 0; i < barCount; i++) {
            FemColumnHistogramBar bar;
            if (i < origBarCount) {
                bar = origBars.get(i);
                assert(bar.getOrdinal() == i);
            } else {
                bar = repos.newFemColumnHistogramBar();
                bar.setOrdinal(i);
            }
            bar.setStartingValue(values.get(i));
            bar.setValueCount(valueCounts.get(i));
            bars.add(bar);
        }
        return bars;
    }

    private static List<String> createValues(
        int barCount,
        String valueDigits,
        int distributionType,
        List<Long> valueCounts)
    {
        int digitCount = valueDigits.length();
        assert (barCount <= (digitCount * digitCount));
        List<String> values = new ArrayList<String>(barCount);

        int iterations = barCount / digitCount;
        int residual = barCount % digitCount;
        for (int i = 0; i < digitCount; i++) {
            int currentIterations = iterations;
            if (i < residual) {
                currentIterations++;
            }
            for (int j = 0; j < currentIterations; j++) {
                char [] chars =
                    { valueDigits.charAt(i), valueDigits.charAt(j) };
                String next = new String(chars);
                if (distributionType > 0) {
                    next += valueDigits.charAt(0);
                }
                values.add(next);
            }
        }

        return values;
    }

    private static List<Long> createValueCounts(
        int barCount,
        long distinctValueCount,
        int distributionType)
    {
        List<Long> valueCounts = new ArrayList<Long>(barCount);
        Double estValuesPerBar =
            (double) distinctValueCount / (double) barCount;

        long remaining = distinctValueCount;
        for (int i = 0; i < barCount; i++) {
            long barStart = (long) Math.ceil(estValuesPerBar * i);
            long barEnd = (long) Math.ceil(estValuesPerBar * (i + 1));

            long current;
            if (i == (barCount - 1)) {
                current = remaining;
            } else {
                current = barEnd - barStart;
            }
            valueCounts.add(current);
            remaining -= current;
        }

        // note: the first bar should be at least 1
        if (barCount > 0) {
            assert (valueCounts.get(0) >= 1) : "first histogram bar must be at least 1";
            assert (remaining == 0) : "generated histogram bars had remaining distinct values";
        }

        return valueCounts;
    }

    private static FemLocalIndex lookupIndex(
        FemLocalSchema schema,
        String indexName)
        throws SqlValidatorException
    {
        FemLocalIndex index = null;
        if (indexName != null) {
            index =
                FarragoCatalogUtil.getModelElementByNameAndType(
                    schema.getOwnedElement(),
                    indexName,
                    FemLocalIndex.class);
        }
        if (index == null) {
            throw FarragoResource.instance().ValidatorUnknownObject.ex(
                indexName);
        }
        return index;
    }

    private static FemAbstractColumn lookupColumn(
        FemAbstractColumnSet columnSet,
        String columnName)
        throws SqlValidatorException
    {
        FemAbstractColumn column = null;
        if (columnName != null) {
            column =
                FarragoCatalogUtil.getModelElementByNameAndType(
                    columnSet.getFeature(),
                    columnName,
                    FemAbstractColumn.class);
        }
        if (column == null) {
            throw FarragoResource.instance().ValidatorUnknownObject.ex(
                columnName);
        }
        return column;
    }
}

// End FarragoStatsUtil.java
