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
package net.sf.farrago.ddl;

import java.sql.*;
import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.SqlWriter.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util14.*;


/**
 * DdlAnalyzeStmt is a Farrago statement for computing the statistics of a
 * relational expression and storing them in repository. Currently, sample table
 * is supported.
 *
 * @author John Pham, Stephan Zuercher
 * @version $Id$
 */
public class DdlAnalyzeStmt
    extends DdlStmt
{
    //~ Static fields/initializers ---------------------------------------------

    private final static int DEFAULT_HISTOGRAM_BAR_COUNT = 100;

    public static final String REPEATABLE_SEED = "test.estimateStatsSeed";

    //~ Instance fields --------------------------------------------------------

    // ddl fields
    private CwmTable table;
    private List<CwmColumn> columnList;
    private boolean estimate;
    private boolean computeRowCount;
    private SqlNumericLiteral samplePercent;
    private Integer sampleRepeatableSeed;
    
    // convenience fields
    private FemAbstractColumnSet femTable;
    private FarragoSessionStmtContext stmtContext;
    private SqlPrettyWriter writer;
    private SqlIdentifier tableName;
    private FarragoRepos repos;

    //~ Constructors -----------------------------------------------------------

    public DdlAnalyzeStmt(CwmTable table)
    {
        super(table, true);
        this.table = table;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the list of columns to be analyzed
     *
     * @param columnList list of {@link CwmColumn} repository objects
     */
    public void setColumns(List<CwmColumn> columnList)
    {
        this.columnList = columnList;
    }

    public void setEstimateOption(boolean estimate)
    {
        this.estimate = estimate;
    }

    public void setSamplePercent(SqlNumericLiteral percent)
    {
        samplePercent = percent;
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement FarragoSessionDdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        // Use a reentrant session to simplify cleanup.
        FarragoSession session = ddlValidator.newReentrantSession();
        try {
            analyzeTable(ddlValidator, session);
        } catch (Throwable ex) {
            throw FarragoResource.instance().ValidatorAnalyzeFailed.ex(ex);
        } finally {
            ddlValidator.releaseReentrantSession(session);
        }
    }

    /**
     * Analyzes a table and associated indexes and columns. The following data
     * are collected:
     *
     * <ul>
     * <li>The number of rows in the table
     * <li>The number of pages in each associated index
     * <li>A histogram of each column specified
     * <li>The number of distinct values for the column.
     * </ul>
     *
     * This implementation issues recursive SQL.
     *
     * @param session reentrant session
     *
     * @throws RuntimeException if the specified table cannot be analyzed
     * @throws SQLExceptoin if there is an error executing the SQL necessary
     *                      for statistics generation
     */
    private void analyzeTable(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
        throws SQLException, RuntimeException
    {
        // null param def factory okay because the SQL does not use dynamic
        // parameters
        stmtContext = session.newStmtContext(null);
        SqlDialect dialect = new SqlDialect(session.getDatabaseMetaData());
        writer = new SqlPrettyWriter(dialect);
        tableName = FarragoCatalogUtil.getQualifiedName(table);
        repos = session.getRepos();

        // Cast abstract catalog objects to required types
        List<FemAbstractColumn> femColumnList = checkCatalogTypes();

        long deletedRowCount = 0;
        // Computing row count implies running a query to calculate row count
        // and then later storing the value in the catalog.  If we don't
        // compute, we don't update the catalog row count.
        computeRowCount = true;
        if (estimate && 
            personalityManagesRowCount(ddlValidator) && 
            femTable.getRowCount() != null)
        {
            // Don't compute row counts during estimate if the personality
            // maintains the catalog's row count
            computeRowCount = false;

            Long delRowCnt = femTable.getDeletedRowCount();
            if (delRowCnt != null) {
                deletedRowCount = delRowCnt.longValue();
            }
        }
        
        // Obtain or compute row counts 
        long rowCount = getRowCount();
        
        if (estimate) {
            setSampleRepeatableSeed(ddlValidator);
            
            if (samplePercent == null) {
                // Choose a reasonable sampling rate.
                chooseSamplePercentage(rowCount);
            }
            
            // 100% sampling means switch to calculated stats (but keep using
            // the catalog's row count if available).
            if (samplePercent.bigDecimalValue().doubleValue() >= 100.0) {
                estimate = false;
                samplePercent = null;
            }
        }
        
        LinkedHashMap<CwmColumn, Histogram> histograms = 
            new LinkedHashMap<CwmColumn, Histogram>();
        if (estimate) {
            estimateStats(femColumnList, rowCount, histograms);            
        } else {
            // Compute column histograms
            for (FemAbstractColumn column : femColumnList) {
                computeColumnStats(histograms, column, rowCount);
            }
        }
        
        // Compute index page counts and optionally compute distinct value
        // counts.
        Map<FemLocalIndex, Long> indexPageCounts = 
            analyzeIndexes(
                ddlValidator,
                rowCount,
                deletedRowCount,
                histograms);

        // Update stats computed above
        updateStats(
            repos,
            rowCount,
            computeRowCount, 
            histograms.values(),
            indexPageCounts);
    }

    /**
     * Verifies that statistics can be estimated for the given table.
     * 
     * @return list of {@link FemAbstractColumn} instances if the table can
     *         be analyzed
     * @throws RuntimeException if analyze is not support for the table
     */
    private List<FemAbstractColumn> checkCatalogTypes()
    {
        List<FemAbstractColumn> femColumnList =
            new ArrayList<FemAbstractColumn>();
        try {
            femTable = (FemAbstractColumnSet) table;
            for (CwmColumn column : columnList) {
                femColumnList.add((FemAbstractColumn) column);
            }
        } catch (ClassCastException e) {
            throw FarragoResource.instance().ValidatorAnalyzeNotSupported.ex(
                table.getName());
        }
        return femColumnList;
    }
    
    /**
     * Returns true if the personality maintains an accurate row count and
     * deleted row count in {@link FemAbstractColumnSet}.
     */
    private boolean personalityManagesRowCount(
        FarragoSessionDdlValidator ddlValidator)
    {
        EigenbaseResource featureResource = 
            EigenbaseResource.instance();
        
        FarragoSessionPersonality personality = 
            ddlValidator.getInvokingSession().getPersonality();
        
        return 
            personality.supportsFeature(
                featureResource.PersonalityManagesRowCount);
    }
    
    /**
     * Retrieve the table's row count.  Queries the table if 
     * {@link #computeRowCount} is true, otherwise uses the value stored in
     * {@link #femTable}.
     *
     * @return table's row count
     * @throws SQLException if there's an error querying the table
     */
    private long getRowCount() throws SQLException
    {
        long rowCount;
        if (computeRowCount) {
            rowCount = computeRowCount();
        } else {
            rowCount = femTable.getRowCount();
        }
        return rowCount;
    }

    /**
     * Executes a query against the table to compute the row count.
     * 
     * @return table's row count
     * @throws SQLException if there's an error querying the table
     */
    private long computeRowCount() throws SQLException
    {
        String sql = getRowCountQuery();
        stmtContext.prepare(sql, true);
        checkRowCountQuery();

        stmtContext.execute();
        ResultSet resultSet = stmtContext.getResultSet();
        boolean gotRow = resultSet.next();
        assert (gotRow);
        long rowCount = resultSet.getLong(1);
        resultSet.close();

        return rowCount;
    }

    /**
     * Generate a query to count a table's rows.
     */
    private String getRowCountQuery()
    {
        writer.print("select count(*) from ");
        tableName.unparse(writer, 0, 0);
        String sql = writer.toString();
        return sql;
    }

    /**
     * Validate that the {@link RelDataType} of the current 
     * {@link #stmtContext} matches a table row count query.
     */
    private void checkRowCountQuery()
    {
        RelDataType rowType = stmtContext.getPreparedRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        assert (fieldList.size() == 1) : "row count wrong number of columns";
        RelDataType type = fieldList.get(0).getType();
        assert (SqlTypeUtil.isExactNumeric(type)) : "row count invalid type";
    }

    /**
     * Looks up the current {@link FarragoSessionVariables session variables}
     * and sets {@link #sampleRepeatableSeed} to the repeatable seed value
     * that <b>may</b> be stored there for testing purposes.
     */
    private void setSampleRepeatableSeed(
        FarragoSessionDdlValidator ddlValidator)
    {
        // For testing, it's useful to get repeatable results from 
        // sampling.  Check to see if a special session variable is
        // set, and if so use it's value as the repeatable seed when
        // generating TABLESAMPLE clauses.
        FarragoSessionVariables vars =
            ddlValidator.getInvokingSession().getSessionVariables();
        if (vars.containsVariable(REPEATABLE_SEED)) {
            String seedString = vars.get(REPEATABLE_SEED);
            if (seedString != null) {
                try {
                    sampleRepeatableSeed = Integer.parseInt(seedString);
                } catch(NumberFormatException e) {
                    sampleRepeatableSeed = seedString.hashCode();
                }
            }
        }
    }

    /**
     * Choose an appropriate sampling percentage for a table with the given
     * row count.  Sets {@link #samplePercent}.
     */
    private void chooseSamplePercentage(long rowCount)
    {
        String rate;
        if (rowCount < 1000L) {
            rate = "100";
        } else if (rowCount < 10000L) {
            rate = "25";
        } else if (rowCount < 100000L) {
            rate = "10";
        } else {
            rate = "1";
        }
        
        samplePercent = 
            SqlNumericLiteral.createExactNumeric(rate, new SqlParserPos(0, 0));
    }

    /**
     * Iterate over the given {@link FemAbstractColumn} instances compute
     * histograms and cardinality from sampled data.  Passes information
     * about a column gleaned from its constraints to the single-column
     * estimation method.
     * 
     * @param femColumnList list of columns to analyze
     * @param rowCount row count of the table
     * @param histograms a map of columns to histograms with predictable
     *                   iteration order
     * @throws SQLException if a sampling query fails
     */
    private void estimateStats(
        List<FemAbstractColumn> femColumnList,
        long rowCount,
        LinkedHashMap<CwmColumn, Histogram> histograms)
        throws SQLException
    {
        // Estimate column histograms

        // BitSet of column ordinal values that are part of a unique or 
        // primary key constraint, but only for those constraints that 
        // contain a single column.  Implies cardinality = row count.
        BitSet singleUniqueCols = new BitSet();
        
        // BitSet of column ordinal values for unique/primary key constrained
        // columns where the column is nullable.  Implies 
        // cardinality = (row count - number of null values).
        BitSet singleUniqueColsNullable = new BitSet();
        
        FemPrimaryKeyConstraint primaryKey =
            FarragoCatalogUtil.getPrimaryKey(table);
        if (primaryKey != null && primaryKey.getFeature().size() == 1) {
            FemAbstractColumn col = 
                (FemAbstractColumn)primaryKey.getFeature().get(0);
            singleUniqueCols.set(col.getOrdinal());
        }
        
        List<FemUniqueKeyConstraint> uniqueKeys = 
            FarragoCatalogUtil.getUniqueKeyConstraints(table);
        for(FemUniqueKeyConstraint uniqueKey: uniqueKeys) {
            if (uniqueKey.getFeature().size() != 1) {
                continue;
            }
            
            FemAbstractColumn col = 
                (FemAbstractColumn)uniqueKey.getFeature().get(0);
            singleUniqueCols.set(col.getOrdinal());
            
            if (col.getIsNullable() == NullableTypeEnum.COLUMN_NULLABLE) {
                singleUniqueColsNullable.set(col.getOrdinal());
            }
        }
        
        for (FemAbstractColumn column : femColumnList) {
            int ordinal = column.getOrdinal();
            boolean isUnique = singleUniqueCols.get(ordinal);
            boolean isUniqueNullable = singleUniqueColsNullable.get(ordinal);
            estimateColumnStats(
                histograms, 
                column, 
                rowCount,
                isUnique,
                isUniqueNullable);
        }
    }

    /**
     * Build sampled histogram and estimate cardinality for the given column
     * by querying it.
     * 
     * @param histograms map in which to store generated Histogram
     * @param column the column to generate a histogram for
     * @param tableRowCount number of rows in the table
     * @param isUnique if true, the column has a uniqueness constraint that
     *                 applies to it only (e.g. single-column primary key or
     *                 single-column unique constraint)
     * @param isUniqueNullable if true, the column allows nulls (ignored if
     *                         isUnique is false)
     * @throws SQLException if there's an error executing the sampling query
     */
    private void estimateColumnStats(
        Map<CwmColumn, Histogram> histograms,
        FemAbstractColumn column,
        long tableRowCount,
        boolean isUnique,
        boolean isUniqueNullable)
        throws SQLException
    {
        assert(estimate);
        
        String sql = getColumnDistributionQuery(column);
        stmtContext.prepare(sql, true);
        checkColumnDistributionQuery();

        stmtContext.execute();
        ResultSet resultSet = stmtContext.getResultSet();
        Histogram columnHistogram =
            buildEstimatedHistogram(
                column, 
                tableRowCount,
                resultSet, 
                isUnique,
                isUniqueNullable);
        histograms.put(column, columnHistogram);
        resultSet.close();
    }

    /**
     * Iterate over the sample given in the result set and generate a 
     * histogram for the column.
     * 
     * @param column the column to generate a histogram for
     * @param tableRowCount number of rows in the table
     * @param resultSet containing column samples aggregated by value
     * @param isUnique if true, the column has a uniqueness constraint that
     *                 applies to it only (e.g. single-column primary key or
     *                 single-column unique constraint)
     * @param isUniqueNullable if true, the column allows nulls (ignored if
     *                         isUnique is false)
     * @return the column's sampled Histogram
     * @throws SQLException if there's an error reading the sample
     */
    private Histogram buildEstimatedHistogram(
        FemAbstractColumn column,
        long tableRowCount,
        ResultSet resultSet, 
        boolean isUnique, 
        boolean isUniqueNullable)
        throws SQLException
    {
        assert(estimate);
        
        FarragoCardinalityEstimator estimator = 
            new FarragoCardinalityEstimator(tableRowCount);

        double sampleRate =
            samplePercent.bigDecimalValue().movePointLeft(2).doubleValue();

        long sampleSizeEstimate = 
            Math.round(sampleRate * (double)tableRowCount);
            
        long rowsPerBar = computeRowsPerHistogramBar(sampleSizeEstimate);

        List<ColumnHistogramBar> bars = 
            buildBars(resultSet, rowsPerBar, estimator);

        long sampleRowCount = estimator.getSampleSize();

        // Manipulate rowsLastBar to make sense.  Since we guessed at how
        // many rows were in the sample to begin with, we may have been off
        // resulting in bars.size() != DEFAULT_HISTOGRAM_BAR_COUNT.
        long rowsLastBar; 
        if (bars.size() > 1) {
            rowsLastBar = sampleRowCount - ((bars.size() - 1) * rowsPerBar);
        } else if (bars.size() == 1) {
            rowsLastBar = rowsPerBar;
        } else {
            rowsLastBar = 0;
        }

        long distinctValues;
        boolean distinctValuesEstimated;

        // Estimate distinct values (using constraint info)
        if (isUnique) {
            // Uniqueness constraint on this column.
            if (isUniqueNullable) {
                distinctValues = estimator.estimateDistinctWithNullClass();
                distinctValuesEstimated = true;
            } else {
                // Nulls are not allowed, so all values are distinct
                distinctValues = tableRowCount;
                distinctValuesEstimated = false;
            }                
        } else {
            distinctValues = estimator.estimate();
            distinctValuesEstimated = true;
        }
        
        return new Histogram(
            column,
            distinctValues,
            distinctValuesEstimated,
            bars.size(),
            rowsPerBar,
            rowsLastBar,
            sampleRowCount,
            bars);
    }
    
    
    /**
     * Build a complete histogram and calculate cardinality for the given 
     * column by querying it.
     * 
     * @param histograms map in which to store generated Histogram
     * @param column the column to generate a histogram for
     * @param tableRowCount number of rows in the table
     * @throws SQLException if there's an error executing the query
     */
    private void computeColumnStats(
        Map<CwmColumn, Histogram> histograms,
        FemAbstractColumn column,
        long tableRowCount)
        throws SQLException
    {
        assert(!estimate);
        
        String sql = getColumnDistributionQuery(column);
        stmtContext.prepare(sql, true);
        checkColumnDistributionQuery();

        stmtContext.execute();
        ResultSet resultSet = stmtContext.getResultSet();
        Histogram columnHistogram =
            buildHistogram(column, tableRowCount, resultSet);
        histograms.put(column, columnHistogram);
        resultSet.close();
    }

    /**
     * Iterate over the given result set and generate a histogram for the 
     * column.
     * 
     * @param column the column to generate a histogram for
     * @param tableRowCount number of rows in the table
     * @param resultSet containing column data aggregated by value
     * @return the column's Histogram
     * @throws SQLException if there's an error reading the sample
     */
    private Histogram buildHistogram(
        FemAbstractColumn column,
        long tableRowCount,
        ResultSet resultSet)
        throws SQLException
    {
        assert(!estimate);
        
        long rowsPerBar = computeRowsPerHistogramBar(tableRowCount);
        long rowsLastBar = 
            computeRowsLastHistogramBar(tableRowCount, rowsPerBar);

        List<ColumnHistogramBar> bars = buildBars(resultSet, rowsPerBar, null);

        long distinctValues = 0;
        for (ColumnHistogramBar bar : bars) {
            distinctValues += bar.valueCount;
        }
        
        return new Histogram(
            column,
            distinctValues,
            false,
            bars.size(),
            rowsPerBar,
            rowsLastBar,
            tableRowCount,
            bars);
    }

    /**
     * Generate a query to generate a columns distribution.  If the
     * {@link #estimate} flag is set, the query uses the TABLESAMPLE keyword
     * to sample the column's data.
     */
    private String getColumnDistributionQuery(
        FemAbstractColumn column)
    {
        SqlIdentifier columnName = FarragoCatalogUtil.getQualifiedName(column);
        writer.reset();
        
        final Frame selectFrame = writer.startList(FrameTypeEnum.Select);
        writer.sep("select");
        
        final Frame selectListFrame = 
            writer.startList(FrameTypeEnum.SelectList);
        columnName.unparse(writer, 0, 0);
        writer.sep(",");
        
        final Frame countFuncFrame = writer.startFunCall("count");
        writer.print("*");
        writer.endFunCall(countFuncFrame);
        writer.endList(selectListFrame);
        
        writer.sep("from");
        final Frame fromFrame = writer.startList(FrameTypeEnum.FromList);
        tableName.unparse(writer, 0, 0);
        
        if (estimate) {
            // Use system sampling for performance.  Farrago will revert to
            // Bernoulli sampling if system sampling is not available for
            // this table.
            Frame frame = writer.startFunCall("tablesample system");
            samplePercent.unparse(writer, 0, 0);
            writer.endFunCall(frame);
            
            if (sampleRepeatableSeed != null) {
                frame = writer.startFunCall("repeatable");
                writer.literal(sampleRepeatableSeed.toString());
                writer.endFunCall(frame);
            }
        }
        
        writer.endList(fromFrame);

        writer.sep("group by");
        final Frame groupByFrame = writer.startList(FrameTypeEnum.GroupByList);
        columnName.unparse(writer, 0, 0);
        writer.endList(groupByFrame);
        
        writer.sep("order by");
        final Frame orderByFrame = writer.startList(FrameTypeEnum.OrderByList);
        columnName.unparse(writer, 0, 0);
        writer.endList(orderByFrame);
        writer.endList(selectFrame);
        
        String sql = writer.toString();
        return sql;
    }

    
    /**
     * Validate that the {@link RelDataType} of the current 
     * {@link #stmtContext} matches what's expected from a column distribution
     * query.
     */
    private void checkColumnDistributionQuery()
    {
        RelDataType rowType = stmtContext.getPreparedRowType();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        assert (fieldList.size() == 2) : "column query wrong number of columns";
        RelDataType type = fieldList.get(1).getType();
        assert (SqlTypeUtil.isExactNumeric(type)) : "column query invalid type";
    }

    /**
     * Compute the number of rows per histogram bar based on the 
     * {@link #DEFAULT_HISTOGRAM_BAR_COUNT}.
     * 
     * @param rowCount number of rows from the table that will be represented 
     *                 by the histogram 
     */
    private long computeRowsPerHistogramBar(final long rowCount)
    {
        if (rowCount <= DEFAULT_HISTOGRAM_BAR_COUNT) {
            return 1;
        }
        
        long rowsPerBar = rowCount / DEFAULT_HISTOGRAM_BAR_COUNT;
        if ((rowCount % DEFAULT_HISTOGRAM_BAR_COUNT) != 0) {
            rowsPerBar++;
        }
        
        return rowsPerBar;
    }

    /**
     * Compute the number of rows in the last histogram bar based on the 
     * {@link #DEFAULT_HISTOGRAM_BAR_COUNT}.
     * 
     * @param rowCount number of rows from the table that will be represented 
     *                 by the histogram 
     * @param rowsPerBar the result of 
     *                   {@link #computeRowsPerHistogramBar(long)}
     */
    private long computeRowsLastHistogramBar(
        final long rowCount, final long rowsPerBar)
    {
        if (rowCount <= DEFAULT_HISTOGRAM_BAR_COUNT) {
            assert(rowsPerBar == 1);
            return 1;
        }

        long rowsLastBar;
        if ((rowCount % rowsPerBar) != 0) {
            int barCount = (int) (rowCount / rowsPerBar);
            rowsLastBar = (rowCount - (barCount * rowsPerBar));
        } else {
            rowsLastBar = rowsPerBar;
        }

        return rowsLastBar;
    }
    
    /**
     * Given a result set from a column distribution query, compute the 
     * bars in the Histogram.
     * 
     * @param resultSet column distribution query result set
     * @param rowsPerBar the number of rows per bar
     * @param estimator an estimator to populate with data from the result set;
     *                  may be null 
     * @return a list of {@link ColumnHistogramBar} instances
     * @throws SQLException if there's an error reading the result set
     */
    private List<ColumnHistogramBar> buildBars(
        ResultSet resultSet,
        long rowsPerBar,
        FarragoCardinalityEstimator estimator)
        throws SQLException
    {
        List<ColumnHistogramBar> bars = new LinkedList<ColumnHistogramBar>();
        boolean newBar = true;
        String barStartValue = null;
        long barValueCount = 0;
        long barRowCount = 0;

        while (resultSet.next()) {
            Object o = resultSet.getObject(1);
            String nextValue;
            if (o == null) {
                nextValue = null;
            } else if (o instanceof byte []) {
                nextValue =
                    ConversionUtil.toStringFromByteArray((byte []) o, 16);
            } else {
                nextValue = resultSet.getString(1);
            }
            long nextRows = resultSet.getLong(2);

            if (estimator != null) {
                estimator.addSampleClass(nextRows, nextValue == null);
            }
            
            if (newBar) {
                barStartValue = nextValue;
                barValueCount = 0;
                barRowCount = 0;
                newBar = false;
            }
            barValueCount++;
            barRowCount += nextRows;

            while (barRowCount >= rowsPerBar) {
                ColumnHistogramBar bar =
                    new ColumnHistogramBar(barStartValue, barValueCount);
                bars.add(bar);

                barRowCount -= rowsPerBar;
                if (barRowCount > 0) {
                    // the next bar starts with the current value
                    barStartValue = nextValue;
                    barValueCount = 0;
                } else {
                    newBar = true;
                }
            }
        }

        // build partial last bars
        if (barRowCount > 0) {
            bars.add(new ColumnHistogramBar(barStartValue, barValueCount));
        }
        return bars;
    }

    /**
     * Convert the {@link ColumnHistogramBar} instances in the given
     * {@link Histogram} into {@link FemColumnHistogramBar} instances.
     */
    private void buildFemBars(
        Histogram histogram,
        List<FemColumnHistogramBar> femBars)
    {
        // If possible, try to reuse the original histogram
        FemColumnHistogram origHistogram = histogram.column.getHistogram();
        int origBarCount = 0;
        List<FemColumnHistogramBar> origBars = null;
        if (origHistogram != null) {
            origBarCount = origHistogram.getBarCount();
            origBars = origHistogram.getBar();
        }

        int count = 0;
        for (ColumnHistogramBar bar : histogram.bars) {
            FemColumnHistogramBar femBar;
            if (count < origBarCount) {
                femBar = origBars.get(count);
            } else {
                femBar = repos.newFemColumnHistogramBar();
            }
            femBar.setStartingValue(bar.startValue);
            femBar.setValueCount(bar.valueCount);
            femBars.add(femBar);
            count++;
        }
    }

    /**
     * Analyze the table's indexes.  If estimated statistics are in use,
     * the index statistics will also be estimated, unless an index can
     * provide a more accurate cardinality for a given column.
     * 
     * @param ddlValidator used to execute index statistics gathering
     * @param rowCount table's row count
     * @param deletedRowCount number of deleted rows in the table store 
     *                        (usually zero outside LucidDb)
     * @param histograms previously gathered column histograms
     * @return a map of index to page count
     */
    private Map<FemLocalIndex, Long> analyzeIndexes(
        FarragoSessionDdlValidator ddlValidator,
        long rowCount,
        long deletedRowCount,
        LinkedHashMap<CwmColumn, Histogram> histograms)
    {
        Map<FemLocalIndex, Long> indexPageCounts =
            new HashMap<FemLocalIndex, Long>();
        Collection<FemLocalIndex> tableIndexes =
            FarragoCatalogUtil.getTableIndexes(repos, table);
        
        HashSet<FemLocalIndex> computedStatsSet = null;
        if (estimate) {
            // Map columns to the index that will be used for distinct
            // value counting, preferring single-column indexes to
            // multi-column indexes.
            HashMap<FemAbstractColumn, FemLocalIndex> countMap = 
                new HashMap<FemAbstractColumn, FemLocalIndex>();
            
            for (FemLocalIndex index: tableIndexes) {
                if (!index.isClustered()) {
                    List<CwmIndexedFeature> indexedColumns = 
                        index.getIndexedFeature();
                    if (indexedColumns.isEmpty()) {
                        continue;
                    }
                    FemAbstractColumn column = 
                        (FemAbstractColumn)indexedColumns.get(0).getFeature();

                    Histogram histogram = histograms.get(column); 
                    if (histogram == null || 
                        !histogram.distinctValuesEstimated) 
                    {
                        // Not analyzing column or already used a constraint 
                        // to accurately compute distinct value count.
                        continue;
                    }
                    
                    if (indexedColumns.size() == 1 || 
                        !countMap.containsKey(column))
                    {
                        countMap.put(column, index);
                    }
                }
            }
            
            computedStatsSet  = new HashSet<FemLocalIndex>(countMap.values());
        }
        
        for (FemLocalIndex index: tableIndexes) {
            // Default is to estimate index page counts when statistics
            // are estimated.  For user-defined indexes we override this
            // behavior and compute statistics in order to retrieve the
            // number of distinct values in the index.
            boolean estimateThisIndex = estimate;
            FemAbstractColumn column = null;
            if (estimateThisIndex && computedStatsSet.contains(index)) {
                estimateThisIndex = false;
                
                column = 
                    (FemAbstractColumn)index.getIndexedFeature().get(0).getFeature();
            }
            
            FarragoMedLocalIndexStats indexStats =
                ddlValidator.getIndexMap().computeIndexStats(
                    ddlValidator.getDataWrapperCache(),
                    index,
                    estimateThisIndex);

            long pageCount = indexStats.getPageCount();

            indexPageCounts.put(index, pageCount);
            
            if (estimate && !estimateThisIndex) {
                long indexDistinctValues = indexStats.getUniqueKeyCount();
                assert(indexDistinctValues >= 0);
                
                Histogram histogram = histograms.get(column);

                // Get the unique key count and place it in the histogram if
                // it's better than the estimate we have.
                if (deletedRowCount == 0) {
                    histogram.distinctValues = indexDistinctValues;
                    histogram.distinctValuesEstimated = false;
                } else {
                    // Estimate distinct values in undeleted rows by computing
                    // the number of distinct values per row and applying that
                    // to just the undeleted row count.
                    double rate = 
                        (double)indexDistinctValues / 
                        (double)(deletedRowCount + rowCount);
                    long estDistinctValues = 
                        Math.round(rate * (double)rowCount);
                    
                    // If distinct values is 0 we must have seen zero rows
                    // in the sample.  Otherwise, if the estimate from the
                    // index is much larger than the estimate from the sample,
                    // use the index-based estimate.
                    if (histogram.distinctValues > 0 &&
                        (estDistinctValues / histogram.distinctValues) >= 10.0)
                    {
                        histogram.distinctValues = estDistinctValues;
                        histogram.distinctValuesEstimated = true;
                    }
                }
            }
        }
        return indexPageCounts;
    }

    /**
     * Updates catalog records with new statistical data, all within a single
     * MDR write txn.
     *
     * @param repos repository
     * @param rowCount table rowcount
     * @param updateRowCount if true, update the catalog row count
     * @param histograms column histograms
     * @param indexPageCounts index page counts
     */
    private void updateStats(
        FarragoRepos repos,
        long rowCount,
        final boolean updateRowCount,
        Collection<Histogram> histograms,
        Map<FemLocalIndex, Long> indexPageCounts)
    {
        FarragoReposTxnContext txn = repos.newTxnContext();

        final float rate = 
            estimate ? samplePercent.bigDecimalValue().floatValue() : 100.0f;
        
        try {
            txn.beginWriteTxn();
            FarragoCatalogUtil.updateRowCount(
                femTable, rowCount, updateRowCount, true);

            for (Histogram histogram : histograms) {
                List<FemColumnHistogramBar> femBars =
                    new LinkedList<FemColumnHistogramBar>();
                buildFemBars(histogram, femBars);
                FarragoCatalogUtil.updateHistogram(
                    repos,
                    histogram.column,
                    histogram.distinctValues,
                    histogram.distinctValuesEstimated,
                    rate,
                    histogram.sampleSize,
                    histogram.barCount,
                    histogram.rowsPerBar,
                    histogram.rowsLastBar,
                    femBars);
            }

            Set<FemLocalIndex> indexes = indexPageCounts.keySet();
            for (FemLocalIndex index : indexes) {
                FarragoCatalogUtil.updatePageCount(
                    index,
                    indexPageCounts.get(index));
            }

            txn.commit();
        } finally {
            txn.rollback();
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Class used to store column histogram information
     */
    private class Histogram
    {
        FemAbstractColumn column;
        long distinctValues;
        boolean distinctValuesEstimated;
        int barCount;
        long rowsPerBar;
        long rowsLastBar;
        long sampleSize;
        List<ColumnHistogramBar> bars;

        Histogram(
            FemAbstractColumn column,
            Long distinctValues,
            boolean distinctValuesEstimated,
            int barCount,
            long rowsPerBar,
            long rowsLastBar,
            long sampleSize,
            List<ColumnHistogramBar> bars)
        {
            this.column = column;
            this.distinctValues = distinctValues;
            this.distinctValuesEstimated = distinctValuesEstimated;
            this.barCount = barCount;
            this.rowsPerBar = rowsPerBar;
            this.rowsLastBar = rowsLastBar;
            this.sampleSize = sampleSize;
            this.bars = bars;
        }
    }

    /**
     * Class used to store column histogram bar information
     */
    private class ColumnHistogramBar
    {
        String startValue;
        long valueCount;

        ColumnHistogramBar(String startValue, long valueCount)
        {
            this.startValue = startValue;
            this.valueCount = valueCount;
        }
    }
}

// End DdlAnalyzeStmt.java
