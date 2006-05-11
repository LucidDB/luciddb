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
package net.sf.farrago.ddl;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;

import java.sql.*;
import java.util.*;

/**
 * DdlAnalyzeStmt is a Farrago statement for computing the statistics of a
 * relational expression and storing them in repository. Currently, sample
 * table is supported.
 *
 * @author John Pham
 * @version $Id$
 */
public class DdlAnalyzeStmt extends DdlStmt
{
    private final static int DEFAULT_HISTOGRAM_BAR_COUNT = 100;
    
    // ddl fields
    private CwmTable table;
    private List<CwmColumn> columnList;
    private boolean estimate;
    private int samplePercent;
    
    // convenience fields
    private FemAbstractColumnSet femTable;
    private FarragoSessionStmtContext stmtContext;
    private SqlPrettyWriter writer;
    private SqlIdentifier tableName;
    private FarragoRepos repos;

    //~ Constructors ------------------------------------------------------

    public DdlAnalyzeStmt()
    {
        super(null);
    }

    //~ Accessors ---------------------------------------------------------

    public void setTable(CwmTable table)
    {
        this.table = table;
    }

    /**
     * Sets the list of columns to be analyzed
     *
     * @param columnList list of CwmColumn repository objects
     */
    public void setColumns(List columnList)
    {
        this.columnList = columnList;
    }

    public void setEstimateOption(boolean estimate)
    {
        this.estimate = estimate;
    }

    public void setSamplePercent(int percent)
    {
        samplePercent = percent;
    }

    //~ Methods -----------------------------------------------------------

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
            throw FarragoResource.instance().ValidatorSetStmtInvalid.ex(ex);
        } finally {
            ddlValidator.releaseReentrantSession(session);
        }
    }

    /**
     * Analyzes a table and associated indexes and columns. The following
     * data are collected:
     * 
     * <ul>
     *   <li>The number of rows in the table
     *   <li>The number of pages in each associated index
     *   <li>A histogram of each column specified
     *   <li>For columns with indexes sorted by the column, 
     *       number of distinct values for the column.
     * </ul>
     * 
     * This implementation issues recursive SQL.
     * 
     * @param session reentrant session
     * @throws Exception
     */
    private void analyzeTable(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session)
    throws Exception
    {
        // null param def factory okay because the SQL does not use dynamic
        // parameters
        stmtContext = session.newStmtContext(null);
        SqlDialect dialect = new SqlDialect(session.getDatabaseMetaData());
        writer = new SqlPrettyWriter(dialect);
        tableName = 
            FarragoCatalogUtil.getQualifiedName(table);
        repos = session.getRepos();

        // Cast abstract catalog objects to required types
        List<FemAbstractColumn> femColumnList = 
            new ArrayList<FemAbstractColumn>();
        try {
            femTable = (FemAbstractColumnSet) table;
            for (CwmColumn column : columnList) {
                femColumnList.add((FemAbstractColumn) column);
            }
        } catch (ClassCastException e) {
            throw FarragoResource.instance().ValidatorAnalyzeSupport.ex(
                femTable.getName());
        }

        // Update statistics
        updateRowCount();
        for (FemAbstractColumn column : femColumnList) {
            updateColumnStats(column);
        }
        
        // Update index page counts
        Collection indexes = 
            FarragoCatalogUtil.getTableIndexes(repos, table);
        for (Object o : indexes) {
            FemLocalIndex index = (FemLocalIndex) o;
            ddlValidator.getIndexMap().computeIndexStats(
                ddlValidator.getDataWrapperCache(),
                index,
                estimate);
        }
    }
    
    private void updateRowCount()
    throws SQLException
    {
        String sql = getRowCountQuery();
        stmtContext.prepare(sql, true);
        checkRowCountQuery();
        
        stmtContext.execute();
        ResultSet resultSet = stmtContext.getResultSet();
        boolean gotRow = resultSet.next();
        assert(gotRow);
        long rowCount = resultSet.getLong(1);
        resultSet.close();

        FarragoCatalogUtil.updateRowCount(femTable, rowCount);
    }
    
    private String getRowCountQuery()
    {
        writer.print("select count(*) from ");
        tableName.unparse(writer, 0, 0);
        String sql = writer.toString();
        return sql;
    }
    
    private void checkRowCountQuery()
    {
        RelDataType rowType = stmtContext.getPreparedRowType();
        List fieldList = rowType.getFieldList();
        assert (fieldList.size() == 1) : "row count wrong number of columns";
        RelDataType type =
            ((RelDataTypeField) fieldList.get(0)).getType();
        assert (SqlTypeUtil.isExactNumeric(type)) : "row count invalid type";
    }
    
    private void updateColumnStats(FemAbstractColumn column)
    throws SQLException
    {
        String sql = getColumnDistributionQuery(column);
        stmtContext.prepare(sql, true);
        checkColumnDistributionQuery(stmtContext);
        
        stmtContext.execute();
        ResultSet resultSet = stmtContext.getResultSet();
        buildHistogram(column, resultSet);
        resultSet.close();
    }
    
    private String getColumnDistributionQuery(
        FemAbstractColumn column)
    {
        SqlIdentifier columnName = 
            FarragoCatalogUtil.getQualifiedName(column);
        writer.reset();
        writer.print("select ");
        columnName.unparse(writer, 0, 0);
        writer.print(", count(*) from ");
        tableName.unparse(writer, 0, 0);
        writer.print(" group by ");
        columnName.unparse(writer, 0, 0);
        writer.print(" order by ");
        columnName.unparse(writer, 0, 0);
        String sql = writer.toString();
        return sql;
    }
    
    private void checkColumnDistributionQuery(
        FarragoSessionStmtContext stmtContext)
    {
        RelDataType rowType = stmtContext.getPreparedRowType();
        List fieldList = rowType.getFieldList();
        assert (fieldList.size() == 2) 
            : "column query wrong number of columns";
        RelDataType type =
            ((RelDataTypeField) fieldList.get(1)).getType();
        assert (SqlTypeUtil.isExactNumeric(type)) 
            : "column query invalid type";
    }
    
    private void buildHistogram(
        FemAbstractColumn column, ResultSet resultSet)
    throws SQLException
    {
        long tableRowCount = femTable.getRowCount();
        int defaultBarCount = DEFAULT_HISTOGRAM_BAR_COUNT;
        
        int barCount;
        long rowsPerBar, rowsLastBar;
        if (tableRowCount <= defaultBarCount) {
            barCount = (int) tableRowCount;
            rowsPerBar = rowsLastBar = 1;
        } else {
            rowsPerBar = tableRowCount / defaultBarCount;
            if (tableRowCount % defaultBarCount != 0) {
                rowsPerBar++;
            }
            barCount = (int) (tableRowCount / rowsPerBar);
            if (tableRowCount % rowsPerBar != 0) {
                barCount++;
                rowsLastBar = (tableRowCount - (barCount-1) * rowsPerBar);
            } else {
                rowsLastBar = rowsPerBar;
            }
        }

        List<FemColumnHistogramBar> bars = 
            buildBars(resultSet, rowsPerBar);
        // for compute statistics, total cardinality is sum of bar values
        long distinctValues = 0;
        for (FemColumnHistogramBar bar : bars) {
            distinctValues += bar.getValueCount();
        }

        FarragoCatalogUtil.updateHistogram(
            repos, column, distinctValues, 100, 
            bars.size(), rowsPerBar, rowsLastBar, bars);
    }

    private List<FemColumnHistogramBar> buildBars(
        ResultSet resultSet,
        long rowsPerBar)
    throws SQLException
    {
        List<FemColumnHistogramBar> bars = 
            new LinkedList<FemColumnHistogramBar>();
        boolean newBar = true;
        String barStartValue = null;
        long barValueCount = 0;
        long barRowCount = 0;
        
        RelDataType rowType = stmtContext.getPreparedRowType();
        List fieldList = rowType.getFieldList();
        RelDataType type =
            ((RelDataTypeField) fieldList.get(0)).getType();
        while (resultSet.next()) {
            Object o = resultSet.getObject(1);
            String nextValue;
            if (o == null) {
                nextValue = null;
            } else if (o instanceof byte[]) {
                nextValue = ConversionUtil.toStringFromByteArray((byte []) o, 16);
            } else {
                nextValue = resultSet.getString(1);
            }
            long nextRows = resultSet.getLong(2);

            if (newBar) {
                barStartValue = nextValue;
                barValueCount = 0;
                barRowCount = 0;
                newBar = false;
            }
            barValueCount++;
            barRowCount += nextRows;
            
            while (barRowCount >= rowsPerBar) {
                FemColumnHistogramBar bar = 
                    buildBar(barStartValue, barValueCount);
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
            bars.add(buildBar(barStartValue, barValueCount));
        }
        return bars;
    }

    private FemColumnHistogramBar buildBar(String startValue, long valueCount)
    {
        FemColumnHistogramBar bar = 
            repos.newFemColumnHistogramBar();
        bar.setStartingValue(startValue);
        bar.setValueCount(valueCount);
        return bar;
    }
}

// End DdlAnalyzeStmt.java
