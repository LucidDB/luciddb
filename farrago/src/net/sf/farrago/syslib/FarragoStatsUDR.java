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
package net.sf.farrago.syslib;

import java.sql.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.test.*;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.stat.*;


/**
 * FarragoStatsUDR implements system procedures for manipulating Farrago
 * statistics stored in the repository. The procedures are intended to be used
 * for testing purposes, with the exception of get_row_count which may be
 * used internally.
 *
 * @author John Pham
 * @version $Id$
 */
public abstract class FarragoStatsUDR
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Sets the row count for a table
     */
    public static void set_row_count(
        String catalog,
        String schema,
        String table,
        long rowCount)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoStatsUtil.setTableRowCount(
                sess,
                catalog,
                schema,
                table,
                rowCount);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * Retrieves the row count for a table
     */
    public static long get_row_count(
        String catalogName, 
        String schemaName, 
        String tableName)
        throws SQLException
    {
        try {
            FarragoSession session = FarragoUdrRuntime.getSession();
            
            // eschew FarragoStatsUtil -- it's in the test package
            FarragoRepos repos = session.getRepos();
            if ((catalogName == null) || (catalogName.length() == 0)) {
                catalogName = session.getSessionVariables().catalogName;
            }
            CwmCatalog catalog = repos.getCatalog(catalogName);
            if (catalog == null) {
                throw FarragoResource.instance().ValidatorUnknownObject.ex(
                    catalogName);
            }

            if ((schemaName == null) || (schemaName.length() == 0)) {
                schemaName = session.getSessionVariables().schemaName;
            }
            FemLocalSchema schema =
                FarragoCatalogUtil.getSchemaByName(catalog, schemaName);
            if (schema == null) {
                throw FarragoResource.instance().ValidatorUnknownObject.ex(
                    schemaName);
            }
            
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

            Long rowcount = columnSet.getRowCount();
            if (rowcount == null) {
                return 0;
            }
            return rowcount.longValue();

        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
        
    }

    /**
     * Sets the page count for an index
     */
    public static void set_page_count(
        String catalog,
        String schema,
        String index,
        long pageCount)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoStatsUtil.setIndexPageCount(
                sess,
                catalog,
                schema,
                index,
                pageCount);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * Creates a column histogram
     */
    public static void set_column_histogram(
        String catalog,
        String schema,
        String table,
        String column,
        long distinctValues,
        int samplePercent,
        long sampleDistinctValues,
        int distributionType,
        String valueDigits)
        throws SQLException
    {
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoStatsUtil.createColumnHistogram(
                sess,
                catalog,
                schema,
                table,
                column,
                distinctValues,
                samplePercent,
                sampleDistinctValues,
                distributionType,
                valueDigits);
        } catch (Throwable e) {
            throw new SQLException(e.getMessage());
        }
    }
    
    public static double get_cardinality(
        String catalog,
        String schema, 
        String table,
        String column,
        String expression)
    throws SQLException
    {
        RelStatColumnStatistics columnStats = getColumnStats(catalog, schema, table, column, expression);
        Double cardinality = columnStats.getCardinality();
        if (cardinality != null) {
            return cardinality;
        }
        return -1.0;
    }
    
    public static Double get_selectivity(
        String catalog,
        String schema, 
        String table,
        String column,
        String expression)
    throws SQLException
    {
        RelStatColumnStatistics columnStats = getColumnStats(catalog, schema, table, column, expression);
        Double selectivity = columnStats.getSelectivity();
        if (selectivity != null) {
            return selectivity;
        }
        return -1.0;
    }
    
    private static RelStatColumnStatistics getColumnStats(
        String catalog,
        String schema, 
        String table,
        String column,
        String expression)
    throws SQLException
    {
        if (expression == null) {
            throw new SQLException("missing expression");
        }

        String[] params = expression.split(",");
        if (params.length > 2) {
            throw new SQLException("cannot use extra commas in expression");            
        }
        
        String lower = null;
        String upper = null;
        boolean lowerClosed = false;
        boolean upperClosed = false;

        if (params.length == 1) {
            String p = params[0];
            if (p.startsWith("[")) {
                lower = p.substring(1);
                lowerClosed = true;
            } else if (p.startsWith("(")) {
                lower = p.substring(1);
            } else if (p.endsWith("]")) {
                upper = p.substring(0, p.length() - 1);
                upperClosed = true;
            } else if (p.endsWith(")")) {
                upper = p.substring(0, p.length() - 1);                
            } else {
                upper = lower = p;
                upperClosed = lowerClosed = true;
            }
        } else {
            lowerClosed = params[0].startsWith("[");
            upperClosed = params[1].endsWith("]");
            
            lower = params[0].substring(1);
            upper = params[1].substring(0, params[1].length() - 1);
        }
        try {
            FarragoSession sess = FarragoUdrRuntime.getSession();
            FarragoRepos repos = sess.getRepos();
            
            RelDataTypeFactory typeFactory = 
                sess.getPersonality().newTypeFactory(repos);
            RexBuilder rexBuilder = new RexBuilder(typeFactory);
            SargFactory sargFactory = new SargFactory(rexBuilder);
            
            FemAbstractColumnSet columnSet = 
                FarragoStatsUtil.lookupColumnSet(sess, repos, catalog, schema, table);
            
            FemAbstractColumn col = 
                FarragoStatsUtil.lookupColumn(columnSet, column);
            
            String cwmTypeName = col.getType().getName();
            SqlTypeName sqlTypeName = SqlTypeName.get(cwmTypeName);
            if (sqlTypeName.getFamily() == SqlTypeFamily.NUMERIC) {
                // RexLiteral.fromJdbcString doesn't like most other numeric
                // types.
                sqlTypeName = SqlTypeName.DECIMAL;
                
                // trim the values
                if (lower != null) {
                    lower = lower.trim();
                }
                if (upper != null) {
                    upper = upper.trim();
                }
            }
            RelDataType type;
            if (col.getPrecision() != null) {
                if (col.getScale() != null) {
                    type = 
                        typeFactory.createSqlType(
                            sqlTypeName, col.getPrecision(), col.getScale());
                } else {
                    type = 
                        typeFactory.createSqlType(
                            sqlTypeName, col.getPrecision());
                }
            } else {
                type = typeFactory.createSqlType(sqlTypeName);
            }
            
            SargIntervalExpr expr = sargFactory.newIntervalExpr(type);
            if (lower != null) {
                RexLiteral lowerLiteral = 
                    RexLiteral.fromJdbcString(type, sqlTypeName, lower); 

                expr.setLower(
                    lowerLiteral, 
                    lowerClosed ? SargStrictness.CLOSED : SargStrictness.OPEN);
            }
            if (upper != null) {
                RexLiteral upperLiteral = 
                    RexLiteral.fromJdbcString(type, sqlTypeName, upper); 

                expr.setUpper(
                    upperLiteral, 
                    upperClosed ? SargStrictness.CLOSED : SargStrictness.OPEN);
            }
            
            FarragoTableStatistics tableStats = 
                new FarragoTableStatistics(repos, columnSet);
            RelStatColumnStatistics columnStats = 
                tableStats.getColumnStatistics(
                    col.getOrdinal(), expr.evaluate());

            return columnStats;
        } catch(Throwable t) {
            throw new SQLException(t.getMessage());
        }
    }
}

// End FarragoStatsUDR.java
