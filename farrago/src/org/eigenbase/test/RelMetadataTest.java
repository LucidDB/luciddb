/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.test;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;

import java.util.*;

/**
 * Unit test for {@link DefaultRelMetadataProvider}.  See {@link
 * SqlToRelTestBase} class comments for details on the schema used.  Note that
 * no optimizer rules are fired on the translation of the SQL into relational
 * algebra (e.g. join conditions in the WHERE clause will look like filters),
 * so it's necessary to phrase the SQL carefully.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelMetadataTest extends SqlToRelTestBase
{
    private static final double EPSILON = 1.0e-5;
    
    private static final double DEFAULT_SELECTIVITY = 0.1;

    private static final double DEFAULT_SELECTIVITY_SQUARED =
        DEFAULT_SELECTIVITY * DEFAULT_SELECTIVITY;

    private static final double EMP_SIZE = 1000.0;
    
    private static final double DEPT_SIZE = 100.0;
    
    // ----------------------------------------------------------------------
    // Tests for getPercentageOriginalRows
    // ----------------------------------------------------------------------

    private RelNode convertSql(String sql)
    {
        RelNode rel = tester.convertSqlToRel(sql);
        DefaultRelMetadataProvider provider = new DefaultRelMetadataProvider();
        rel.getCluster().setMetadataProvider(provider);
        return rel;
    }
    
    private void checkPercentageOriginalRows(String sql, double expected)
    {
        checkPercentageOriginalRows(sql, expected, EPSILON);
    }
    
    private void checkPercentageOriginalRows(
        String sql, double expected, double epsilon)
    {
        RelNode rel = convertSql(sql);
        Double result = RelMetadataQuery.getPercentageOriginalRows(rel);
        assertTrue(result != null);
        assertEquals(expected, result.doubleValue(), epsilon);
    }
    
    public void testPercentageOriginalRowsTableOnly()
    {
        checkPercentageOriginalRows(
            "select * from dept",
            1.0);
    }
    
    public void testPercentageOriginalRowsAgg()
    {
        checkPercentageOriginalRows(
            "select deptno from dept group by deptno",
            1.0);
    }
    
    public void testPercentageOriginalRowsOneFilter()
    {
        checkPercentageOriginalRows(
            "select * from dept where deptno = 20",
            DEFAULT_SELECTIVITY);
    }
    
    public void testPercentageOriginalRowsTwoFilters()
    {
        checkPercentageOriginalRows(
            "select * from (select * from dept where name='X')"
            + " where deptno = 20",
            DEFAULT_SELECTIVITY_SQUARED);
    }

    // TODO jvs 28-Mar-2006:  enable this one when Broadbase
    // selectivity formula with redundancy detection gets ported
    public void _testPercentageOriginalRowsRedundantFilter()
    {
        checkPercentageOriginalRows(
            "select * from (select * from dept where deptno=20)"
            + " where deptno = 20",
            DEFAULT_SELECTIVITY);
    }
    
    public void testPercentageOriginalRowsJoin()
    {
        checkPercentageOriginalRows(
            "select * from emp inner join dept on emp.deptno=dept.deptno",
            1.0);
    }
    
    public void testPercentageOriginalRowsJoinTwoFilters()
    {
        checkPercentageOriginalRows(
            "select * from (select * from emp where deptno=10) e"
            + " inner join (select * from dept where deptno=10) d"
            + " on e.deptno=d.deptno",
            DEFAULT_SELECTIVITY_SQUARED);
    }
    
    public void testPercentageOriginalRowsUnionNoFilter()
    {
        checkPercentageOriginalRows(
            "select name from dept union all select ename from emp",
            1.0);
    }
    
    public void testPercentageOriginalRowsUnionLittleFilter()
    {
        checkPercentageOriginalRows(
            "select name from dept where deptno=20"
            + " union all select ename from emp",
            (DEPT_SIZE*DEFAULT_SELECTIVITY + EMP_SIZE)
            / (DEPT_SIZE + EMP_SIZE));
    }
    
    public void testPercentageOriginalRowsUnionBigFilter()
    {
        checkPercentageOriginalRows(
            "select name from dept"
            + " union all select ename from emp where deptno=20",
            (EMP_SIZE*DEFAULT_SELECTIVITY + DEPT_SIZE)
            / (DEPT_SIZE + EMP_SIZE));
    }

    // ----------------------------------------------------------------------
    // Tests for getColumnOrigins
    // ----------------------------------------------------------------------

    private Set<RelColumnOrigin> checkColumnOrigin(String sql)
    {
        RelNode rel = convertSql(sql);
        return RelMetadataQuery.getColumnOrigins(rel, 0);
    }
    
    private void checkNoColumnOrigin(String sql)
    {
        Set<RelColumnOrigin> result = checkColumnOrigin(sql);
        assertTrue(result != null);
        assertTrue(result.isEmpty());
    }

    public static void checkColumnOrigin(
        RelColumnOrigin rco,
        String expectedTableName,
        String expectedColumnName,
        boolean expectedDerived)
    {
        RelOptTable actualTable = rco.getOriginTable();
        String [] actualTableName = actualTable.getQualifiedName();
        assertEquals(
            actualTableName[actualTableName.length - 1],
            expectedTableName);
        assertEquals(
            actualTable.getRowType().getFields()[
                rco.getOriginColumnOrdinal()].getName(),
            expectedColumnName);
        assertEquals(
            rco.isDerived(),
            expectedDerived);
    }
    
    private void checkSingleColumnOrigin(
        String sql,
        String expectedTableName,
        String expectedColumnName,
        boolean expectedDerived)
    {
        Set<RelColumnOrigin> result = checkColumnOrigin(sql);
        assertTrue(result != null);
        assertEquals(1, result.size());
        RelColumnOrigin rco = result.iterator().next();
        checkColumnOrigin(
            rco,
            expectedTableName,
            expectedColumnName,
            expectedDerived);
    }

    // WARNING:  this requires the two table names to be different
    private void checkTwoColumnOrigin(
        String sql,
        String expectedTableName1,
        String expectedColumnName1,
        String expectedTableName2,
        String expectedColumnName2,
        boolean expectedDerived)
    {
        Set<RelColumnOrigin> result = checkColumnOrigin(sql);
        assertTrue(result != null);
        assertEquals(2, result.size());
        for (RelColumnOrigin rco : result) {
            RelOptTable actualTable = rco.getOriginTable();
            String [] actualTableName = actualTable.getQualifiedName();
            String actualUnqualifiedName =
                actualTableName[actualTableName.length - 1];
            if (actualUnqualifiedName.equals(expectedTableName1)) {
                checkColumnOrigin(
                    rco,
                    expectedTableName1,
                    expectedColumnName1,
                    expectedDerived);
            } else {
                checkColumnOrigin(
                    rco,
                    expectedTableName2,
                    expectedColumnName2,
                    expectedDerived);
            }
        }
    }
    
    public void testColumnOriginsTableOnly()
    {
        checkSingleColumnOrigin(
            "select name as dname from dept",
            "DEPT",
            "NAME",
            false);
    }
    
    public void testColumnOriginsExpression()
    {
        checkSingleColumnOrigin(
            "select upper(name) as dname from dept",
            "DEPT",
            "NAME",
            true);
    }
    
    public void testColumnOriginsDyadicExpression()
    {
        checkTwoColumnOrigin(
            "select name||ename from dept,emp",
            "DEPT",
            "NAME",
            "EMP",
            "ENAME",
            true);
    }
    
    public void testColumnOriginsConstant()
    {
        checkNoColumnOrigin(
            "select 'Minstrelsy' as dname from dept");
    }
    
    public void testColumnOriginsFilter()
    {
        checkSingleColumnOrigin(
            "select name as dname from dept where deptno=10",
            "DEPT",
            "NAME",
            false);
    }
    
    public void testColumnOriginsJoinLeft()
    {
        checkSingleColumnOrigin(
            "select ename from emp,dept",
            "EMP",
            "ENAME",
            false);
    }
    
    public void testColumnOriginsJoinRight()
    {
        checkSingleColumnOrigin(
            "select name as dname from emp,dept",
            "DEPT",
            "NAME",
            false);
    }
    
    public void testColumnOriginsJoinOuter()
    {
        checkSingleColumnOrigin(
            "select name as dname from emp left outer join dept"
            + " on emp.deptno = dept.deptno",
            "DEPT",
            "NAME",
            true);
    }
    
    public void testColumnOriginsJoinFullOuter()
    {
        checkSingleColumnOrigin(
            "select name as dname from emp full outer join dept"
            + " on emp.deptno = dept.deptno",
            "DEPT",
            "NAME",
            true);
    }
    
    public void testColumnOriginsAggKey()
    {
        checkSingleColumnOrigin(
            "select name,count(deptno) from dept group by name",
            "DEPT",
            "NAME",
            false);
    }
    
    public void testColumnOriginsAggMeasure()
    {
        checkSingleColumnOrigin(
            "select count(deptno),name from dept group by name",
            "DEPT",
            "DEPTNO",
            true);
    }
    
    public void testColumnOriginsAggCountStar()
    {
        checkNoColumnOrigin(
            "select count(*),name from dept group by name");
    }
    
    public void testColumnOriginsValues()
    {
        checkNoColumnOrigin(
            "values(1,2,3)");
    }
    
    public void testColumnOriginsUnion()
    {
        checkTwoColumnOrigin(
            "select name from dept union all select ename from emp",
            "DEPT",
            "NAME",
            "EMP",
            "ENAME",
            false);
    }
    
    public void testColumnOriginsSelfUnion()
    {
        checkSingleColumnOrigin(
            "select ename from emp union all select ename from emp",
            "EMP",
            "ENAME",
            false);
    }

    private void checkRowCount(
        String sql,
        double expected)
    {
        RelNode rel = convertSql(sql);
        Double result = RelMetadataQuery.getRowCount(rel);
        assertTrue(result != null);
        assertEquals(expected, result.doubleValue());
    }

    public void testRowCountEmp()
    {
        checkRowCount(
            "select * from emp",
            EMP_SIZE);
    }

    public void testRowCountDept()
    {
        checkRowCount(
            "select * from dept",
            DEPT_SIZE);
    }

    public void testRowCountCartesian()
    {
        checkRowCount(
            "select * from emp,dept",
            EMP_SIZE * DEPT_SIZE);
    }

    public void testRowCountJoin()
    {
        checkRowCount(
            "select * from emp inner join dept on emp.deptno = dept.deptno",
            EMP_SIZE * DEPT_SIZE * DEFAULT_SELECTIVITY);
    }

    public void testRowCountUnion()
    {
        checkRowCount(
            "select ename from emp union all select name from dept",
            EMP_SIZE + DEPT_SIZE);
    }

    public void testRowCountFilter()
    {
        checkRowCount(
            "select * from emp where ename='Mathilda'",
            EMP_SIZE * DEFAULT_SELECTIVITY);
    }
}

// End RelMetadataTest.java
