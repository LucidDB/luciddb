/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

import com.disruptivetech.farrago.volcano.VolcanoPlanner;
import junit.framework.TestCase;
import openjava.mop.OJSystem;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Unit test for {@link SqlToRelConverter}.
 *
 * @author jhyde
 * @version $Id$
 */
public class SqlToRelConverterTest extends TestCase
{
    protected static final String NL = System.getProperty("line.separator");

    protected void check(
        String sql,
        String plan)
    {
        final SqlNode sqlQuery;
        try {
            sqlQuery = parseQuery(sql);
        } catch (Exception e) {
            throw Util.newInternal(e); // todo: better handling
        }
        final RelDataTypeFactory typeFactory =
            new SqlTypeFactoryImpl();

        final SqlValidator.CatalogReader catalogReader =
            createCatalogReader(typeFactory);
        final SqlValidator validator =
            createValidator(catalogReader, typeFactory);
        final RelOptSchema relOptSchema =
            new MockRelOptSchema(catalogReader, typeFactory);
        final RelOptConnection relOptConnection = new RelOptConnection()
        {
            public RelOptSchema getRelOptSchema()
            {
                return relOptSchema;
            }

            public Object contentsAsArray(
                String qualifier,
                String tableName)
            {
                return null;
            }
        };
        final SqlToRelConverter converter =
            new SqlToRelConverter(
                validator,
                relOptSchema,
                OJSystem.env,
                new VolcanoPlanner(),
                relOptConnection,
                new JavaRexBuilder(typeFactory));
        final RelNode rel = converter.convertQuery(sqlQuery);
        assertTrue(rel != null);
        final StringWriter sw = new StringWriter();
        final RelOptPlanWriter planWriter =
            new RelOptPlanWriter(new PrintWriter(sw));
        planWriter.withIdPrefix = false;
        rel.explain(planWriter);
        planWriter.flush();
        String actual = sw.toString();
        Util.assertEqualsVerbose(plan, actual);
    }

    protected SqlNode parseQuery(String sql) throws Exception {
        SqlParser parser = new SqlParser(sql);
        SqlNode sqlNode = parser.parseQuery();
        return sqlNode;
    }

    /**
     * Factory method for a {@link SqlValidator}.
     */
    protected SqlValidator createValidator(
        SqlValidator.CatalogReader catalogReader,
        RelDataTypeFactory typeFactory)
    {
        return new SqlValidator(
            SqlStdOperatorTable.instance(),
            new MockCatalogReader(typeFactory),
            typeFactory);
    }

    /**
     * Factory method for a {@link SqlValidator.CatalogReader}.
     */
    protected SqlValidator.CatalogReader createCatalogReader(
        RelDataTypeFactory typeFactory)
    {
        return new MockCatalogReader(typeFactory);
    }

    private class MockRelOptSchema implements RelOptSchema {
        private final SqlValidator.CatalogReader catalogReader;
        private final RelDataTypeFactory typeFactory;

        public MockRelOptSchema(
            SqlValidator.CatalogReader catalogReader,
            RelDataTypeFactory typeFactory)
        {
            this.catalogReader = catalogReader;
            this.typeFactory = typeFactory;
        }

        public RelOptTable getTableForMember(String[] names)
        {
            final SqlValidator.Table table = catalogReader.getTable(names);
            final RelDataType rowType = table.getRowType();
            return new MockColumnSet(
                names,
                rowType);
        }

        public RelDataTypeFactory getTypeFactory()
        {
            return typeFactory;
        }

        public void registerRules(RelOptPlanner planner)
            throws Exception
        {
        }

        class MockColumnSet implements RelOptTable {
            private final String[] names;
            private final RelDataType rowType;

            MockColumnSet(String[] names, RelDataType rowType)
            {
                this.names = names;
                this.rowType = rowType;
            }

            public String[] getQualifiedName() {
                return names;
            }

            public double getRowCount() {
                return 0;
            }

            public RelDataType getRowType() {
                return rowType;
            }

            public RelOptSchema getRelOptSchema() {
                return MockRelOptSchema.this;
            }

            public RelNode toRel(RelOptCluster cluster, RelOptConnection connection) {
                return new TableAccessRel(cluster, this, connection);
            }
        }
    }

    //~ TESTS --------------------------------

    public void testIntegerLiteral()
    {
        check("select 1 from emp",
            "ProjectRel(EXPR$0=[1])" + NL +
            "  TableAccessRel(table=[[EMP]])" + NL);
    }

    public void testGroup()
    {
        check("select deptno from emp group by deptno",
            "ProjectRel(DEPTNO=[$0])" + NL +
            "  AggregateRel(groupCount=[1])" + NL +
            "    ProjectRel(field#0=[$7])" + NL +
            "      TableAccessRel(table=[[EMP]])" + NL);

        // just one agg
        check("select deptno, sum(sal) from emp group by deptno",
            "ProjectRel(DEPTNO=[$0], EXPR$1=[$0])" + NL +
            "  AggregateRel(groupCount=[1], agg#0=[SUM(1)])" + NL +
            "    ProjectRel(field#0=[$7], field#1=[$5])" + NL +
            "      TableAccessRel(table=[[EMP]])" + NL);

        // expressions inside and outside aggs
        check("select deptno + 4, sum(sal), sum(3 + sal), 2 * sum(sal) from emp group by deptno",
            "ProjectRel(EXPR$0=[+($0, 4)], EXPR$1=[$0], EXPR$2=[$1], EXPR$3=[*(2, $2)])" + NL +
            "  AggregateRel(groupCount=[1], agg#0=[SUM(1)], agg#1=[SUM(2)], agg#2=[SUM(3)])" + NL +
            "    ProjectRel(field#0=[$7], field#1=[$5], field#2=[+(3, $5)], field#3=[$5])" + NL +
            "      TableAccessRel(table=[[EMP]])" + NL);

        // empty group-by clause, having
        check("select sum(sal + sal) from emp having sum(sal) > 10",
            "FilterRel(condition=[>($1, 10)])" + NL +
            "  ProjectRel(EXPR$0=[$0])" + NL +
            "    AggregateRel(groupCount=[0], agg#0=[SUM(0)], agg#1=[SUM(1)])" + NL +
            "      ProjectRel(field#0=[+($5, $5)], field#1=[$5])" + NL +
            "        TableAccessRel(table=[[EMP]])" + NL);
    }

    public void testGroupBug281() {
        // Dtbug 281 gives:
        //   Internal error:
        //   Type 'RecordType(VARCHAR(128) $f0)' has no field 'NAME'
        if(false) check("select name from (select name from dept group by name)",
            "ProjectRel(NAME=[$0])" + NL +
            "  ProjectRel(NAME=[$0])" + NL +
            "    AggregateRel(groupCount=[1])" + NL +
            "      ProjectRel(field#0=[$1])" + NL +
            "        TableAccessRel(table=[[DEPT]])" + NL);

        // Try to confuse it with spurious columns.
        check("select name, foo from (" +
            "select deptno, name, count(deptno) as foo " +
            "from dept " +
            "group by name, deptno, name)",
            "ProjectRel(NAME=[$1], FOO=[$2])" + NL +
            "  ProjectRel(DEPTNO=[$1], NAME=[$0], FOO=[$0])" + NL +
            "    AggregateRel(groupCount=[3], agg#0=[COUNT(3)])" + NL +
            "      ProjectRel(field#0=[$1], field#1=[$0], field#2=[$1], field#3=[$0])" + NL +
            "        TableAccessRel(table=[[DEPT]])" + NL);
    }

    public void testUnnest() {
        check("select*from unnest(multiset[1,2])",
            "ProjectRel(EXPR$0=[$0])" + NL +
            "  UncollectRel" + NL +
            "    CollectRel" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EXPR$0=[1])" + NL +
            "          OneRowRel" + NL +
            "        ProjectRel(EXPR$0=[2])" + NL +
            "          OneRowRel" + NL);

        check("select*from unnest(multiset(select*from dept))",
            "ProjectRel(DEPTNO=[$0], NAME=[$1])" + NL +
            "  UncollectRel" + NL +
            "    CollectRel" + NL +
            "      ProjectRel(DEPTNO=[$0], NAME=[$1])" + NL +
            "        TableAccessRel(table=[[DEPT]])" + NL);

    }

    public void testMultiset() {
        check("select multiset(select deptno from dept) from values(true)",
            "ProjectRel(EXPR$0=[$1])" + NL +
            "  CorrelatorRel(condition=[true], joinType=[left])" + NL +
            "    ProjectRel(EXPR$0=[$0])" + NL +
            "      ProjectRel(EXPR$0=[true])" + NL +
            "        OneRowRel" + NL +
            "    CollectRel" + NL +
            "      ProjectRel(DEPTNO=[$0])" + NL +
            "        TableAccessRel(table=[[DEPT]])" + NL);

        check("select 'a',multiset[10] from dept",
            "ProjectRel(EXPR$0=[_ISO-8859-1'a'], EXPR$1=[$2])" + NL +
            "  CorrelatorRel(condition=[true], joinType=[left])" + NL +
            "    TableAccessRel(table=[[DEPT]])" + NL +
            "    CollectRel" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EXPR$0=[10])" + NL +
            "          OneRowRel" + NL);

        check("select 'abc',multiset[deptno,sal] from emp",
            "ProjectRel(EXPR$0=[_ISO-8859-1'abc'], EXPR$1=[$8])" + NL +
            "  CorrelatorRel(condition=[true], joinType=[left])" + NL +
            "    TableAccessRel(table=[[EMP]])" + NL +
            "    CollectRel" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(DEPTNO=[$cor0.DEPTNO])" + NL +
            "          OneRowRel" + NL +
            "        ProjectRel(SAL=[$cor1.SAL])" + NL +
            "          OneRowRel" + NL);
    }

    public void testCorrelationJoin() {
        check("select *," +
            "         multiset(select * from emp where deptno=dept.deptno) " +
            "               as empset" +
            "      from dept",

            "ProjectRel(DEPTNO=[$0], NAME=[$1], EMPSET=[$2])" + NL +
            "  CorrelatorRel(condition=[true], joinType=[left])" + NL +
            "    TableAccessRel(table=[[DEPT]])" + NL +
            "    CollectRel" + NL +
            "      ProjectRel(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])" + NL +
            "        FilterRel(condition=[=($7, $cor0.DEPTNO)])" + NL +
            "          TableAccessRel(table=[[EMP]])" + NL);
    }

    public void testUnnestSelect() {
        check("select*from unnest(select multiset[deptno] from dept)",
            "ProjectRel(EXPR$0=[$0])" + NL +
            "  UncollectRel" + NL +
            "    ProjectRel(EXPR$0=[$2])" + NL +
            "      CorrelatorRel(condition=[true], joinType=[left])" + NL +
            "        TableAccessRel(table=[[DEPT]])" + NL +
            "        CollectRel" + NL +
            "          UnionRel(all=[true])" + NL +
            "            ProjectRel(DEPTNO=[$cor0.DEPTNO])" + NL +
            "              OneRowRel" + NL);
    }
}

// End ConverterTest.java
