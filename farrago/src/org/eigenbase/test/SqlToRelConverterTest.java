/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import junit.framework.TestCase;
import openjava.mop.OJSystem;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRel;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.fun.SqlCaseOperator;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.*;

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
    protected final Tester tester = createTester();

    protected Tester createTester()
    {
        return new TesterImpl();
    }

    protected void check(
        String sql,
        String plan)
    {
        final RelNode rel = tester.convertSqlToRel(sql);

        assertTrue(rel != null);
        final StringWriter sw = new StringWriter();
        final RelOptPlanWriter planWriter =
            new RelOptPlanWriter(new PrintWriter(sw));
        planWriter.setIdPrefix(false);
        rel.explain(planWriter);
        planWriter.flush();
        String actual = sw.toString();
        TestUtil.assertEqualsVerbose(plan, actual);
    }

    /**
     * Mock implementation of {@link RelOptSchema}.
     */
    private static class MockRelOptSchema implements RelOptSchema {
        private final SqlValidatorCatalogReader catalogReader;
        private final RelDataTypeFactory typeFactory;

        public MockRelOptSchema(
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory)
        {
            this.catalogReader = catalogReader;
            this.typeFactory = typeFactory;
        }

        public RelOptTable getTableForMember(String[] names)
        {
            final SqlValidatorTable table = catalogReader.getTable(names);
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

    /**
     * Mock implementation of {@link RelOptConnection}, contains a
     * {@link MockRelOptSchema}.
     */
    private static class MockRelOptConnection implements RelOptConnection
    {
        private final RelOptSchema relOptSchema;

        public MockRelOptConnection(RelOptSchema relOptSchema)
        {
            this.relOptSchema = relOptSchema;
        }

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
    }

    /**
     * Helper class which contains default implementations of methods used
     * for running sql-to-rel conversion tests.
     */
    public static interface Tester
    {
        /**
         * Converts a SQL string to a {@link RelNode} tree.
         *
         * @param sql SQL statement
         * @return Relational expression, never null
         *
         * @pre sql != null
         * @post return != null
         */
        RelNode convertSqlToRel(String sql);

        SqlNode parseQuery(String sql) throws Exception;

        /**
         * Factory method to create a {@link SqlValidator}.
         */
        SqlValidator createValidator(
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory);

        /**
         * Factory method for a {@link SqlValidatorCatalogReader}.
         */
        SqlValidatorCatalogReader createCatalogReader(
            RelDataTypeFactory typeFactory);

        RelOptPlanner createPlanner();

        /**
         * Returns the {@link SqlOperatorTable} to use.
         */
        SqlOperatorTable getOperatorTable();
    };

    /**
     * Default implementation of {@link Tester}, using mock classes
     * {@link MockRelOptSchema}, {@link MockRelOptConnection} and
     * {@link MockRelOptPlanner}.
     */
    public static class TesterImpl implements Tester
    {
        private RelOptPlanner planner;

        protected TesterImpl()
        {
        }

        public RelNode convertSqlToRel(String sql)
        {
            Util.pre(sql != null, "sql != null");
            final SqlNode sqlQuery;
            try {
                sqlQuery = parseQuery(sql);
            } catch (Exception e) {
                throw Util.newInternal(e); // todo: better handling
            }
            final RelDataTypeFactory typeFactory = createTypeFactory();
            final SqlValidatorCatalogReader catalogReader =
                createCatalogReader(typeFactory);
            final SqlValidator validator =
                createValidator(catalogReader, typeFactory);
            final RelOptSchema relOptSchema =
                new MockRelOptSchema(catalogReader, typeFactory);
            final RelOptConnection relOptConnection =
                new MockRelOptConnection(relOptSchema);
            final SqlToRelConverter converter =
                new SqlToRelConverter(
                    validator,
                    relOptSchema,
                    OJSystem.env,
                    getPlanner(),
                    relOptConnection,
                    new JavaRexBuilder(typeFactory));
            final RelNode rel = converter.convertQuery(sqlQuery);
            Util.post(rel != null, "return != null");
            return rel;
        }

        protected RelDataTypeFactory createTypeFactory()
        {
            return new SqlTypeFactoryImpl();
        }

        protected final RelOptPlanner getPlanner()
        {
            if (planner == null) {
                planner = createPlanner();
            }
            return planner;
        }

        public SqlNode parseQuery(String sql) throws Exception {
            SqlParser parser = new SqlParser(sql);
            SqlNode sqlNode = parser.parseQuery();
            return sqlNode;
        }

        public SqlValidator createValidator(
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory)
        {
            return new FarragoTestValidator(
                getOperatorTable(),
                new MockCatalogReader(typeFactory),
                typeFactory);
        }

        public SqlOperatorTable getOperatorTable()
        {
            return SqlStdOperatorTable.instance();
        }

        public SqlValidatorCatalogReader createCatalogReader(
            RelDataTypeFactory typeFactory)
        {
            return new MockCatalogReader(typeFactory);
        }

        public RelOptPlanner createPlanner()
        {
            return new MockRelOptPlanner();
        }
    }

    private static class FarragoTestValidator extends SqlValidatorImpl
    {
        public FarragoTestValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory)
        {
            super(opTab, catalogReader, typeFactory);
        }

        // override SqlValidator
        protected boolean shouldExpandIdentifiers()
        {
            return true;
        }

    }

    //~ TESTS --------------------------------

    public void testIntegerLiteral()
    {
        check("select 1 from emp",
            "ProjectRel(EXPR$0=[1])" + NL +
            "  TableAccessRel(table=[[SALES, EMP]])" + NL);
    }

    public void testGroup()
    {
        check("select deptno from emp group by deptno",
            "ProjectRel(DEPTNO=[$0])" + NL +
            "  AggregateRel(groupCount=[1])" + NL +
            "    ProjectRel(field#0=[$7])" + NL +
            "      TableAccessRel(table=[[SALES, EMP]])" + NL);

        // just one agg
        check("select deptno, sum(sal) from emp group by deptno",
            "ProjectRel(DEPTNO=[$0], EXPR$1=[$1])" + NL +
            "  AggregateRel(groupCount=[1], agg#0=[SUM(1)])" + NL +
            "    ProjectRel(field#0=[$7], field#1=[$5])" + NL +
            "      TableAccessRel(table=[[SALES, EMP]])" + NL);

        // expressions inside and outside aggs
        check("select deptno + 4, sum(sal), sum(3 + sal), 2 * sum(sal) from emp group by deptno",
            "ProjectRel(EXPR$0=[+($0, 4)], EXPR$1=[$1], EXPR$2=[$2], EXPR$3=[*(2, $3)])" + NL +
            "  AggregateRel(groupCount=[1], agg#0=[SUM(1)], agg#1=[SUM(2)], agg#2=[SUM(3)])" + NL +
            "    ProjectRel(field#0=[$7], field#1=[$5], field#2=[+(3, $5)], field#3=[$5])" + NL +
            "      TableAccessRel(table=[[SALES, EMP]])" + NL);

        // empty group-by clause, having
        check("select sum(sal + sal) from emp having sum(sal) > 10",
            "FilterRel(condition=[>($1, 10)])" + NL +
            "  ProjectRel(EXPR$0=[$0])" + NL +
            "    AggregateRel(groupCount=[0], agg#0=[SUM(0)], agg#1=[SUM(1)])" + NL +
            "      ProjectRel(field#0=[+($5, $5)], field#1=[$5])" + NL +
            "        TableAccessRel(table=[[SALES, EMP]])" + NL);
    }

    public void testGroupBug281() {
        // Dtbug 281 gives:
        //   Internal error:
        //   Type 'RecordType(VARCHAR(128) $f0)' has no field 'NAME'
        check("select name from (select name from dept group by name)",
            "ProjectRel(NAME=[$0])" + NL +
            "  ProjectRel(NAME=[$0])" + NL +
            "    AggregateRel(groupCount=[1])" + NL +
            "      ProjectRel(field#0=[$1])" + NL +
            "        TableAccessRel(table=[[SALES, DEPT]])" + NL);

        // Try to confuse it with spurious columns.
        check("select name, foo from (" +
            "select deptno, name, count(deptno) as foo " +
            "from dept " +
            "group by name, deptno, name)",
            "ProjectRel(NAME=[$1], FOO=[$2])" + NL +
            "  ProjectRel(DEPTNO=[$1], NAME=[$0], FOO=[$3])" + NL +
            "    AggregateRel(groupCount=[3], agg#0=[COUNT(3)])" + NL +
            "      ProjectRel(field#0=[$1], field#1=[$0], field#2=[$1], field#3=[$0])" + NL +
            "        TableAccessRel(table=[[SALES, DEPT]])" + NL);
    }

    public void testUnnest() {
        check("select*from unnest(multiset[1,2])",
                "ProjectRel(EXPR$0=[$0])" + NL +
                "  UncollectRel" + NL +
                "    ProjectRel(EXPR$0=[$0])" + NL +
                "      CollectRel" + NL +
                "        UnionRel(all=[true])" + NL +
                "          ProjectRel(EXPR$0=[1])" + NL +
                "            OneRowRel" + NL +
                "          ProjectRel(EXPR$0=[2])" + NL +
                "            OneRowRel" + NL);

        check("select*from unnest(multiset(select*from dept))",
                "ProjectRel(DEPTNO=[$0], NAME=[$1])" + NL +
                "  UncollectRel" + NL +
                "    ProjectRel(EXPR$0=[$0])" + NL +
                "      CollectRel" + NL +
                "        ProjectRel(DEPTNO=[$0], NAME=[$1])" + NL +
                "          TableAccessRel(table=[[SALES, DEPT]])" + NL);

    }

    public void testMultiset() {
        check("select multiset(select deptno from dept) from (values(true))",
            "ProjectRel(EXPR$0=[$1])" + NL +
            "  JoinRel(condition=[true], joinType=[left])" + NL +
            "    ProjectRel(EXPR$0=[$0])" + NL +
            "      ProjectRel(EXPR$0=[true])" + NL +
            "        OneRowRel" + NL +
            "    CollectRel" + NL +
            "      ProjectRel(DEPTNO=[$0])" + NL +
            "        TableAccessRel(table=[[SALES, DEPT]])" + NL);

        check("select 'a',multiset[10] from dept",
            "ProjectRel(EXPR$0=[_ISO-8859-1'a'], EXPR$1=[$2])" + NL +
            "  JoinRel(condition=[true], joinType=[left])" + NL +
            "    TableAccessRel(table=[[SALES, DEPT]])" + NL +
            "    CollectRel" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EXPR$0=[10])" + NL +
            "          OneRowRel" + NL);

        check("select 'abc',multiset[deptno,sal] from emp",
            "ProjectRel(EXPR$0=[_ISO-8859-1'abc'], EXPR$1=[$8])" + NL +
            "  CorrelatorRel(condition=[true], joinType=[left], correlations=[[var0=offset7, var1=offset5]])" + NL +
            "    TableAccessRel(table=[[SALES, EMP]])" + NL +
            "    CollectRel" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EXPR$0=[$cor0.DEPTNO])" + NL +
            "          OneRowRel" + NL +
            "        ProjectRel(EXPR$0=[$cor1.SAL])" + NL +
            "          OneRowRel" + NL);
    }

    public void testCorrelationJoin() {
        check("select *," +
            "         multiset(select * from emp where deptno=dept.deptno) " +
            "               as empset" +
            "      from dept",

            "ProjectRel(DEPTNO=[$0], NAME=[$1], EMPSET=[$2])" + NL +
            "  CorrelatorRel(condition=[true], joinType=[left], correlations=[[var0=offset0]])" + NL +
            "    TableAccessRel(table=[[SALES, DEPT]])" + NL +
            "    CollectRel" + NL +
            "      ProjectRel(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])" + NL +
            "        FilterRel(condition=[=($7, $cor0.DEPTNO)])" + NL +
            "          TableAccessRel(table=[[SALES, EMP]])" + NL);
    }

    public void testExists() {
        check("select*from emp where exists (select 1 from dept where deptno=55)",
            "ProjectRel(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])" + NL +
            "  FilterRel(condition=[$9])" + NL +
            "    JoinRel(condition=[true], joinType=[left])" + NL +
            "      TableAccessRel(table=[[SALES, EMP]])" + NL +
            "      ProjectRel(EXPR$0=[$0], $indicator=[true])" + NL +
            "        ProjectRel(EXPR$0=[1])" + NL +
            "          FilterRel(condition=[=($0, 55)])" + NL +
            "            TableAccessRel(table=[[SALES, DEPT]])" + NL);

        check("select*from emp where exists (select 1 from dept where emp.deptno=dept.deptno)",
            "ProjectRel(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])" + NL +
            "  FilterRel(condition=[$9])" + NL +
            "    CorrelatorRel(condition=[true], joinType=[left], correlations=[[var0=offset7]])" + NL +
            "      TableAccessRel(table=[[SALES, EMP]])" + NL +
            "      ProjectRel(EXPR$0=[$0], $indicator=[true])" + NL +
            "        ProjectRel(EXPR$0=[1])" + NL +
            "          FilterRel(condition=[=($cor0.DEPTNO, $0)])" + NL +
            "            TableAccessRel(table=[[SALES, DEPT]])" + NL);
    }

    public void testUnnestSelect() {
        check("select*from unnest(select multiset[deptno] from dept)",
            "ProjectRel(EXPR$0=[$0])" + NL +
            "  UncollectRel" + NL +
            "    ProjectRel(EXPR$0=[$0])" + NL +
            "      ProjectRel(EXPR$0=[$2])" + NL +
            "        CorrelatorRel(condition=[true], joinType=[left], correlations=[[var0=offset0]])" + NL +
            "          TableAccessRel(table=[[SALES, DEPT]])" + NL +
            "          CollectRel" + NL +
            "            UnionRel(all=[true])" + NL +
            "              ProjectRel(EXPR$0=[$cor0.DEPTNO])" + NL +
            "                OneRowRel" + NL);
    }

    public void testLateral() {
        check("select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno)",
            "ProjectRel(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[$8], NAME=[$9])" + NL +
            "  CorrelatorRel(condition=[true], joinType=[left], correlations=[[var0=offset7]])" + NL +
            "    TableAccessRel(table=[[SALES, EMP]])" + NL +
            "    ProjectRel(DEPTNO=[$0], NAME=[$1])" + NL +
            "      FilterRel(condition=[=($cor0.DEPTNO, $0)])" + NL +
            "        TableAccessRel(table=[[SALES, DEPT]])" + NL);
    }

    public void testElement() {
        check("select element(multiset[5]) from emp",
            "ProjectRel(EXPR$0=[ELEMENT($8)])" + NL +
            "  JoinRel(condition=[true], joinType=[left])" + NL +
            "    TableAccessRel(table=[[SALES, EMP]])" + NL +
            "    CollectRel" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EXPR$0=[5])" + NL +
            "          OneRowRel" + NL);
        check("values element(multiset[5])",
            "ProjectRel(EXPR$0=[$0])" + NL +
            "  ProjectRel(EXPR$0=[ELEMENT($0)])" + NL +
            "    CollectRel" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EXPR$0=[5])" + NL +
            "          OneRowRel" + NL);
    }

    public void testUnion() {
        // union all
        check( "select empno from emp union all select deptno from dept",
            "ProjectRel(EMPNO=[$0])" + NL +
            "  UnionRel(all=[true])" + NL +
            "    ProjectRel(EMPNO=[$0])" + NL +
            "      TableAccessRel(table=[[SALES, EMP]])" + NL +
            "    ProjectRel(DEPTNO=[$0])" + NL +
            "      TableAccessRel(table=[[SALES, DEPT]])" + NL);

        // union without all
        check("select empno from emp union select deptno from dept",
            "ProjectRel(EMPNO=[$0])" + NL +
            "  UnionRel(all=[false])" + NL +
            "    ProjectRel(EMPNO=[$0])" + NL +
            "      TableAccessRel(table=[[SALES, EMP]])" + NL +
            "    ProjectRel(DEPTNO=[$0])" + NL +
            "      TableAccessRel(table=[[SALES, DEPT]])" + NL);

        // union with values
        check("values (10), (20)" + NL +
            "union all" + NL +
            "select 34 from emp" + NL +
            "union all values (30), (45 + 10)",
            "ProjectRel(EXPR$0=[$0])" + NL +
            "  UnionRel(all=[true])" + NL +
            "    ProjectRel(EXPR$0=[$0])" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EXPR$0=[$0])" + NL +
            "          UnionRel(all=[true])" + NL +
            "            ProjectRel(EXPR$0=[10])" + NL +
            "              OneRowRel" + NL +
            "            ProjectRel(EXPR$0=[20])" + NL +
            "              OneRowRel" + NL +
            "        ProjectRel(EXPR$0=[34])" + NL +
            "          TableAccessRel(table=[[SALES, EMP]])" + NL +
            "    ProjectRel(EXPR$0=[$0])" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EXPR$0=[30])" + NL +
            "          OneRowRel" + NL +
            "        ProjectRel(EXPR$0=[+(45, 10)])" + NL +
            "          OneRowRel" + NL);

        // union of subquery, inside from list, also values
        check("select deptno from emp as emp0 cross join" + NL +
            " (select empno from emp union all " + NL +
            "  select deptno from dept where deptno > 20 union all" + NL +
            "  values (45), (67))",
            "ProjectRel(DEPTNO=[$7])" + NL +
            "  JoinRel(condition=[true], joinType=[inner])" + NL +
            "    TableAccessRel(table=[[SALES, EMP]])" + NL +
            "    ProjectRel(EMPNO=[$0])" + NL +
            "      UnionRel(all=[true])" + NL +
            "        ProjectRel(EMPNO=[$0])" + NL +
            "          UnionRel(all=[true])" + NL +
            "            ProjectRel(EMPNO=[$0])" + NL +
            "              TableAccessRel(table=[[SALES, EMP]])" + NL +
            "            ProjectRel(DEPTNO=[$0])" + NL +
            "              FilterRel(condition=[>($0, 20)])" + NL +
            "                TableAccessRel(table=[[SALES, DEPT]])" + NL +
            "        ProjectRel(EXPR$0=[$0])" + NL +
            "          UnionRel(all=[true])" + NL +
            "            ProjectRel(EXPR$0=[45])" + NL +
            "              OneRowRel" + NL +
            "            ProjectRel(EXPR$0=[67])" + NL +
            "              OneRowRel" + NL);

    }

    public void testIsDistinctFrom() {
        check("select 1 is distinct from 2 from (values(true))",
            "ProjectRel(EXPR$0=[CASE(IS NULL(1), IS NOT NULL(2), IS NULL(2), IS NOT NULL(1), <>(1, 2))])" + NL +
            "  ProjectRel(EXPR$0=[$0])" + NL +
            "    ProjectRel(EXPR$0=[true])" + NL +
            "      OneRowRel" + NL);

        check("select 1 is not distinct from 2 from (values(true))",
            "ProjectRel(EXPR$0=[CASE(IS NULL(1), IS NULL(2), IS NULL(2), IS NULL(1), =(1, 2))])" + NL +
            "  ProjectRel(EXPR$0=[$0])" + NL +
            "    ProjectRel(EXPR$0=[true])" + NL +
            "      OneRowRel" + NL);
    }

    public void testNotLike() {
        // note that 'x not like y' becomes 'not(x like y)'
        check("values ('a' not like 'b' escape 'c')",
            "ProjectRel(EXPR$0=[$0])" + NL +
            "  ProjectRel(EXPR$0=[NOT(LIKE(_ISO-8859-1'a', _ISO-8859-1'b', _ISO-8859-1'c'))])" + NL +
            "    OneRowRel" + NL);
    }

    public void testOverMultiple() {
        check(
            "select sum(sal) over w1," + NL +
            "  sum(deptno) over w1," + NL +
            "  sum(deptno) over w2" + NL +
            "from emp" + NL +
            "where sum(deptno - sal) over w1 > 999" + NL +
            "window w1 as (partition by job order by hiredate rows 2 preceding)," + NL +
            "  w2 as (partition by job order by hiredate rows 3 preceding)," + NL +
            "  w3 as (partition by job order by hiredate range interval '1' second preceding)",

            "ProjectRel(EXPR$0=[SUM($5) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING)], EXPR$1=[SUM($7) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING)], EXPR$2=[SUM($7) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 3 PRECEDING)])" + NL +
            "  FilterRel(condition=[>(SUM(-($7, $5)) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING), 999)])" + NL +
            "    TableAccessRel(table=[[SALES, EMP]])" + NL);
    }

    /**
     * Test one of the custom conversions which is recognized by the class
     * of the operator (in this case, {@link SqlCaseOperator}).
     */
    public void testCase()
    {
        check("values (case 'a' when 'a' then 1 end)",
            "ProjectRel(EXPR$0=[$0])" + NL +
            "  ProjectRel(EXPR$0=[CASE(=(_ISO-8859-1'a', _ISO-8859-1'a'), 1, CAST(null):INTEGER)])" + NL +
            "    OneRowRel" + NL);
    }

    /**
     * Tests one of the custom conversions which is recognized by the identity
     * of the operator (in this case,
     * {@link SqlStdOperatorTable#characterLengthFunc}).
     */
    public void testCharLength()
    {
        // Note that CHARACTER_LENGTH becomes CHAR_LENGTH.
        check("values (character_length('foo'))",
            "ProjectRel(EXPR$0=[$0])" + NL +
            "  ProjectRel(EXPR$0=[CHAR_LENGTH(_ISO-8859-1'foo')])" + NL +
            "    OneRowRel" + NL);
    }

    public void testOverAvg()
    {
        check(
            "select sum(sal) over w1," + NL +
            "  avg(sal) over w1" + NL +
            "from emp" + NL +
            "window w1 as (partition by job order by hiredate rows 2 preceding)",

            "ProjectRel(EXPR$0=[SUM($5) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING)], EXPR$1=[CASE(=(COUNT($5) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING), 0), CAST(null):INTEGER, /(SUM($5) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING), COUNT($5) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING)))])" + NL +
            "  TableAccessRel(table=[[SALES, EMP]])" + NL);
    }

    public void testOverCountStar()
    {
        check(
            "select count(sal) over w1," + NL +
            "  count(*) over w1" + NL +
            "from emp" + NL +
            "window w1 as (partition by job order by hiredate rows 2 preceding)",

            "ProjectRel(EXPR$0=[COUNT($5) OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING)], EXPR$1=[COUNT() OVER (PARTITION BY $2 ORDER BY $4" + NL +
            "ROWS 2 PRECEDING)])" + NL +
            "  TableAccessRel(table=[[SALES, EMP]])" + NL);
    }

    public void testExplainAsXml() {
        String sql = "select 1 + 2, 3 from (values (true))";
        final RelNode rel = tester.convertSqlToRel(sql);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelOptXmlPlanWriter planWriter = new RelOptXmlPlanWriter(pw);
        rel.explain(planWriter);
        pw.flush();
        TestUtil.assertEqualsVerbose(
            "<RelNode type=\"ProjectRel\">" + NL +
            "\t<Property name=\"EXPR$0\">" + NL +
            "\t\t+(1, 2)\t</Property>" + NL +
            "\t<Property name=\"EXPR$1\">" + NL +
            "\t\t3\t</Property>" + NL +
            "\t<Inputs>" + NL +
            "\t\t<RelNode type=\"ProjectRel\">" + NL +
            "\t\t\t<Property name=\"EXPR$0\">" + NL +
            "\t\t\t\t$0\t\t\t</Property>" + NL +
            "\t\t\t<Inputs>" + NL +
            "\t\t\t\t<RelNode type=\"ProjectRel\">" + NL +
            "\t\t\t\t\t<Property name=\"EXPR$0\">" + NL +
            "\t\t\t\t\t\ttrue\t\t\t\t\t</Property>" + NL +
            "\t\t\t\t\t<Inputs>" + NL +
            "\t\t\t\t\t\t<RelNode type=\"OneRowRel\">" + NL +
            "\t\t\t\t\t\t\t<Inputs/>" + NL +
            "\t\t\t\t\t\t</RelNode>" + NL +
            "\t\t\t\t\t</Inputs>" + NL +
            "\t\t\t\t</RelNode>" + NL +
            "\t\t\t</Inputs>" + NL +
            "\t\t</RelNode>" + NL +
            "\t</Inputs>" + NL +
            "</RelNode>" + NL,
            sw.toString());
    }

}

// End ConverterTest.java
