/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
import org.eigenbase.sql.*;
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

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(SqlToRelConverterTest.class);
    }

    protected void check(
        String sql,
        String plan)
    {
        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand("sql", sql);
        final RelNode rel = tester.convertSqlToRel(sql2);

        assertTrue(rel != null);
        String actual = RelOptUtil.toString(rel);
        diffRepos.assertEquals("plan", plan, actual);
    }

    /**
     * Mock implementation of {@link RelOptSchema}.
     */
    protected static class MockRelOptSchema implements RelOptSchema {
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
            return createColumnSet(names, rowType);
        }

        protected MockColumnSet createColumnSet(
            String[] names,
            final RelDataType rowType)
        {
            return new MockColumnSet(names, rowType);
        }

        public RelDataTypeFactory getTypeFactory()
        {
            return typeFactory;
        }

        public void registerRules(RelOptPlanner planner)
            throws Exception
        {
        }

        protected class MockColumnSet implements RelOptTable {
            private final String[] names;
            private final RelDataType rowType;

            protected MockColumnSet(String[] names, RelDataType rowType)
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

        MockRelOptSchema createRelOptSchema(
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory);
    };

    /**
     * Default implementation of {@link Tester}, using mock classes
     * {@link MockRelOptSchema}, {@link MockRelOptConnection} and
     * {@link MockRelOptPlanner}.
     */
    public static class TesterImpl implements Tester
    {
        private RelOptPlanner planner;
        private static final boolean Dtbug471Fixed = false;

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
                createRelOptSchema(catalogReader, typeFactory);
            final RelOptConnection relOptConnection =
                new MockRelOptConnection(relOptSchema);
            final SqlToRelConverter converter =
                createSqlToRelConverter(
                    validator, relOptSchema, relOptConnection, typeFactory);
            final RelNode rel;
            if (Dtbug471Fixed) {
                final SqlNode validatedQuery = validator.validate(sqlQuery);
                rel = converter.convertQuery(validatedQuery, false, true);
            } else {
                rel = converter.convertQuery(sqlQuery, true, true);
            }
            Util.post(rel != null, "return != null");
            return rel;
        }

        public MockRelOptSchema createRelOptSchema(
            final SqlValidatorCatalogReader catalogReader,
            final RelDataTypeFactory typeFactory)
        {
            return new MockRelOptSchema(catalogReader, typeFactory);
        }

        protected SqlToRelConverter createSqlToRelConverter(
            final SqlValidator validator,
            final RelOptSchema relOptSchema,
            final RelOptConnection relOptConnection,
            final RelDataTypeFactory typeFactory)
        {
            final SqlToRelConverter converter =
                new SqlToRelConverter(
                    validator,
                    relOptSchema,
                    OJSystem.env,
                    getPlanner(),
                    relOptConnection,
                    new JavaRexBuilder(typeFactory));
            return converter;
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
            "${plan}");
    }

    public void testGroup()
    {
        check("select deptno from emp group by deptno",
            "${plan}");
    }

    public void testGroupJustOneAgg()
    {
        // just one agg
        check("select deptno, sum(sal) from emp group by deptno",
            "${plan}");
    }

    public void testGroupExpressionsInsideAndOut()
    {
        // Expressions inside and outside aggs.
        // Common sub-expressions should be eliminated: 'sal' always translates
        // to expression #2.
        check("select deptno + 4, sum(sal), sum(3 + sal), 2 * count(sal) from emp group by deptno",
            "${plan}");
    }

    public void testHaving()
    {
        // empty group-by clause, having
        check("select sum(sal + sal) from emp having sum(sal) > 10",
            "${plan}");
    }

    public void testGroupBug281() {
        // Dtbug 281 gives:
        //   Internal error:
        //   Type 'RecordType(VARCHAR(128) $f0)' has no field 'NAME'
        check("select name from (select name from dept group by name)",
            "${plan}");
    }

    public void testGroupBug281b() {

        // Try to confuse it with spurious columns.
        check("select name, foo from (" +
            "select deptno, name, count(deptno) as foo " +
            "from dept " +
            "group by name, deptno, name)",
            "${plan}");
    }

    public void testAggDistinct()
    {
        check(
            "select deptno, sum(sal), sum(distinct sal), count(*) " +
            "from emp " +
            "group by deptno",
            "${plan}");
    }

    public void testUnnest()
    {
        check("select*from unnest(multiset[1,2])",
            "${plan}");
    }

    public void testUnnestSubquery()
    {
        check("select*from unnest(multiset(select*from dept))",
            "${plan}");
    }

    public void testMultisetSubquery()
    {
        check("select multiset(select deptno from dept) from (values(true))",
            "${plan}");
    }

    public void testMultiset()
    {
        check("select 'a',multiset[10] from dept",
            "${plan}");
    }

    public void testMultisetOfColumns() {
        check("select 'abc',multiset[deptno,sal] from emp",
            "${plan}");
    }

    public void testCorrelationJoin()
    {
        check("select *," +
            "         multiset(select * from emp where deptno=dept.deptno) " +
            "               as empset" +
            "      from dept",
            "${plan}");
    }

    public void testExists()
    {
        check("select*from emp where exists (select 1 from dept where deptno=55)",
            "${plan}");
    }

    public void testExistsCorrelated()
    {
        check("select*from emp where exists (select 1 from dept where emp.deptno=dept.deptno)",
            "${plan}");
    }

    public void testUnnestSelect() {
        check("select*from unnest(select multiset[deptno] from dept)",
            "${plan}");
    }

    public void testLateral() {
        check("select * from emp, LATERAL (select * from dept where emp.deptno=dept.deptno)",
            "${plan}");
    }

    public void testElement()
    {
        check("select element(multiset[5]) from emp",
            "${plan}");

    }

    public void testElementInValues()
    {
        check("values element(multiset[5])",
            "${plan}");
    }

    public void testUnionAll()
    {
        // union all
        check( "select empno from emp union all select deptno from dept",
            "${plan}");
    }

    public void testUnion()
    {
        // union without all
        check("select empno from emp union select deptno from dept",
            "${plan}");
    }

    public void testUnionValues()
    {
        // union with values
        check("values (10), (20)" + NL +
            "union all" + NL +
            "select 34 from emp" + NL +
            "union all values (30), (45 + 10)",
            "${plan}");
    }

    public void testUnionSubquery()
    {
        // union of subquery, inside from list, also values
        check("select deptno from emp as emp0 cross join" + NL +
            " (select empno from emp union all " + NL +
            "  select deptno from dept where deptno > 20 union all" + NL +
            "  values (45), (67))",
            "${plan}");
    }

    public void testIsDistinctFrom()
    {
        check("select 1 is distinct from 2 from (values(true))",
            "${plan}");
    }

    public void testIsNotDistinctFrom()
    {
        check("select 1 is not distinct from 2 from (values(true))",
            "${plan}");
    }

    public void testNotLike()
    {
        // note that 'x not like y' becomes 'not(x like y)'
        check("values ('a' not like 'b' escape 'c')",
            "${plan}");
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
            "${plan}");
    }

    /**
     * Test one of the custom conversions which is recognized by the class
     * of the operator (in this case, {@link SqlCaseOperator}).
     */
    public void testCase()
    {
        check("values (case 'a' when 'a' then 1 end)",
            "${plan}");
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
            "${plan}");
    }

    public void testOverAvg()
    {
        check(
            "select sum(sal) over w1," + NL +
            "  avg(sal) over w1" + NL +
            "from emp" + NL +
            "window w1 as (partition by job order by hiredate rows 2 preceding)",

            "${plan}");
    }

    public void testOverCountStar()
    {
        check(
            "select count(sal) over w1," + NL +
            "  count(*) over w1" + NL +
            "from emp" + NL +
            "window w1 as (partition by job order by hiredate rows 2 preceding)",

            "${plan}");
    }

    public void testExplainAsXml() {
        String sql = "select 1 + 2, 3 from (values (true))";
        final RelNode rel = tester.convertSqlToRel(sql);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelOptXmlPlanWriter planWriter =
            new RelOptXmlPlanWriter(pw, SqlExplainLevel.DIGEST_ATTRIBUTES);
        rel.explain(planWriter);
        pw.flush();
        DiffRepository diffRepos = getDiffRepos();
        TestUtil.assertEqualsVerbose(
            TestUtil.fold(new String[]{
                "<RelNode type=\"ProjectRel\">",
                "\t<Property name=\"EXPR$0\">",
                "\t\t+(1, 2)\t</Property>",
                "\t<Property name=\"EXPR$1\">",
                "\t\t3\t</Property>",
                "\t<Inputs>",
                "\t\t<RelNode type=\"ProjectRel\">",
                "\t\t\t<Property name=\"EXPR$0\">",
                "\t\t\t\t$0\t\t\t</Property>",
                "\t\t\t<Inputs>",
                "\t\t\t\t<RelNode type=\"ProjectRel\">",
                "\t\t\t\t\t<Property name=\"EXPR$0\">",
                "\t\t\t\t\t\ttrue\t\t\t\t\t</Property>",
                "\t\t\t\t\t<Inputs>",
                "\t\t\t\t\t\t<RelNode type=\"OneRowRel\">",
                "\t\t\t\t\t\t\t<Inputs/>",
                "\t\t\t\t\t\t</RelNode>",
                "\t\t\t\t\t</Inputs>",
                "\t\t\t\t</RelNode>",
                "\t\t\t</Inputs>",
                "\t\t</RelNode>",
                "\t</Inputs>",
                "</RelNode>",
                ""}),
            sw.toString());
    }
}

// End SqlToRelConverterTest.java
