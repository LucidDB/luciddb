/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
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

package net.sf.saffron.sql2rel;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import net.sf.saffron.jdbc.SaffronJdbcConnection;
import net.sf.saffron.oj.OJPlannerFactory;
import openjava.mop.*;
import openjava.ptree.ClassDeclaration;
import openjava.ptree.MemberDeclarationList;
import openjava.ptree.ModifierList;
import org.eigenbase.oj.*;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Pattern;


/**
 * Unit test for {@link SqlToRelConverter}.
 */
public class ConverterTest extends TestCase
{
    private static final String NL = System.getProperty("line.separator");
    private static TestContext testContext;
    private static final Pattern pattern =
        Pattern.compile(
            "net.sf.saffron.oj.OJConnectionRegistry.instance.get\\( \"[0-9]+\" \\)");
    private static final Pattern pattern2 =
        Pattern.compile(
            "\\(\\(sales.SalesInMemory\\) \\(\\(net.sf.saffron.jdbc.SaffronJdbcConnection.MyConnection\\) \\{con\\}\\).target\\)");

    protected void setUp()
        throws Exception
    {
        super.setUp();

        // Create a type factory.
        OJTypeFactory typeFactory =
            OJUtil.threadTypeFactory();
        if (typeFactory == null) {
            typeFactory = new OJTypeFactoryImpl();
            OJUtil.setThreadTypeFactory(typeFactory);
        }

        // And a planner factory.
        if (OJPlannerFactory.threadInstance() == null) {
            OJPlannerFactory.setThreadInstance(new OJPlannerFactory());
        }
    }

    private void check(
        String sql,
        String plan)
    {
        TestContext testContext = getTestContext();
        final SqlNode sqlQuery;
        try {
            sqlQuery = new SqlParser(sql).parseQuery();
        } catch (SqlParseException e) {
            throw new AssertionFailedError(e.toString());
        }
        final SqlValidator validator =
            new SqlValidator(
                SqlStdOperatorTable.instance(),
                testContext.seeker,
                testContext.connection.getRelOptSchema().getTypeFactory());
        final SqlToRelConverter converter =
            new SqlToRelConverter(validator,
                testContext.connection.getRelOptSchema(), testContext.env,
                OJPlannerFactory.threadInstance().newPlanner(),
                testContext.connection,
                new JavaRexBuilder(testContext.connection.getRelOptSchema()
                        .getTypeFactory()));
        final RelNode rel = converter.convertQuery(sqlQuery);
        assertTrue(rel != null);
        final StringWriter sw = new StringWriter();
        final RelOptPlanWriter planWriter =
            new RelOptPlanWriter(new PrintWriter(sw));
        planWriter.withIdPrefix = false;
        rel.explain(planWriter);
        planWriter.flush();
        String actual = sw.toString();
        String actual2 = pattern.matcher(actual).replaceAll("{con}");
        String actual3 = pattern2.matcher(actual2).replaceAll("{sales}");
        Util.assertEqualsVerbose(plan, actual3);
    }

    static TestContext getTestContext()
    {
        if (testContext == null) {
            testContext = new TestContext();
        }
        return testContext;
    }

    /**
     * Contains context shared between unit tests.
     *
     * <p>Lots of nasty stuff to set up the Openjava environment, should be
     * removed when we're not dependent upon Openjava.</p>
     */
    static class TestContext
    {
        private final SqlValidator.CatalogReader seeker;
        private final Connection jdbcConnection;
        private final RelOptConnection connection;
        Environment env;
        private int executionCount;

        TestContext()
        {
            try {
                Class.forName("net.sf.saffron.jdbc.SaffronJdbcDriver");
            } catch (ClassNotFoundException e) {
                throw Util.newInternal(e, "Error loading JDBC driver");
            }
            try {
                jdbcConnection =
                    DriverManager.getConnection(
                        "jdbc:saffron:schema=sales.SalesInMemory");
            } catch (SQLException e) {
                throw Util.newInternal(e);
            }
            connection =
                ((SaffronJdbcConnection) jdbcConnection).saffronConnection;
            seeker =
                new SqlToRelConverter.SchemaCatalogReader(
                    connection.getRelOptSchema(),
                    false);

            // Nasty OJ stuff
            env = OJSystem.env;

            String packageName = getTempPackageName();
            String className = getTempClassName();
            env = new FileEnvironment(env, packageName, className);
            ClassDeclaration decl =
                new ClassDeclaration(new ModifierList(ModifierList.PUBLIC),
                    className, null, null, new MemberDeclarationList());
            OJClass clazz = new OJClass(env, null, decl);
            env.record(
                clazz.getName(),
                clazz);
            env = new ClosedEnvironment(clazz.getEnvironment());

            // Ensure that the thread has factories for types and planners. (We'd
            // rather that the client sets these.)
            OJTypeFactory typeFactory =
                OJUtil.threadTypeFactory();
            if (typeFactory == null) {
                typeFactory = new OJTypeFactoryImpl();
                OJUtil.setThreadTypeFactory(typeFactory);
            }
            if (OJPlannerFactory.threadInstance() == null) {
                OJPlannerFactory.setThreadInstance(
                    new OJPlannerFactory());
            }

            OJUtil.threadDeclarers.set(clazz);
        }

        protected static String getClassRoot()
        {
            return SaffronProperties.instance().classDir.get(true);
        }

        protected String getTempClassName()
        {
            return "Dummy_"
            + Integer.toHexString(this.hashCode() + executionCount++);
        }

        protected static String getJavaRoot()
        {
            return SaffronProperties.instance().javaDir.get();
        }

        protected String getTempPackageName()
        {
            return SaffronProperties.instance().packageName.get();
        }
    }

    //~ TESTS  -------------------------------------

    public void testIntegerLiteral()
    {
        check("select 1 from \"emps\"",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void testStringLiteral()
    {
        check("select 'foo' from \"emps\"",
            "ProjectRel(EXPR$0=[_ISO-8859-1'foo' COLLATE ISO-8859-1$en_US$primary])"
            + NL
            + "  ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void testSelectListAlias()
    {
        check("select 1 as one, 'foo' foo, 1 bar from \"emps\"",
            "ProjectRel(ONE=[1], FOO=[_ISO-8859-1'foo' COLLATE ISO-8859-1$en_US$primary], BAR=[1])"
            + NL
            + "  ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void testSelectListColumns()
    {
        check("select \"emps\".\"gender\", \"empno\", \"deptno\" as \"d\" from \"emps\"",
            "ProjectRel(gender=[$3], empno=[$0], d=[$2])" + NL
            + "  ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void testFromList()
    {
        // "FROM x, y" == "x INNER JOIN y ON true"
        check("select 1 from \"emps\", \"depts\"",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  JoinRel(condition=[true], joinType=[inner])" + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])"
            + NL);
    }

    public void testFromAlias()
    {
        check("select 1 from \"emps\" as \"e\"",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void testFromJoin()
    {
        check("select 1 from \"emps\" join \"depts\" on \"emps\".\"deptno\" = \"depts\".\"deptno\"",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  JoinRel(condition=[=($2, $6)], joinType=[inner])" + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])"
            + NL);
    }

    // todo: Enable when validator can handle USING
    public void _testFromLeftJoinUsing()
    {
        check("select 1 from \"emps\" left join \"depts\" using (\"deptno\")",
            "?");
    }

    public void testFromFullJoin()
    {
        check("select 1 from \"emps\" full join \"depts\" on \"emps\".\"deptno\" = \"depts\".\"deptno\"",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  JoinRel(condition=[=($2, $6)], joinType=[full])" + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])" + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])" + NL);
    }

    public void testFromJoin3()
    {
        check("select 1 from \"emps\" "
            + "join \"depts\" on \"emps\".\"deptno\" = \"depts\".\"deptno\" "
            + "join (select * from \"emps\" where \"gender\" = 'F') as \"femaleEmps\" on \"femaleEmps\".\"empno\" = \"emps\".\"empno\"",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  JoinRel(condition=[=($8, $0)], joinType=[inner])" + NL
            + "    JoinRel(condition=[=($2, $6)], joinType=[inner])" + NL
            + "      ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])" + NL
            + "      ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])" + NL
            + "    ProjectRel(empno=[$0], name=[$1], deptno=[$2], gender=[$3], city=[$4], slacker=[$5])" + NL
            + "      FilterRel(condition=[=($3, _ISO-8859-1'F' COLLATE ISO-8859-1$en_US$primary)])" + NL
            + "        ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])" + NL);
    }

    // todo: Enable when validator can handle NATURAL JOIN
    public void _testFromNaturalRightJoin()
    {
        check("select 1 from \"emps\" natural right join \"depts\"", "?");
    }

    public void testWhereSimple()
    {
        check("select 1 from \"emps\" where \"gender\" = 'F'",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  FilterRel(condition=[=($3, _ISO-8859-1'F' COLLATE ISO-8859-1$en_US$primary)])"
            + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void testWhereAnd()
    {
        check("select 1 from \"emps\" where \"gender\" = 'F' and \"deptno\" = 10",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  FilterRel(condition=[AND(=($3, _ISO-8859-1'F' COLLATE ISO-8859-1$en_US$primary), =($2, 10))])"
            + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void _testOrder()
    {
        check("select * from \"emps\" order by empno asc, salary, deptno desc",
            "?");
    }

    public void _testOrderOrdinal()
    {
        check("select * from \"emps\" order by 3 asc, salary, 1 desc", "?");
    }

    public void _testOrderLiteral()
    {
        check("select * from \"emps\" order by 'A string', salary, true, null desc",
            "?");
    }

    public void _testFromQuery()
    {
        check("select 1 from (select * from \"emps\")",
            "ProjectRel(EXPR$0=[1])" + NL
            + "  ProjectRel(city=[emps.city], gender=[emps.gender], name=[emps.name], deptno=[emps.deptno], empno=[emps.empno])"
            + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void testQueryInSelect()
    {
        check("select \"gender\", (select \"name\" from \"depts\" where \"deptno\" = \"e\".\"deptno\") from \"emps\" as \"e\"",
            "ProjectRel(gender=[$3], EXPR$1=[$6])" + NL
            + "  JoinRel(condition=[true], joinType=[left])" + NL
            + "    ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL + "    ProjectRel(name=[$1])" + NL
            + "      FilterRel(condition=[=($0, $cor0.deptno)])" + NL
            + "        ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])"
            + NL);
    }

    public void testExistsUncorrelated()
    {
        check("select * from \"emps\" where exists (select 1 from \"depts\")",
            "ProjectRel(empno=[$0], name=[$1], deptno=[$2], gender=[$3], city=[$4], slacker=[$5])"
            + NL + "  FilterRel(condition=[$7])" + NL
            + "    JoinRel(condition=[true], joinType=[left])" + NL
            + "      ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL + "      ProjectRel(EXPR$0=[$0], $indicator=[true])" + NL
            + "        ProjectRel(EXPR$0=[1])" + NL
            + "          ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])"
            + NL);
    }

    // todo: implement IN
    public void _testQueryInWhereUncorrelated()
    {
        check("select * from \"depts\" where \"deptno\" in (select \"deptno\" from \"depts\")",
            "");
    }

    public void testExistsCorrelated()
    {
        check("select * from \"emps\" "
            + "where exists (select 1 + 2 from \"depts\" where \"deptno\" > 10) "
            + "or exists (select 'foo' from \"emps\" where \"gender\" = 'Pig')",
            "ProjectRel(empno=[$0], name=[$1], deptno=[$2], gender=[$3], city=[$4], slacker=[$5])"
            + NL + "  FilterRel(condition=[OR($7, $9)])" + NL
            + "    JoinRel(condition=[true], joinType=[left])" + NL
            + "      JoinRel(condition=[true], joinType=[left])" + NL
            + "        ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL + "        ProjectRel(EXPR$0=[$0], $indicator=[true])" + NL
            + "          ProjectRel(EXPR$0=[+(1, 2)])" + NL
            + "            FilterRel(condition=[>($0, 10)])" + NL
            + "              ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])"
            + NL + "      ProjectRel(EXPR$0=[$0], $indicator=[true])" + NL
            + "        ProjectRel(EXPR$0=[_ISO-8859-1'foo' COLLATE ISO-8859-1$en_US$primary])"
            + NL
            + "          FilterRel(condition=[=($3, _ISO-8859-1'Pig' COLLATE ISO-8859-1$en_US$primary)])"
            + NL
            + "            ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL);
    }

    public void testUnion()
    {
        check("select 1 from \"emps\" union select 2 from \"depts\"",
            "ProjectRel(EXPR$0=[$0])" + NL + "  UnionRel(all=[false])" + NL
            + "    ProjectRel(EXPR$0=[1])" + NL
            + "      ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL + "    ProjectRel(EXPR$0=[2])" + NL
            + "      ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])"
            + NL);
    }

    public void testUnionAll()
    {
        check("select 1 from \"emps\" union all select 2 from \"depts\"",
            "ProjectRel(EXPR$0=[$0])" + NL + "  UnionRel(all=[true])" + NL
            + "    ProjectRel(EXPR$0=[1])" + NL
            + "      ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL + "    ProjectRel(EXPR$0=[2])" + NL
            + "      ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])"
            + NL);
    }

    public void testUnionInFrom()
    {
        check("select * from (select 1 as \"i\", 3 as \"j\" from \"emps\" union select 2, 5 from \"depts\") where \"j\" > 4",
            "ProjectRel(i=[$0], j=[$1])" + NL
            + "  FilterRel(condition=[>($1, 4)])" + NL
            + "    ProjectRel(i=[$0], j=[$1])" + NL
            + "      UnionRel(all=[false])" + NL
            + "        ProjectRel(i=[1], j=[3])" + NL
            + "          ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL + "        ProjectRel(EXPR$0=[2], EXPR$1=[5])" + NL
            + "          ExpressionReaderRel(expression=[Java((sales.Dept[]) {sales}.depts)])"
            + NL);
    }

    public void testJoinOfValues()
    {
        // NOTE jvs 15-Nov-2003:  I put this test in when I fixed a
        // converter bug; the individual rows were getting registered as
        // leaves, rather than the entire VALUES expression (as required to
        // get the join references correct).
        check("select * from values (1), (2), values (3)",
            "ProjectRel(EXPR$0=[$0], EXPR$00=[$1])" + NL
            + "  JoinRel(condition=[true], joinType=[inner])" + NL
            + "    ProjectRel(EXPR$0=[$0])" + NL
            + "      UnionRel(all=[true])" + NL
            + "        ProjectRel(EXPR$0=[1])" + NL + "          OneRowRel"
            + NL + "        ProjectRel(EXPR$0=[2])" + NL
            + "          OneRowRel" + NL + "    ProjectRel(EXPR$0=[$0])" + NL
            + "      ProjectRel(EXPR$0=[3])" + NL + "        OneRowRel" + NL);
    }

    // todo: implement EXISTS
    public void _testComplexCorrelation()
    {
        // This query is an example of relational division: it finds all of
        // the genders which exist in all departments.
        check("select distinct \"gender\" from \"emps\" as \"e1\" "
            + "where not exists (" + "  select * from \"depts\" as \"d\" "
            + "  where not exists (" + "    select * from \"emps\" as \"e2\" "
            + "    where \"e1\".\"gender\" = \"e2\".\"gender\" "
            + "    and \"e2\".\"deptno\" = \"d\".\"deptno\"))", "");
    }

    // FIXME jvs 15-Nov-2003:  I disabled this because it was failing and the
    // expected output looks very wrong.
    public void _testInList()
    {
        check("select * from \"emps\" where \"deptno\" in (10,20,30)",
            "ProjectRel(city=[$input0.$f0.city], gender=[$input0.$f0.gender], name=[$input0.$f0.name], deptno=[$input0.$f0.deptno], empno=[$input0.$f0.empno])"
            + NL + "  FilterRel(condition=[$input1.$f0])" + NL
            + "    JoinRel(condition=[true], joinType=[left])" + NL
            + "      ExpressionReaderRel(expression=[Java((sales.Emp[]) {sales}.emps)])"
            + NL + "      ProjectRel($indicator=[true])" + NL
            + "        FilterRel(condition=[$input0.deptno == $input0.$f0])"
            + NL + "          UnionRel(all=[true])" + NL
            + "            ProjectRel(EXPR$0=[10])" + NL
            + "              OneRowRel" + NL
            + "            ProjectRel(EXPR$0=[20])" + NL
            + "              OneRowRel" + NL
            + "            ProjectRel(EXPR$0=[30])" + NL
            + "              OneRowRel" + NL);
    }

    // todo: make parser handle IN VALUES
    // REVIEW jvs 15-Nov-2003:  I'm not sure what you're after here.  You
    // should be able to put VALUES inside parentheses, and in that case it's
    // producing a row, not a table of scalars (same as everywhere else).  If
    // you want to construct a table, you do it like this:
    // ROW(X,Y,Z) IN (VALUES (1,2,3), (4,5,6)); in this case the
    // VALUES evaluates to a table of two rows with three columns.  I think.
    public void _testInValues()
    {
        check("select * from \"emps\" where \"deptno\" in values (10,20,30)",
            "");
    }

    // todo: make parser handle (1,deptno)
    public void _testInCompound()
    {
        check("select * from \"emps\" where (1,deptno) in (select 1,deptno from \"depts\")",
            "");
    }

    // TODO jvs 15-Nov-2003: Parser handles this monstrosity OK, but converter
    // chokes.  The only reason I thought of it is that I was reading
    // CommonParser.jj and noticed that it allowed an arbitrary number of IN's
    // in sequence.  At first I thought it was a mistake, but then I remembered
    // boolean values.
    public void _testInSquared()
    {
        check("select * from \"depts\" where deptno in (10) in (true)", "");
    }
}


// End ConverterTest.java
