/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2006 Disruptive Tech
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
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.*;
import org.eigenbase.test.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Pattern;


/**
 * Unit test for {@link SqlToRelConverter}.
 *
 * @version $Id$
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

    protected DiffRepository getDiffRepos()
    {
        return DiffRepository.lookup(ConverterTest.class);
    }
    
    private void check(
        String sql)
    {
        TestContext testContext = getTestContext();
        final SqlNode sqlQuery;
        try {
            sqlQuery = new SqlParser(sql).parseQuery();
        } catch (SqlParseException e) {
            throw new AssertionFailedError(e.toString());
        }
        final SqlValidator validator =
            SqlValidatorUtil.newValidator(
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
        final RelNode rel = converter.convertQuery(sqlQuery, true, true);
        assertTrue(rel != null);
        final StringWriter sw = new StringWriter();
        final RelOptPlanWriter planWriter =
            new RelOptPlanWriter(new PrintWriter(sw));
        planWriter.setIdPrefix(false);
        rel.explain(planWriter);
        planWriter.flush();
        String actual = sw.toString();
        String actual2 = pattern.matcher(actual).replaceAll("{con}");
        String actual3 = pattern2.matcher(actual2).replaceAll("{sales}");
        final DiffRepository diffRepos = getDiffRepos();
        String sql2 = diffRepos.expand("sql", sql);
        diffRepos.assertEquals("plan", "${plan}", NL + actual3);
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
        private final SqlValidatorCatalogReader seeker;
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
        check("select 1 from \"emps\"");
    }

    public void testStringLiteral()
    {
        check("select 'foo' from \"emps\"");
    }

    public void testSelectListAlias()
    {
        check("select 1 as one, 'foo' foo, 1 bar from \"emps\"");
    }

    public void testSelectListColumns()
    {
        check("select \"emps\".\"gender\", \"empno\", \"deptno\" as \"d\" from \"emps\"");
    }

    public void testFromList()
    {
        // "FROM x, y" == "x INNER JOIN y ON true"
        check("select 1 from \"emps\", \"depts\"");
    }

    public void testFromAlias()
    {
        check("select 1 from \"emps\" as \"e\"");
    }

    public void testFromJoin()
    {
        check("select 1 from \"emps\" join \"depts\" on \"emps\".\"deptno\" = \"depts\".\"deptno\"");
    }

    // todo: Enable when validator can handle USING
    public void _testFromLeftJoinUsing()
    {
        check("select 1 from \"emps\" left join \"depts\" using (\"deptno\")");
    }

    public void testFromFullJoin()
    {
        check("select 1 from \"emps\" full join \"depts\" on \"emps\".\"deptno\" = \"depts\".\"deptno\"");
    }

    public void testFromJoin3()
    {
        check("select 1 from \"emps\" "
            + "join \"depts\" on \"emps\".\"deptno\" = \"depts\".\"deptno\" "
            + "join (select * from \"emps\" where \"gender\" = 'F') as \"femaleEmps\" on \"femaleEmps\".\"empno\" = \"emps\".\"empno\"");
    }

    // todo: Enable when validator can handle NATURAL JOIN
    public void _testFromNaturalRightJoin()
    {
        check("select 1 from \"emps\" natural right join \"depts\"");
    }

    public void testWhereSimple()
    {
        check("select 1 from \"emps\" where \"gender\" = 'F'");
    }

    public void testWhereAnd()
    {
        check("select 1 from \"emps\" where \"gender\" = 'F' and \"deptno\" = 10");
    }

    public void _testOrder()
    {
        check("select * from \"emps\" order by empno asc, salary, deptno desc");
    }

    public void _testOrderOrdinal()
    {
        check("select * from \"emps\" order by 3 asc, salary, 1 desc");
    }

    public void _testOrderLiteral()
    {
        check("select * from \"emps\" order by 'A string', salary, true, null desc");
    }

    public void _testFromQuery()
    {
        check("select 1 from (select * from \"emps\")");
    }

    public void testQueryInSelect()
    {
        check("select \"gender\", (select \"name\" from \"depts\" where \"deptno\" = \"e\".\"deptno\") from \"emps\" as \"e\"");
    }

    public void testExistsUncorrelated()
    {
        check("select * from \"emps\" where exists (select 1 from \"depts\")");
    }

    public void testExistsCorrelated()
    {
        check("select * from \"emps\" "
            + "where exists (select 1 + 2 from \"depts\" where \"deptno\" > 10) "
            + "or exists (select 'foo' from \"emps\" where \"gender\" = 'Pig')");
    }

    public void testUnion()
    {
        check("select 1 from \"emps\" union select 2 from \"depts\"");
    }

    public void testUnionAll()
    {
        check("select 1 from \"emps\" union all select 2 from \"depts\"");
    }

    public void testUnionInFrom()
    {
        check("select * from (select 1 as \"i\", 3 as \"j\" from \"emps\" union select 2, 5 from \"depts\") where \"j\" > 4");
    }

    public void testJoinOfValues()
    {
        // NOTE jvs 15-Nov-2003:  I put this test in when I fixed a
        // converter bug; the individual rows were getting registered as
        // leaves, rather than the entire VALUES expression (as required to
        // get the join references correct).
        check("select * from (values (1), (2)), (values (3))");
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
            + "    and \"e2\".\"deptno\" = \"d\".\"deptno\"))");
    }

    // TODO jvs 15-Nov-2003: Parser handles this monstrosity OK, but converter
    // chokes.  The only reason I thought of it is that I was reading
    // CommonParser.jj and noticed that it allowed an arbitrary number of IN's
    // in sequence.  At first I thought it was a mistake, but then I remembered
    // boolean values.
    public void _testInSquared()
    {
        check("select * from \"depts\" where deptno in (10) in (true)");
    }
}


// End ConverterTest.java
