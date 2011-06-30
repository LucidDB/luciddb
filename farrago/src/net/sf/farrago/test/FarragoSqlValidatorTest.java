/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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

import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.test.*;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.util.EigenbaseException;

import net.sf.farrago.query.FarragoSqlValidator;
import net.sf.farrago.session.mock.*;
import net.sf.farrago.parser.FarragoParser;

/**
 * Unit test for farrago functionality in validator (mainly DML, which cannot
 * be tested in a pure org.eigenbase environment).
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FarragoSqlValidatorTest
    extends SqlValidatorTest
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FarragoSqlValidatorTest.
     *
     * @param testName JUnit test name
     */
    public FarragoSqlValidatorTest(String testName)
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlValidatorTestCase.Tester getTester(
        SqlConformance conformance)
    {
        return new TesterImpl(conformance);
    }

    //~ Test cases -------------------------------------------------------------

    public void testRewriteWithoutIdentifierExpansion()
    {
        // Intentionally empty.
        // This test fails when run under FarragoValidator.  The
        // FarragoValidator layer forces ID expansion no matter
        // what the flag is set to in SqlValidatorImpl.
    }

    /**
     * Tests non-streaming INSERT. (Cannot put into
     * {@link org.eigenbase.test.SqlValidatorTest} because DDL parser depends
     * on stuff in net.sf.farrago, not just org.eigenbase.)
     */
    public void testInsert()
    {
        checkFails(
            "insert into ^nonexistent^ (a, b) values (1, 2)",
            "Table 'NONEXISTENT' not found");

        checkFails(
            "insert into emp (empno, ^nonexistent^) values (1, 2)",
            "Unknown target column 'NONEXISTENT'");

        checkResultType(
            "insert into emp(empno,hiredate) values (1,null)",
            "RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER NOT NULL MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER) NOT NULL");

        checkResultType(
            "insert into dept values (10, 'Foo')",
            "RecordType(INTEGER NOT NULL DEPTNO,"
            + " VARCHAR(10) NOT NULL NAME) NOT NULL");

        checkResultType(
            "insert into dept values (10, ?)",
            "RecordType(INTEGER NOT NULL DEPTNO,"
            + " VARCHAR(10) NOT NULL NAME) NOT NULL");
    }

    public void testInsertDatatypeMismatch()
    {
        // datatype mismatch
        checkFails(
            "insert into emp (empno, ^sal^) values (1, 'abc')",
            "Cannot assign to target field 'SAL' of type INTEGER from source field 'EXPR\\$1' of type CHAR\\(3\\)");

        // repeated column
        checkFails(
            "insert into emp (empno, sal, ^empno^) values (1, 2, 3)",
            "Target column 'EMPNO' is assigned more than once");
    }

    public void testUpdate()
    {
        final String x =
            "RecordType(INTEGER NOT NULL EMPNO,"
            + " VARCHAR(20) NOT NULL ENAME,"
            + " VARCHAR(10) NOT NULL JOB,"
            + " INTEGER NOT NULL MGR,"
            + " TIMESTAMP(0) NOT NULL HIREDATE,"
            + " INTEGER NOT NULL SAL,"
            + " INTEGER NOT NULL COMM,"
            + " INTEGER NOT NULL DEPTNO,"
            + " BOOLEAN NOT NULL SLACKER) NOT NULL";
        tester.checkResultType(
            "UPDATE emp SET sal = 345 WHERE empno = 123",
            x);
        tester.checkResultType(
            "UPDATE emp SET sal = ? WHERE empno = 123",
            x);
    }

    //~ Inner classes ----------------------------------------------------------

    /**
     * Test policy which uses a Farrago parser and validator.
     */
    static class TesterImpl extends SqlValidatorTestCase.TesterImpl
    {
        TesterImpl(SqlConformance conformance)
        {
            super(conformance);
        }

        public SqlValidator getValidator()
        {
            final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl();
            final MockCatalogReader catalogReader =
                new MockCatalogReader(typeFactory);

            FarragoSqlValidator validator =
                new FarragoSqlValidator(
                    opTab,
                    catalogReader,
                    typeFactory,
                    conformance,
                    MockSession.instance().getPreparingStmt());

            // REVIEW: SWZ: 9/5/2006: org.eigenbase.test.SqlValidatorTest
            // assumes no expansion.  Should probably rework things to
            // avoid this.
            validator.setIdentifierExpansion(false);

            return validator;
        }

        public SqlNode parseQuery(String sql) throws SqlParseException
        {
            try {
                final FarragoParser parser = new FarragoParser();
                final MockSessionStmtValidator stmtValidator =
                     new MockSessionStmtValidator();
                stmtValidator.setParser(parser);

                final MockReposTxnContext reposTxnContext =
                    new MockReposTxnContext();
                stmtValidator.setReposTxnContext(reposTxnContext);
                return (SqlNode)
                    parser.parseSqlText(stmtValidator, null, sql, true);
            } catch (EigenbaseException e) {
                Throwable cause = e.getCause();
                if (cause instanceof SqlParseException) {
                    // Fake a SqlParseException, so this method behaves
                    // like its base method.
                    throw (SqlParseException) cause;
                }
                throw e;
            }
        }
    }
}

// End FarragoSqlValidatorTest.java
