/*
// $Id$
// Farrago is a relational database management system.
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
package com.disruptivetech.farrago.test;

import com.disruptivetech.farrago.calc.RexToCalcTranslator;
import com.disruptivetech.farrago.volcano.VolcanoPlannerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.*;

import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.test.*;

import openjava.mop.*;
import openjava.ptree.ClassDeclaration;
import openjava.ptree.MemberDeclarationList;
import openjava.ptree.ModifierList;
import openjava.ptree.util.ClassMap;

import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptConnection;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexTransformer;
import org.eigenbase.runtime.SyntheticObject;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.SqlValidator;
import org.eigenbase.sql.parser.ParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.SaffronProperties;
import org.eigenbase.util.Util;


/**
 * Validates that rex expressions gets correctly translated to a correct
 * calculator program
 *
 * @author Wael Chatila
 * @since Feb 3, 2004
 * @version $Id$
 **/
public class Rex2CalcPlanTest extends FarragoTestCase
{
    //~ Static fields/initializers --------------------------------------------

    private static final String NL = System.getProperty("line.separator");
    private static TestContext testContext;

    //~ Constructors ----------------------------------------------------------

    public Rex2CalcPlanTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ---------------------------------------------------------------

    protected void setUp()
        throws Exception
    {
        super.setUp();
        stmt.execute("set schema sales");
        FarragoJdbcEngineConnection farragoConn =
            (FarragoJdbcEngineConnection) connection;
        TestContext testContext = getTestContext();
        testContext.stmtValidator =
            farragoConn.getSession().newStmtValidator();
        testContext.stmt =
            (FarragoPreparingStmt) farragoConn.getSession().newPreparingStmt(testContext.stmtValidator);
    }

    protected void tearDown()
        throws Exception
    {
        TestContext testContext = getTestContext();
        testContext.stmt.closeAllocation();
        testContext.stmt = null;
        testContext.stmtValidator.closeAllocation();
        testContext.stmtValidator = null;
        super.tearDown();
    }

    // implement TestCase
    public static Test suite()
    {
        // TODO jvs 30-Aug-2004:  re-enable this test.  Apparently it was never
        // enabled in Saffron.  Once I moved it over to Farrago and
        // rehabilitated it, it started running automatically because of
        // Farrago's wildcard test system, but with lots of diffs.
        if (false) {
            return wrappedSuite(Rex2CalcPlanTest.class);
        } else {
            return new TestSuite();
        }
    }

    //--- Helper Functions ------------------------------------------------
    private void check(
        String sql,
        String expectedProgram,
        boolean nullSemanics,
        boolean shortCircuit,
        boolean doComments)
    {
        TestContext testContext = getTestContext();
        final SqlNode sqlQuery;
        try {
            sqlQuery = new SqlParser(sql).parseQuery();
        } catch (ParseException e) {
            throw new AssertionFailedError(e.toString());
        }
        RelDataTypeFactory typeFactory =
            testContext.stmt.getRelOptSchema().getTypeFactory();
        final SqlValidator validator =
            new SqlValidator(
                SqlOperatorTable.instance(),
                testContext.stmt,
                typeFactory);
        final JavaRexBuilder rexBuilder = new JavaRexBuilder(typeFactory);
        final SqlToRelConverter converter =
            new SqlToRelConverter(validator,
                testContext.stmt.getRelOptSchema(), testContext.env,
                testContext.stmt, rexBuilder);
        RelNode rootRel = converter.convertQuery(sqlQuery);
        assertTrue(rootRel != null);

        ProjectRel project = (ProjectRel) rootRel;
        FilterRel filter = (FilterRel) project.getInput(0);
        RexNode condition = filter.condition;
        if (nullSemanics) {
            condition =
                rexBuilder.makeCall(SqlOperatorTable.std().isTrueOperator,
                    condition);
            condition =
                new RexTransformer(condition, rexBuilder)
                    .tranformNullSemantics();
        }
        RexToCalcTranslator translator =
            new RexToCalcTranslator(rexBuilder,
                project.getChildExps(), condition);
        translator.setGenerateShortCircuit(shortCircuit);
        translator.setGenerateComments(doComments);
        String actual = translator.getProgram(null).trim();
        String expected = expectedProgram.trim();
        if (!expected.equals(actual)) {
            String message =
                "expected:<" + expected + ">" + NL + "but was:<" + actual
                + ">";

            String fileName = "CalcImpl_" + this.getClass().toString();
            try {
                OutputStreamWriter o =
                    new OutputStreamWriter(new FileOutputStream(fileName
                            + "_actual"));
                o.write(
                    actual,
                    0,
                    actual.length());
                o.close();
                o = new OutputStreamWriter(
                        new FileOutputStream(fileName + "_expected"));
                o.write(
                    expected,
                    0,
                    expected.length());
                o.close();
            } catch (IOException ignored) {
            }

            fail(message);
        }
    }

    static TestContext getTestContext()
    {
        if (testContext == null) {
            testContext = new TestContext();
        }
        return testContext;
    }

    //--- Tests ------------------------------------------------
    public void testSimplePassThroughFilter()
    {
        String sql = "SELECT empno,empno FROM emps WHERE empno > 10";
        String prg =
            "O s4, s4;" + NL + "I s4;" + NL + "L bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4;" + NL + "V 1, 0, 10;" + NL + "T;" + NL
            + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "REF O0, I0;" + NL
            + "REF O1, I0;" + NL + "RETURN;";
        check(sql, prg, false, false, false);
    }

    public void testSimplyEqualsFilter()
    {
        String sql = "select empno from emps where empno=123";
        String prg =
            "O s4;" + NL + "I s4;" + NL + "L bo, bo, bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4;" + NL + "V 1, 0, 123;" + NL + "T;" + NL
            + "ISNOTNULL L0, I0;" + NL + "EQ L1, I0, C2;" + NL
            + "AND L2, L0, L1;" + NL + "JMPT @6, L2;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "REF O0, I0;" + NL
            + "RETURN;" + NL;
        check(sql, prg, true, false, false);
    }

    public void testSimplyEqualsFilterWithComments()
    {
        String sql = "select empno from emps where name='Wael' and 123=0";
        String prg =
            "O s4;" + NL + "I vc,0, s4;" + NL + "L bo, s4, bo, bo;" + NL
            + "S bo;" + NL + "C bo, bo, vc,8, vc,48, s4, s4, s4;" + NL
            + "V 1, 0, 0x5761656C /* Wael */, 0x49534F2D383835392D3124656E5F5553247072696D617279 /* ISO-8859-1$en_US$primary */, 0, 123, 0;"
            + NL + "T;" + NL + "CALL 'strCmpA(L1, I0, C2) /* 0: */;" + NL
            + "EQ L0, L1, C4 /* 1: =($1, _ISO-8859-1'Wael' COLLATE ISO-8859-1$en_US$primary) */;"
            + NL + "EQ L2, C5, C6 /* 2: =(123, 0) */;" + NL
            + "AND L3, L0, L2 /* 3: AND(=($1, _ISO-8859-1'Wael' COLLATE ISO-8859-1$en_US$primary), =(123, 0)) */;"
            + NL + "JMPT @7, L3 /* 4: */;" + NL + "MOVE S0, C0 /* 5: */;" + NL
            + "RETURN /* 6: */;" + NL + "MOVE S0, C1 /* 7: */;" + NL
            + "REF O0, I1 /* 8: */;" + NL + "RETURN /* 9: */;";
        check(sql, prg, false, false, true);
    }

    public void testSimplyEqualsFilterShortCircuit()
    {
        String sql = "select empno from emps where empno=123";
        String prg =
            "O s4;" + NL + "I s4;" + NL + "L bo, bo, bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4;" + NL + "V 1, 0, 123;" + NL + "T;" + NL
            + "ISNOTNULL L0, I0;" + NL + "JMPF @5, L0;" + NL
            + "EQ L1, I0, C2;" + NL + "MOVE L2, L1;" + NL + "JMP @6;" + NL
            + "MOVE L2, C1;" + NL + "JMPT @9, L2;" + NL + "MOVE S0, C0;" + NL
            + "RETURN;" + NL + "MOVE S0, C1;" + NL + "REF O0, I0;" + NL
            + "RETURN;" + NL;
        check(sql, prg, true, true, false);
    }

    public void testBooleanExpressions()
    {
        //AND has higher precedence than OR
        String sql =
            "SELECT empno FROM emps WHERE true and not true or false and (not true and true)";
        String prg =
            "O s4;" + NL + "I s4;" + NL + "L bo, bo, bo, bo, bo;" + NL
            + "S bo;" + NL + "C bo, bo;" + NL + "V 1, 0;" + NL + "T;" + NL
            + "NOT L0, C0;" + NL + "AND L1, C0, L0;" + NL + "AND L2, L0, C0;"
            + NL + "AND L3, C1, L2;" + NL + "OR L4, L1, L3;" + NL
            + "JMPT @8, L4;" + NL + "MOVE S0, C0;" + NL + "RETURN;" + NL
            + "MOVE S0, C1;" + NL + "REF O0, I0;" + NL + "RETURN;" + NL;
        check(sql, prg, false, false, false);
    }

    public void testScalarExpression()
    {
        String sql = "SELECT 2-2*2+2/2-2  FROM emps WHERE empno > 10";
        String prg =
            "O s4;" + NL + "I s4;" + NL
            + "L bo, bo, bo, s4, s4, s4, s4, s4, bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4, s4, vc,5;" + NL + "V 1, 0, 10, 2, 0x3232303034;"
            + NL + "T;" + NL + "ISNOTNULL L0, I0;" + NL + "GT L1, I0, C2;"
            + NL + "AND L2, L0, L1;" + NL + "JMPT @6, L2;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "MUL L3, C3, C3;" + NL + "SUB L4, C3, L3;" + NL
            + "DIV L5, C3, C3;" + NL + "ADD L6, L4, L5;" + NL
            + "SUB L7, L6, C3;" + NL + "ISNULL L8, L7;" + NL + "JMPF @16, L8;"
            + NL + "RAISE C4;" + NL + "RETURN;" + NL + "REF O0, L7;" + NL
            + "RETURN;" + NL;
        check(sql, prg, true, false, false);
    }

    public void testMixedExpression()
    {
        String sql =
            "SELECT name, 2*2  FROM emps WHERE name = 'Fred' AND  empno > 10";
        String prg =
            "O vc,0, s4;" + NL + "I vc,0, s4;" + NL
            + "L bo, bo, s4, bo, bo, bo, bo, bo, s4, bo;" + NL + "S bo;" + NL
            + "C bo, bo, vc,8, vc,48, s4, s4, s4, vc,5;" + NL
            + "V 1, 0, 0x46726564, 0x49534F2D383835392D3124656E5F5553247072696D617279, 0, 10, 2, 0x3232303034;"
            + NL + "T;" + NL + "ISNOTNULL L0, I0;" + NL
            + "CALL 'strCmpA(L2, I0, C2);" + NL + "EQ L1, L2, C4;" + NL
            + "AND L3, L0, L1;" + NL + "ISNOTNULL L4, I1;" + NL
            + "GT L5, I1, C5;" + NL + "AND L6, L4, L5;" + NL
            + "AND L7, L3, L6;" + NL + "JMPT @11, L7;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "MUL L8, C6, C6;"
            + NL + "REF O0, I0;" + NL + "ISNULL L9, L8;" + NL
            + "JMPF @18, L9;" + NL + "RAISE C7;" + NL + "RETURN;" + NL
            + "REF O1, L8;" + NL + "RETURN;" + NL;
        check(sql, prg, true, false, false);
    }

    public void testNumbers()
    {
        String sql =
            "SELECT " + "-(1+-2.*-3.e-1/-.4)>=+5, " + " 1e200 / 0.4"
            + "FROM emps WHERE empno > 10";
        String prg =
            "O bo, d;" + NL + "I s4;" + NL
            + "L bo, s4, d, d, d, d, d, d, d, d, d, bo, d, bo;" + NL + "S bo;"
            + NL + "C bo, bo, s4, s4, s4, d, d, s4, d, vc,5;" + NL
            + "V 1, 0, 10, 1, 2, 3E-1, 4E-1, 5, 1.0000000000000000000E200, 0x3232303034;"
            + NL + "T;" + NL + "GT L0, I0, C2;" + NL // empno > 10
            + "JMPT @4, L0;" + NL + "MOVE S0, C0;" + NL + "RETURN;" + NL
            + "MOVE S0, C1;" + NL + "NEG L1, C4;" + NL // -2
            + "NEG L2, C5;" + NL // -0.3
            + "CAST L3, L1;" + NL // convert -2 to -2.0
            + "MUL L4, L3, L2;" + NL // -2.0 * -0.3
            + "NEG L5, C6;" + NL // -0.4
            + "DIV L6, L4, L5;" + NL // (-2.0 * 0.3) / -0.4
            + "CAST L7, C3;" + NL // convert 1 to 1.0
            + "ADD L8, L7, L6;" + NL // 1.0 + ((-2 * -0.3) / -0.4)
            + "NEG L9, L8;" + NL + "CAST L10, C7;" + NL + "GE L11, L9, L10;"
            + NL // x >= 5
            + "DIV L12, C8, C6;" + NL // 1e2 / 0.4
            + "REF O0, L11;" + NL + "ISNULL L13, L12;" + NL + "JMPF @22, L13;"
            + NL + "RAISE C9;" + NL + "RETURN;" + NL + "REF O1, L12;" + NL
            + "RETURN;" + NL;
        check(sql, prg, false, false, false);
    }

    public void testHexBitBinaryString()
    {
        String sql =
            "SELECT x'abc'=x'', b''=B'00111', X'0001'=x'FFeeDD' FROM emps WHERE empno > 10";
        String prg =
            "O bo, bo, bo;" + NL + "I s4;" + NL
            + "L bo, bo, s4, bo, s4, bo, s4;" + NL + "S bo;" + NL
            + "C bo, bo, s4, vb,2, vb,0, s4, vb,0, vb,1, vb,2, vb,3;" + NL
            + "V 1, 0, 10, 0x0ABC, 0x, 0, 0x, 0x07, 0x0001, 0xFFEEDD;" + NL
            + "T;" + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "CALL 'strCmpOct(L2, C3, C4);" + NL + "EQ L1, L2, C5;" + NL
            + "CALL 'strCmpOct(L4, C6, C7);" + NL + "EQ L3, L4, C5;" + NL
            + "CALL 'strCmpOct(L6, C8, C9);" + NL + "EQ L5, L6, C5;" + NL
            + "REF O0, L1;" + NL + "REF O1, L3;" + NL + "REF O2, L5;" + NL
            + "RETURN;";
        check(sql, prg, false, false, false);
    }

    public void testStringLiterals()
    {
        String sql =
            "SELECT n'aBc',_iso_8859-1'', 'abc' FROM emps WHERE empno > 10";
        String prg =
            "O vc,6, vc,0, vc,6;" + NL + "I s4;" + NL + "L bo, bo;" + NL
            + "S bo;" + NL + "C bo, bo, s4, vc,6, vc,0, vc,6, vc,5;" + NL
            + "V 1, 0, 10, 0x614263, 0x, 0x616263, 0x3232303034;" + NL + "T;"
            + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "ISNULL L1, C3;" + NL + "JMPF @9, L1;" + NL + "RAISE C6;" + NL
            + "RETURN;" + NL + "REF O0, C3;" + NL + "ISNULL L1, C4;" + NL
            + "JMPF @14, L1;" + NL + "RAISE C6;" + NL + "RETURN;" + NL
            + "REF O1, C4;" + NL + "ISNULL L1, C5;" + NL + "JMPF @19, L1;"
            + NL + "RAISE C6;" + NL + "RETURN;" + NL + "REF O2, C5;" + NL
            + "RETURN;";
        check(sql, prg, false, false, false);
    }

    public void testSimpleCompare()
    {
        String sql =
            "SELECT " + "1<>2" + ",1=2 is true is false is null is unknown"
            + ",true is not true " + ",true is not false"
            + ",true is not null " + ",true is not unknown"
            + " FROM emps WHERE empno > 10";
        String prg =
            "O bo, bo, bo, bo, bo, bo;" + NL + "I s4;" + NL
            + "L bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo, bo;"
            + NL + "S bo;" + NL + "C bo, bo, s4, s4, s4;" + NL
            + "V 1, 0, 10, 1, 2;" + NL + "T;" + NL + "GT L0, I0, C2;"
            + NL // empno > 10
            + "JMPT @4, L0;" + NL + "MOVE S0, C0;" + NL + "RETURN;" + NL
            + "MOVE S0, C1;" + NL + "NE L1, C3, C4;" + NL // 1 != 2
            + "EQ L2, C3, C4;" + NL // 1 = 2
            + "ISNOTNULL L3, L2;" + NL // X IS TRUE
            + "EQ L4, L2, C0;" + NL + "AND L5, L3, L4;" + NL
            + "ISNOTNULL L6, L5;" + NL // X IS FALSE
            + "EQ L7, L5, C1;" + NL + "AND L8, L6, L7;" + NL
            + "ISNULL L9, L8;" + NL // X IS NULL
            + "ISNULL L10, L9;" + NL // X IS UNKNOWN
            + "EQ L11, C0, C0;" + NL // TRUE IS NOT TRUE; TODO: optimize
            + "NOT L12, L11;" + NL + "EQ L13, C0, C1;" + NL // TODO: optimize
            + "NOT L14, L13;" + NL + "ISNOTNULL L15, C0;" + NL + "REF O0, L1;"
            + NL + "REF O1, L10;" + NL + "REF O2, L12;" + NL + "REF O3, L14;"
            + NL + "REF O4, L15;" + NL + "REF O5, L15;" + NL + "RETURN;" + NL;
        check(sql, prg, false, false, false);
    }

    public void testArithmeticOperators()
    {
        String sql =
            "SELECT POW(1.0,1.0), MOD(1,1), ABS(5000000000), ABS(1), "
            + "ABS(1.1), LN(1), LOG(1) FROM emps WHERE empno > 10";
        String prg =
            "O d, s4, s8, s4, d, d, d;" + NL + "I s4;" + NL
            + "L bo, d, s4, s8, s8, s8, s4, d, d, d, d, bo;" + NL + "S bo;"
            + NL + "C bo, bo, s4, d, s4, s8, d, vc,5;" + NL
            + "V 1, 0, 10, 1.0E0, 1, 5000000000, 1.1E0, 0x3232303034;" + NL
            + "T;" + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "CALL 'POW(L1, C3, C3);" + NL + "MOD L2, C4, C4;" + NL
            + "CALL 'ABS(L3, C5);" + NL + "CAST L4, C4;" + NL
            + "CALL 'ABS(L5, L4);" + NL + "CAST L6, L5;" + NL
            + "CALL 'ABS(L7, C6);" + NL + "CAST L8, C4;" + NL
            + "CALL 'LN(L9, L8);" + NL + "CALL 'LOG10(L10, L8);" + NL
            + "REF O0, L1;" + NL + "ISNULL L11, L2;" + NL + "JMPF @20, L11;"
            + NL + "RAISE C7;" + NL + "RETURN;" + NL + "REF O1, L2;" + NL
            + "ISNULL L11, L3;" + NL + "JMPF @25, L11;" + NL + "RAISE C7;"
            + NL + "RETURN;" + NL + "REF O2, L3;" + NL + "ISNULL L11, L6;"
            + NL + "JMPF @30, L11;" + NL + "RAISE C7;" + NL + "RETURN;" + NL
            + "REF O3, L6;" + NL + "ISNULL L11, L7;" + NL + "JMPF @35, L11;"
            + NL + "RAISE C7;" + NL + "RETURN;" + NL + "REF O4, L7;" + NL
            + "REF O5, L9;" + NL + "REF O6, L10;" + NL + "RETURN;";
        check(sql, prg, false, false, false);
    }

    public void testFunctionInFunction()
    {
        String sql = "SELECT POW(3.0, ABS(2)+1) FROM emps WHERE empno > 10";
        String prg =
            "O d;" + NL + "I s4;" + NL + "L bo, s8, s8, s4, s4, d, d;" + NL
            + "S bo;" + NL + "C bo, bo, s4, d, s4, s4;" + NL
            + "V 1, 0, 10, 3.0E0, 2, 1;" + NL + "T;" + NL + "GT L0, I0, C2;"
            + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;" + NL + "RETURN;" + NL
            + "MOVE S0, C1;" + NL + "CAST L1, C4;" + NL + "CALL 'ABS(L2, L1);"
            + NL + "CAST L3, L2;" + NL + "ADD L4, L3, C5;" + NL
            + "CAST L5, L4;" + NL + "CALL 'POW(L6, C3, L5);" + NL
            + "REF O0, L6;" + NL + "RETURN;" + NL;
        check(sql, prg, false, false, false);
    }

    public void testCaseExpressions()
    {
        String sql =
            "SELECT case 1+1 when 1 then 'wael' when 2 then 'waels clone' end"
            + ",case when 1=1 then 1+1+2 else 2+10 end"
            + " FROM emps WHERE empno > 10";
        String prg =
            "O vc,22, s4;" + NL + "I s4;" + NL
            + "L bo, vc,22, s4, bo, bo, s4, bo, s4, s4, bo;" + NL + "S bo;"
            + NL + "C bo, bo, s4, s4, vc,8, s4, vc,22, vc,0, vc,5;" + NL
            + "V 1, 0, 10, 1, 0x7761656C, 2, 0x7761656C7320636C6F6E65, , 0x3232303034;"
            + NL + "T;" + NL + "GT L0, I0, C2;" + NL // empno > 10
            + "JMPT @4, L0;" + NL + "MOVE S0, C0;" + NL + "RETURN;" + NL
            + "MOVE S0, C1;" + NL + "ADD L2, C3, C3;" + NL // 1 + 1
            + "EQ L3, L2, C3;" + NL // WHEN 1 + 1 = 1
            + "JMPF @11, L3;" + NL + "JMPN @11, L3;" + NL + "MOVE L1, C4;"
            + NL // THEN
            + "JMP @17;" + NL + "EQ L4, L2, C5;" + NL // WHEN 1 + 1 = 2
            + "JMPF @16, L4;" + NL + "JMPN @16, L4;" + NL + "MOVE L1, C6;"
            + NL // THEN
            + "JMP @17;" + NL + "MOVE L1, C7;" + NL // ELSE NULL
            + "EQ L6, C3, C3;" + NL // SECOND CASE 1 = 1
            + "JMPF @23, L6;" + NL + "JMPN @23, L6;" + NL + "ADD L7, L2, C5;"
            + NL // 1 + 1 + 2
            + "MOVE L5, L7;" + NL + "JMP @25;" + NL + "ADD L8, C5, C2;"
            + NL // ELSE 2 + 10
            + "MOVE L5, L8;" + NL + "REF O0, L1;" + NL + "ISNULL L9, L5;" + NL
            + "JMPF @30, L9;" + NL + "RAISE C8;" + NL + "RETURN;" + NL
            + "REF O1, L5;" + NL + "RETURN;" + NL;
        check(sql, prg, false, false, false);
    }

    public void testNullifExpression()
    {
        String sql = "SELECT nullif(1,2) " + " FROM emps WHERE empno > 10";
        String prg =
            "O s4;" + NL + "I s4;" + NL + "L bo, s4, bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4, s4, s4, s4;" + NL + "V 1, 0, 10, 1, 2, ;" + NL
            + "T;" + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "EQ L2, C3, C4;" + NL + "JMPF @10, L2;" + NL + "JMPN @10, L2;"
            + NL + "MOVE L1, C5;" + NL + "JMP @11;" + NL + "MOVE L1, C3;" + NL
            + "REF O0, L1;" + NL + "RETURN;";

        check(sql, prg, false, false, false);
    }

    public void testCoalesce()
    {
        String sql = "SELECT coalesce(1,2,3) FROM emps WHERE empno > 10";

        //CASE WHEN 1 IS NOT NULL THEN 1 ELSE (CASE WHEN 2 IS NOT NULL THEN 2 ELSE 3) END
        String prg =
            "O s4;" + NL + "I s4;" + NL + "L bo, s4, bo, s4, bo, bo;" + NL
            + "S bo;" + NL + "C bo, bo, s4, s4, s4, s4, vc,5;" + NL
            + "V 1, 0, 10, 1, 2, 3, 0x3232303034;" + NL + "T;" + NL
            + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "ISNOTNULL L2, C3;"
            + NL + "JMPF @10, L2;" + NL + "JMPN @10, L2;" + NL
            + "MOVE L1, C3;" + NL + "JMP @17;" + NL + "ISNOTNULL L4, C4;" + NL
            + "JMPF @15, L4;" + NL + "JMPN @15, L4;" + NL + "MOVE L3, C4;"
            + NL + "JMP @16;" + NL + "MOVE L3, C5;" + NL + "MOVE L1, L3;" + NL
            + "ISNULL L5, L1;" + NL + "JMPF @21, L5;" + NL + "RAISE C6;" + NL
            + "RETURN;" + NL + "REF O0, L1;" + NL + "RETURN;" + NL;
        check(sql, prg, false, false, false);
    }

    public void testCase()
    {
        String sql =
            "SELECT CASE WHEN TRUE THEN 0 ELSE 1 END "
            + "FROM emps WHERE empno > 10";
        String prg =
            "O s4;" + NL + "I s4;" + NL + "L bo, s4, bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4, s4, vc,5;" + NL + "V 1, 0, 10, 0, 0x3232303034;"
            + NL + "T;" + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "MOVE L1, C3;" + NL + "ISNULL L2, L1;" + NL + "JMPF @10, L2;"
            + NL + "RAISE C4;" + NL + "RETURN;" + NL + "REF O0, L1;" + NL
            + "RETURN;";
        check(sql, prg, false, false, false);

        String sql1 =
            "SELECT CASE WHEN slacker then 2 when TRUE THEN 0 ELSE 1 END "
            + "FROM emps WHERE empno > 10";
        String prg1 =
            "O s4;" + NL + "I s4, bo;" + NL + "L bo, s4, bo;" + NL + "S bo;"
            + NL + "C bo, bo, s4, s4, s4, vc,5;" + NL
            + "V 1, 0, 10, 2, 0, 0x3232303034;" + NL + "T;" + NL
            + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "JMPF @9, I1;" + NL
            + "JMPN @9, I1;" + NL + "MOVE L1, C3;" + NL + "JMP @11;" + NL
            + "MOVE L1, C4;" + NL + "JMP @11;" + NL + "ISNULL L2, L1;" + NL
            + "JMPF @15, L2;" + NL + "RAISE C5;" + NL + "RETURN;" + NL
            + "REF O0, L1;" + NL + "RETURN;";
        check(sql1, prg1, false, false, false);
        String sql2 =
            "select CASE 1 WHEN 1 THEN cast(null as integer) else 0 END "
            + "FROM emps WHERE empno > 10";
        String prg2 =
            "O s4;" + NL + "I s4;" + NL + "L bo, s4, bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4, s4, s4, s4;" + NL + "V 1, 0, 10, 1, , 0;" + NL
            + "T;" + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "EQ L2, C3, C3;" + NL + "JMPF @10, L2;" + NL + "JMPN @10, L2;"
            + NL + "MOVE L1, C4;" + NL + "JMP @11;" + NL + "MOVE L1, C5;" + NL
            + "REF O0, L1;" + NL + "RETURN;";
        check(sql2, prg2, false, false, false);
    }

    public void testCharEq()
    {
        checkCharOp("=", "EQ");
    }

    public void testCharNe()
    {
        checkCharOp("<>", "NE");
    }

    public void testCharGt()
    {
        checkCharOp(">", "GT");
    }

    public void testCharLt()
    {
        checkCharOp("<", "LT");
    }

    public void testCharGe()
    {
        checkCharOp(">=", "GE");
    }

    public void testCharLe()
    {
        checkCharOp("<=", "LE");
    }

    private void checkCharOp(
        final String op,
        final String instr)
    {
        String sql =
            "SELECT 'a' " + op
            + "'b' collate latin1$sv$1 FROM emps WHERE empno > 10";
        String prg =
            "O bo;" + NL + "I s4;" + NL + "L bo, bo, s4;" + NL + "S bo;" + NL
            + "C bo, bo, s4, vc,2, vc,2, vc,30, s4;" + NL
            + "V 1, 0, 10, 0x61, 0x62, 0x49534F2D383835392D312473762431, 0;"
            + NL + "T;" + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "CALL 'strCmpA(L2, C3, C4);" + NL + instr + " L1, L2, C6;" + NL
            + "REF O0, L1;" + NL + "RETURN;" + NL;
        check(sql, prg, false, false, false);
    }

    public void testBinaryGt()
    {
        checkBinaryOp(">", "GT");
    }

    private void checkBinaryOp(
        final String op,
        final String instr)
    {
        String sql = "SELECT x'ff' " + op + "x'01' FROM emps WHERE empno > 10";
        String prg =
            "O bo;" + NL + "I s4;" + NL + "L bo, bo, s4;" + NL + "S bo;" + NL
            + "C bo, bo, s4, vb,1, vb,1, s4;" + NL
            + "V 1, 0, 10, 0xFF, 0x01, 0;" + NL + "T;" + NL + "GT L0, I0, C2;"
            + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;" + NL + "RETURN;" + NL
            + "MOVE S0, C1;" + NL + "CALL 'strCmpOct(L2, C3, C4);" + NL
            + instr + " L1, L2, C5;" + NL + "REF O0, L1;" + NL + "RETURN;"
            + NL;
        check(sql, prg, false, false, false);
    }

    public void testStringFunctions()
    {
        String sql =
            "SELECT " + "char_length('a')," + "upper('a')," + "lower('a'),"
            + "position('a' in 'a')," + "trim('a' from 'a'),"
            + "overlay('a' placing 'a' from 1)," + "substring('a' from 1),"
            + 
            //                "substring(cast('a' as char(2)) from 1)," + todo uncomment this once cast char to varchar works
            "substring('a' from 1 for 10),"
            + "substring('a' from 'a' for '\\' )," + "'a'||'a'||'b'"
            + " FROM emps WHERE empno > 10";
        String prg =
            "O s4, vc,2, vc,2, s4, vc,2, vc,4, vc,2, vc,2, vc,2, vc,6;" + NL
            + "I s4;" + NL
            + "L bo, s4, vc,2, vc,2, s4, vc,2, vc,4, vc,2, vc,2, vc,2, vc,6, bo;"
            + NL + "S bo;" + NL
            + "C bo, bo, s4, vc,2, s4, s4, vc,2, vc,2, vc,5;" + NL
            + "V 1, 0, 10, 0x61, 1, 1, 0x5C, 0x62, 0x3232303034;" + NL + "T;"
            + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "CALL 'strLenCharA(L1, C3);" + NL + "CALL 'strToUpperA(L2, C3);"
            + NL + "CALL 'strToLowerA(L3, C3);" + NL
            + "CALL 'strPosA(L4, C3, C3);" + NL
            + "CALL 'strTrimA(L5, C3, C3, C4, C4);" + NL
            + "CALL 'strOverlayA4(L6, C3, C3, C5);" + NL
            + "CALL 'strSubStringA3(L7, C3, C5);" + NL
            + "CALL 'strSubStringA4(L8, C3, C5, C2);" + NL
            + "CALL 'strSubStringA4(L9, C3, C3, C6);" + NL
            + "CALL 'strCatA3(L10, C3, C3);" + NL + "CALL 'strCatA2(L10, C7);"
            + NL + "REF O0, L1;" + NL + "ISNULL L11, L2;" + NL
            + "JMPF @21, L11;" + NL + "RAISE C8;" + NL + "RETURN;" + NL
            + "REF O1, L2;" + NL + "ISNULL L11, L3;" + NL + "JMPF @26, L11;"
            + NL + "RAISE C8;" + NL + "RETURN;" + NL + "REF O2, L3;" + NL
            + "REF O3, L4;" + NL + "ISNULL L11, L5;" + NL + "JMPF @32, L11;"
            + NL + "RAISE C8;" + NL + "RETURN;" + NL + "REF O4, L5;" + NL
            + "REF O5, L6;" + NL + "ISNULL L11, L7;" + NL + "JMPF @38, L11;"
            + NL + "RAISE C8;" + NL + "RETURN;" + NL + "REF O6, L7;" + NL
            + "ISNULL L11, L8;" + NL + "JMPF @43, L11;" + NL + "RAISE C8;"
            + NL + "RETURN;" + NL + "REF O7, L8;" + NL + "ISNULL L11, L9;"
            + NL + "JMPF @48, L11;" + NL + "RAISE C8;" + NL + "RETURN;" + NL
            + "REF O8, L9;" + NL + "REF O9, L10;" + NL + "RETURN;" + NL;
        check(sql, prg, false, false, false);
    }

    public void testLikeAndSimilar()
    {
        String sql =
            "SELECT " + "'a' like 'b'" + ",'a' like 'b' escape 'c'"
            + ",'a' similar to 'a'" + ",'a' similar to 'a' escape 'c'"
            + " FROM emps WHERE empno > 10";
        String prg =
            "O bo, bo, bo, bo;" + NL + "I s4;" + NL + "L bo, bo, bo, bo, bo;"
            + NL + "S bo;" + NL + "C bo, bo, s4, vc,2, vc,2, vc,2;" + NL
            + "V 1, 0, 10, 0x61, 0x62, 0x63;" + NL + "T;" + NL
            + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "CALL 'strLikeA3(L1, C3, C4);" + NL
            + "CALL 'strLikeA4(L2, C3, C4, C5);" + NL
            + "CALL 'strSimilarA3(L3, C3, C3);" + NL
            + "CALL 'strSimilarA4(L4, C3, C3, C5);" + NL + "REF O0, L1;" + NL
            + "REF O1, L2;" + NL + "REF O2, L3;" + NL + "REF O3, L4;" + NL
            + "RETURN;";
        check(sql, prg, false, false, false);
    }

    public void testBetween()
    {
        String sql1 = "select empno  from emps where empno between 40 and 60";
        String sql2 =
            "select empno  from emps where empno between asymmetric 40 and 60";
        String prg1 =
            "O s4;" + NL + "I s4;" + NL + "L bo, bo, bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4, s4;" + NL + "V 1, 0, 40, 60;" + NL + "T;" + NL
            + "GE L0, I0, C2;" + NL + "LE L1, I0, C3;" + NL
            + "AND L2, L0, L1;" + NL + "JMPT @6, L2;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "REF O0, I0;" + NL
            + "RETURN;";
        check(sql1, prg1, false, false, false);
        check(sql2, prg1, false, false, false);
        String sql3 =
            "select empno  from emps where empno not between 40 and 60";
        String prg2 =
            "O s4;" + NL + "I s4;" + NL + "L bo, bo, bo, bo;" + NL + "S bo;"
            + NL + "C bo, bo, s4, s4;" + NL + "V 1, 0, 40, 60;" + NL + "T;"
            + NL + "GE L0, I0, C2;" + NL + "LE L1, I0, C3;" + NL
            + "AND L2, L0, L1;" + NL + "NOT L3, L2;" + NL + "JMPT @7, L3;"
            + NL + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "REF O0, I0;" + NL + "RETURN;";
        check(sql3, prg2, false, false, false);
        String sql4 =
            "select empno  from emps where empno between symmetric 40 and 60";
        String prg3 =
            "O s4;" + NL + "I s4;" + NL + "L bo, bo, bo, bo, bo, bo, bo;" + NL
            + "S bo;" + NL + "C bo, bo, s4, s4;" + NL + "V 1, 0, 40, 60;" + NL
            + "T;" + NL + "GE L0, I0, C2;" + NL + "LE L1, I0, C3;" + NL
            + "AND L2, L0, L1;" + NL + "GE L3, I0, C3;" + NL
            + "LE L4, I0, C2;" + NL + "AND L5, L3, L4;" + NL
            + "OR L6, L2, L5;" + NL + "JMPT @10, L6;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "REF O0, I0;" + NL
            + "RETURN;";
        check(sql4, prg3, false, false, false);
    }

    public void testCastNull()
    {
        String sql =
            "SELECT " + "cast(null as varchar)" + ", cast(null as integer)"
            + " FROM emps WHERE empno > 10";
        String prg =
            "O vc,0, s4;" + NL + "I s4;" + NL + "L bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4, vc,0, s4;" + NL + "V 1, 0, 10, , ;" + NL + "T;"
            + NL + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL
            + "MOVE S0, C0;" + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL
            + "REF O0, C3;" + NL + "REF O1, C4;" + NL + "RETURN;";
        check(sql, prg, false, false, false);
    }

    public void testJdbcFunctionSyntax()
    {
        String sql =
            "SELECT " + "{fn log(1.0)}" + " FROM emps WHERE empno > 10";
        String prg =
            "O d;" + NL + "I s4;" + NL + "L bo, d;" + NL + "S bo;" + NL
            + "C bo, bo, s4, d;" + NL + "V 1, 0, 10, 1.0E0;" + NL + "T;" + NL
            + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "CALL 'LN(L1, C3);"
            + NL + "REF O0, L1;" + NL + "RETURN;";
        check(sql, prg, false, false, false);
    }

    public void testMixingTypes()
    {
        String sql = "SELECT " + "1+1.0" + " FROM emps WHERE empno > 10";
        String prg =
            "O d;" + NL + "I s4;" + NL + "L bo, d, d, bo;" + NL + "S bo;" + NL
            + "C bo, bo, s4, s4, d, vc,5;" + NL
            + "V 1, 0, 10, 1, 1.0E0, 0x3232303034;" + NL + "T;" + NL
            + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "CAST L1, C3;" + NL
            + "ADD L2, L1, C4;" + NL + "ISNULL L3, L2;" + NL + "JMPF @11, L3;"
            + NL + "RAISE C5;" + NL + "RETURN;" + NL + "REF O0, L2;" + NL
            + "RETURN;";
        check(sql, prg, false, false, false);
    }

    public void testCastCharTypesToNumbersAndBack()
    {
        String sql =
            "SELECT " + "cast(123 as varchar(3))" + ",cast(123 as char(3))"
            + ",cast(12.3 as varchar(3))" + ",cast(12.3 as char(3))"
            + ",cast('123' as bigint)" + ",cast('123' as tinyint)"
            + ",cast('123' as double)" + " FROM emps WHERE empno > 10";
        String prg =
            "O vc,6, c,6, vc,6, c,6, s8, s1, d;" + NL + "I s4;" + NL
            + "L bo, s8, vc,6, c,6, vc,6, c,6, s8, s1, d;" + NL + "S bo;" + NL
            + "C bo, bo, s4, s4, d, vc,6;" + NL
            + "V 1, 0, 10, 123, 1.23E1, 0x313233;" + NL + "T;" + NL
            + "GT L0, I0, C2;" + NL + "JMPT @4, L0;" + NL + "MOVE S0, C0;"
            + NL + "RETURN;" + NL + "MOVE S0, C1;" + NL + "CAST L1, C3;" + NL
            + "CALL 'castA(L2, L1);" + NL + "CALL 'castA(L3, L1);" + NL
            + "CALL 'castA(L4, C4);" + NL + "CALL 'castA(L5, C4);" + NL
            + "CALL 'castA(L6, C5);" + NL + "CAST L7, L6;" + NL
            + "CALL 'castA(L8, C5);" + NL + "REF O0, L2;" + NL + "REF O1, L3;"
            + NL + "REF O2, L4;" + NL + "REF O3, L5;" + NL + "REF O4, L6;"
            + NL + "REF O5, L7;" + NL + "REF O6, L8;" + NL + "RETURN;";
        check(sql, prg, false, false, false);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Contains context shared between unit tests.
     *
     * <p>Lots of nasty stuff to set up the Openjava environment, should be
     * removed when we're not dependent upon Openjava.</p>
     */
    static class TestContext
    {
        private FarragoSessionStmtValidator stmtValidator;
        private FarragoPreparingStmt stmt;
        Environment env;
        private int executionCount;

        TestContext()
        {
            // Nasty OJ stuff
            env = OJSystem.env;

            // DynamicJava's compiler contains a class loader, so it was
            // important that compiler and class map had same life-cycle. We no
            // longer use DynamicJava, so it may not matter anymore.
            if (ClassMap.instance() == null) {
                ClassMap.setInstance(new ClassMap(SyntheticObject.class));
            }
            String packageName = getTempPackageName();
            String className = getTempClassName();
            env = new FileEnvironment(env, packageName, className);
            ClassDeclaration decl =
                new ClassDeclaration(
                    new ModifierList(ModifierList.PUBLIC),
                    className,
                    null,
                    null,
                    new MemberDeclarationList());
            OJClass clazz = new OJClass(env, null, decl);
            env.record(
                clazz.getName(),
                clazz);
            env = new ClosedEnvironment(clazz.getEnvironment());
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
            return SaffronProperties.instance().javaDir.get(getClassRoot());
        }

        protected String getTempPackageName()
        {
            return SaffronProperties.instance().packageName.get();
        }
    }
}


// End Rex2CalcPlanTest.java
