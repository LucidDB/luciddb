/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.saffron.test;

import junit.framework.TestCase;
import net.sf.saffron.rex.RexBuilder;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexInputRef;
import net.sf.saffron.rex.RexLiteral;
import net.sf.saffron.oj.util.JavaRexBuilder;
import net.sf.saffron.oj.OJTypeFactoryImpl;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.core.SaffronTypeFactoryImpl;
import net.sf.saffron.opt.RexTransformer;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.sql.SqlOperatorTable;

/**
 * Validates that rex expressions gets correctly translated to a correct calculator program
 *
 * @author wael
 * @since Mar 9, 2004
 * @version $Id$
 **/


public class RexTransformerTest extends TestCase
{
    RexBuilder rexBuilder = null;
    SqlOperatorTable opTab = SqlOperatorTable.instance();
    RexNode x;
    RexNode y;
    RexNode z;
    RexNode trueRex;
    RexNode falseRex;
    SaffronTypeFactory typeFactory;

    protected void setUp() throws Exception {
//        typeFactory = new SaffronTypeFactoryImpl();
        typeFactory = new OJTypeFactoryImpl();
        rexBuilder = new JavaRexBuilder(typeFactory);

        x = new RexInputRef(0, typeFactory.createSqlType(SqlTypeName.Boolean));
        y = new RexInputRef(1, typeFactory.createSqlType(SqlTypeName.Boolean));
        z = new RexInputRef(2, typeFactory.createSqlType(SqlTypeName.Boolean));
        trueRex = rexBuilder.makeLiteral(true);
        falseRex = rexBuilder.makeLiteral(false);
    }

    void check(RexNode node, String expected) {
        RexTransformer transformer = new RexTransformer(node, rexBuilder);
        RexNode result = transformer.tranformNullSemantics();
        String actual = result.toString();
        if (!actual.equals(expected)){
            String msg = "\nExpected=<"+expected+">\n  Actual=<"+actual+">";
            fail(msg);
        }
    }

    //~ tests ----------------------------------------------------------------------

    public void testPreTests() {
        //can make variable nullable?
        RexNode node = new RexInputRef(0, typeFactory.createTypeWithNullability(
                                          typeFactory.createSqlType(SqlTypeName.Boolean), true));
        assertTrue(node.getType().isNullable());

        //can make variable not nullable?
        node = new RexInputRef(0, typeFactory.createTypeWithNullability(
                                  typeFactory.createSqlType(SqlTypeName.Boolean), false));
        assertFalse(node.getType().isNullable());
    }

    public void testNonBooleans() {
        RexNode node = rexBuilder.makeCall(opTab.plusOperator, x, y);
        String expected = node.toString();
        check(node, expected);
    }

    /**
     * the or operator should pass through unchanged since e.g. x OR y should return true if x=null and y=true
     * if it was transformed into something like (x ISNOTNULL) AND (y ISNOTNULL) AND (x OR y)
     * an incorrect result could be produced
     */
    public void testOrUnchanged() {
        RexNode node = rexBuilder.makeCall(opTab.orOperator, x, y);
        String expected = node.toString();
        check(node, expected);
    }

    public void testSimpleAnd() {
        RexNode node = rexBuilder.makeCall(opTab.andOperator, x, y);
        check(node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), AND($0, $1))");
    }

    public void testSimpleEquals() {
        RexNode node = rexBuilder.makeCall(opTab.equalsOperator, x, y);
        check(node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1))");
    }

    public void testSimpleNotEquals() {
        RexNode node = rexBuilder.makeCall(opTab.notEqualsOperator, x, y);
        check(node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <>($0, $1))");
    }

    public void testSimpleGreaterThan() {
        RexNode node = rexBuilder.makeCall(opTab.greaterThanOperator, x, y);
        check(node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >($0, $1))");
    }

    public void testSimpleGreaterEquals() {
        RexNode node = rexBuilder.makeCall(opTab.greaterThanOrEqualOperator, x, y);
        check(node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >=($0, $1))");
    }

    public void testSimpleLessThan() {
        RexNode node = rexBuilder.makeCall(opTab.lessThanOperator, x, y);
        check(node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <($0, $1))");
    }

    public void testSimpleLessEqual() {
        RexNode node = rexBuilder.makeCall(opTab.lessThanOrEqualOperator, x, y);
        check(node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <=($0, $1))");
    }

    public void testOptimizeNonNullLiterals() {
        RexNode node = rexBuilder.makeCall(opTab.lessThanOrEqualOperator, x, trueRex);
        check(node, "AND(IS NOT NULL($0), <=($0, true))");
        node = rexBuilder.makeCall(opTab.lessThanOrEqualOperator, trueRex, x);
        check(node, "AND(IS NOT NULL($0), <=(true, $0))");
    }

    public void testMixed1() {
        //x=true AND y
        RexNode op1 = rexBuilder.makeCall(opTab.equalsOperator, x, trueRex);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, y);
        check(and, "AND(IS NOT NULL($1), AND(AND(IS NOT NULL($0), =($0, true)), $1))");
    }

    public void testMixed2() {
        //x!=true AND y>z
        RexNode op1 = rexBuilder.makeCall(opTab.notEqualsOperator, x, trueRex);
        RexNode op2 = rexBuilder.makeCall(opTab.greaterThanOperator, y, z);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, op2);
        check(and, "AND(AND(IS NOT NULL($0), <>($0, true)), AND(AND(IS NOT NULL($1), IS NOT NULL($2)), >($1, $2)))");
    }

    public void testMixed3() {
        //x=y AND false>z
        RexNode op1 = rexBuilder.makeCall(opTab.equalsOperator, x, y);
        RexNode op2 = rexBuilder.makeCall(opTab.greaterThanOperator, falseRex, z);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, op2);
        check(and, "AND(AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1)), AND(IS NOT NULL($2), >(false, $2)))");
    }
}

// End RexTransformerTest.java