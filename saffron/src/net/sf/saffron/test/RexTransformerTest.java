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
import net.sf.saffron.rex.*;
import net.sf.saffron.oj.util.JavaRexBuilder;
import net.sf.saffron.oj.OJTypeFactoryImpl;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.rex.RexTransformer;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.sql.SqlOperatorTable;
import net.sf.saffron.sql.fun.SqlStdOperatorTable;

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
    SqlStdOperatorTable opTab = SqlOperatorTable.std();
    RexNode x;
    RexNode y;
    RexNode z;
    RexNode trueRex;
    RexNode falseRex;
    SaffronType boolSaffronType;
    SaffronTypeFactory typeFactory;

    protected void setUp() throws Exception {
//        typeFactory = new SaffronTypeFactoryImpl();
        typeFactory = new OJTypeFactoryImpl();
        rexBuilder = new JavaRexBuilder(typeFactory);
        boolSaffronType = typeFactory.createSqlType(SqlTypeName.Boolean);

        x = new RexInputRef(0, typeFactory.createTypeWithNullability(
                               boolSaffronType, true));
        y = new RexInputRef(1, typeFactory.createTypeWithNullability(
                               boolSaffronType, true));
        z = new RexInputRef(2, typeFactory.createTypeWithNullability(
                               boolSaffronType, true));
        trueRex = rexBuilder.makeLiteral(true);
        falseRex = rexBuilder.makeLiteral(false);
    }

    void check(Boolean encapsulateType, RexNode node, String expected) {
        RexNode root;
        if (null==encapsulateType) {
            root=node;
        } else if (encapsulateType.equals(Boolean.TRUE)) {
            root = rexBuilder.makeCall(opTab.isTrueOperator, node);
        } else { //if (encapsulateType.equals(Boolean.FALSE))
            root = rexBuilder.makeCall(opTab.isFalseOperator, node);
        }

        RexTransformer transformer = new RexTransformer(root, rexBuilder);
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
        check(Boolean.TRUE, node, expected);
        check(Boolean.FALSE, node, expected);
        check(null, node, expected);
    }

    /**
     * the or operator should pass through unchanged since e.g. x OR y should return true if x=null and y=true
     * if it was transformed into something like (x ISNOTNULL) AND (y ISNOTNULL) AND (x OR y)
     * an incorrect result could be produced
     */
    public void testOrUnchanged() {
        RexNode node = rexBuilder.makeCall(opTab.orOperator, x, y);
        String expected = node.toString();
        check(Boolean.TRUE,node, expected);
        check(Boolean.FALSE,node, expected);
        check(null,node, expected);
    }

    public void testSimpleAnd() {
        RexNode node = rexBuilder.makeCall(opTab.andOperator, x, y);
        check(Boolean.FALSE,node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), AND($0, $1))");
    }

    public void testSimpleEquals() {
        RexNode node = rexBuilder.makeCall(opTab.equalsOperator, x, y);
        check(Boolean.TRUE,node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1))");
    }

    public void testSimpleNotEquals() {
        RexNode node = rexBuilder.makeCall(opTab.notEqualsOperator, x, y);
        check(Boolean.FALSE,node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <>($0, $1))");
    }

    public void testSimpleGreaterThan() {
        RexNode node = rexBuilder.makeCall(opTab.greaterThanOperator, x, y);
        check(Boolean.TRUE,node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >($0, $1))");
    }

    public void testSimpleGreaterEquals() {
        RexNode node = rexBuilder.makeCall(opTab.greaterThanOrEqualOperator, x, y);
        check(Boolean.FALSE,node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >=($0, $1))");
    }

    public void testSimpleLessThan() {
        RexNode node = rexBuilder.makeCall(opTab.lessThanOperator, x, y);
        check(Boolean.TRUE,node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <($0, $1))");
    }

    public void testSimpleLessEqual() {
        RexNode node = rexBuilder.makeCall(opTab.lessThanOrEqualOperator, x, y);
        check(Boolean.FALSE,node, "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <=($0, $1))");
    }

    public void testOptimizeNonNullLiterals() {
        RexNode node = rexBuilder.makeCall(opTab.lessThanOrEqualOperator, x, trueRex);
        check(Boolean.TRUE,node, "AND(IS NOT NULL($0), <=($0, true))");
        node = rexBuilder.makeCall(opTab.lessThanOrEqualOperator, trueRex, x);
        check(Boolean.FALSE,node, "AND(IS NOT NULL($0), <=(true, $0))");
    }

    public void testSimpleIdentifier() {
        RexNode node= rexBuilder.makeInputRef(boolSaffronType, 0);
        check(Boolean.TRUE,node,"AND(IS NOT NULL($0), =($0, true))");
    }

    public void testMixed1() {
        //x=true AND y
        RexNode op1 = rexBuilder.makeCall(opTab.equalsOperator, x, trueRex);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, y);
        check(Boolean.FALSE,and, "AND(IS NOT NULL($1), AND(AND(IS NOT NULL($0), =($0, true)), $1))");
    }

    public void testMixed2() {
        //x!=true AND y>z
        RexNode op1 = rexBuilder.makeCall(opTab.notEqualsOperator, x, trueRex);
        RexNode op2 = rexBuilder.makeCall(opTab.greaterThanOperator, y, z);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, op2);
        check(Boolean.FALSE,and, "AND(AND(IS NOT NULL($0), <>($0, true)), AND(AND(IS NOT NULL($1), IS NOT NULL($2)), >($1, $2)))");
    }

    public void testMixed3() {
        //x=y AND false>z
        RexNode op1 = rexBuilder.makeCall(opTab.equalsOperator, x, y);
        RexNode op2 = rexBuilder.makeCall(opTab.greaterThanOperator, falseRex, z);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, op2);
        check(Boolean.TRUE,and, "AND(AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1)), AND(IS NOT NULL($2), >(false, $2)))");
    }
}

// End RexTransformerTest.java