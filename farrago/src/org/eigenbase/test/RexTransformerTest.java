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

import junit.framework.TestCase;

import org.eigenbase.oj.OJTypeFactoryImpl;
import org.eigenbase.oj.util.JavaRexBuilder;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.*;
import org.eigenbase.rex.RexTransformer;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;

// FIXME jvs 16-Dec-2004:  class comment below was wrongly copied and
// pasted!

/**
 * Validates that rex expressions gets correctly translated to a correct calculator program
 *
 * @author wael
 * @since Mar 9, 2004
 * @version $Id$
 **/
public class RexTransformerTest extends TestCase
{
    //~ Instance fields -------------------------------------------------------

    RexBuilder rexBuilder = null;
    SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();
    RexNode x;
    RexNode y;
    RexNode z;
    RexNode trueRex;
    RexNode falseRex;
    RelDataType boolRelDataType;
    RelDataTypeFactory typeFactory;

    //~ Methods ---------------------------------------------------------------

    protected void setUp()
        throws Exception
    {
        typeFactory = new OJTypeFactoryImpl();
        rexBuilder = new JavaRexBuilder(typeFactory);
        boolRelDataType = typeFactory.createSqlType(SqlTypeName.Boolean);

        x = new RexInputRef(
                0,
                typeFactory.createTypeWithNullability(boolRelDataType, true));
        y = new RexInputRef(
                1,
                typeFactory.createTypeWithNullability(boolRelDataType, true));
        z = new RexInputRef(
                2,
                typeFactory.createTypeWithNullability(boolRelDataType, true));
        trueRex = rexBuilder.makeLiteral(true);
        falseRex = rexBuilder.makeLiteral(false);
    }

    void check(
        Boolean encapsulateType,
        RexNode node,
        String expected)
    {
        RexNode root;
        if (null == encapsulateType) {
            root = node;
        } else if (encapsulateType.equals(Boolean.TRUE)) {
            root = rexBuilder.makeCall(opTab.isTrueOperator, node);
        } else { //if (encapsulateType.equals(Boolean.FALSE))
            root = rexBuilder.makeCall(opTab.isFalseOperator, node);
        }

        RexTransformer transformer = new RexTransformer(root, rexBuilder);
        RexNode result = transformer.tranformNullSemantics();
        String actual = result.toString();
        if (!actual.equals(expected)) {
            String msg =
                "\nExpected=<" + expected + ">\n  Actual=<" + actual + ">";
            fail(msg);
        }
    }

    public void testPreTests()
    {
        //can make variable nullable?
        RexNode node =
            new RexInputRef(0,
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.Boolean),
                    true));
        assertTrue(node.getType().isNullable());

        //can make variable not nullable?
        node =
            new RexInputRef(
                0,
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.Boolean),
                    false));
        assertFalse(node.getType().isNullable());
    }

    public void testNonBooleans()
    {
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
    public void testOrUnchanged()
    {
        RexNode node = rexBuilder.makeCall(opTab.orOperator, x, y);
        String expected = node.toString();
        check(Boolean.TRUE, node, expected);
        check(Boolean.FALSE, node, expected);
        check(null, node, expected);
    }

    public void testSimpleAnd()
    {
        RexNode node = rexBuilder.makeCall(opTab.andOperator, x, y);
        check(Boolean.FALSE, node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), AND($0, $1))");
    }

    public void testSimpleEquals()
    {
        RexNode node = rexBuilder.makeCall(opTab.equalsOperator, x, y);
        check(Boolean.TRUE, node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1))");
    }

    public void testSimpleNotEquals()
    {
        RexNode node = rexBuilder.makeCall(opTab.notEqualsOperator, x, y);
        check(Boolean.FALSE, node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <>($0, $1))");
    }

    public void testSimpleGreaterThan()
    {
        RexNode node = rexBuilder.makeCall(opTab.greaterThanOperator, x, y);
        check(Boolean.TRUE, node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >($0, $1))");
    }

    public void testSimpleGreaterEquals()
    {
        RexNode node =
            rexBuilder.makeCall(opTab.greaterThanOrEqualOperator, x, y);
        check(Boolean.FALSE, node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >=($0, $1))");
    }

    public void testSimpleLessThan()
    {
        RexNode node = rexBuilder.makeCall(opTab.lessThanOperator, x, y);
        check(Boolean.TRUE, node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <($0, $1))");
    }

    public void testSimpleLessEqual()
    {
        RexNode node =
            rexBuilder.makeCall(opTab.lessThanOrEqualOperator, x, y);
        check(Boolean.FALSE, node,
            "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <=($0, $1))");
    }

    public void testOptimizeNonNullLiterals()
    {
        RexNode node =
            rexBuilder.makeCall(opTab.lessThanOrEqualOperator, x, trueRex);
        check(Boolean.TRUE, node, "AND(IS NOT NULL($0), <=($0, true))");
        node = rexBuilder.makeCall(opTab.lessThanOrEqualOperator, trueRex, x);
        check(Boolean.FALSE, node, "AND(IS NOT NULL($0), <=(true, $0))");
    }

    public void testSimpleIdentifier()
    {
        RexNode node = rexBuilder.makeInputRef(boolRelDataType, 0);
        check(Boolean.TRUE, node, "=(IS TRUE($0), true)");
    }

    public void testMixed1()
    {
        //x=true AND y
        RexNode op1 = rexBuilder.makeCall(opTab.equalsOperator, x, trueRex);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, y);
        check(Boolean.FALSE, and,
            "AND(IS NOT NULL($1), AND(AND(IS NOT NULL($0), =($0, true)), $1))");
    }

    public void testMixed2()
    {
        //x!=true AND y>z
        RexNode op1 = rexBuilder.makeCall(opTab.notEqualsOperator, x, trueRex);
        RexNode op2 = rexBuilder.makeCall(opTab.greaterThanOperator, y, z);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, op2);
        check(Boolean.FALSE, and,
            "AND(AND(IS NOT NULL($0), <>($0, true)), AND(AND(IS NOT NULL($1), IS NOT NULL($2)), >($1, $2)))");
    }

    public void testMixed3()
    {
        //x=y AND false>z
        RexNode op1 = rexBuilder.makeCall(opTab.equalsOperator, x, y);
        RexNode op2 =
            rexBuilder.makeCall(opTab.greaterThanOperator, falseRex, z);
        RexNode and = rexBuilder.makeCall(opTab.andOperator, op1, op2);
        check(Boolean.TRUE, and,
            "AND(AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1)), AND(IS NOT NULL($2), >(false, $2)))");
    }
}


// End RexTransformerTest.java
