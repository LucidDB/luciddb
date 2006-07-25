/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.disruptivetech.farrago.calc;

import java.lang.reflect.*;

import java.util.*;

import junit.framework.*;

import org.eigenbase.util.*;


/**
 * Unit test for {@link CalcProgramBuilder}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 11, 2004
 */
public class CalcProgramBuilderTest
    extends TestCase
{

    //~ Static fields/initializers ---------------------------------------------

    public static final String NL = System.getProperty("line.separator");
    public static final String T = ";" + NL;

    //~ Instance fields --------------------------------------------------------

    //Member varialbes-------------
    CalcProgramBuilder builder;

    //~ Constructors -----------------------------------------------------------

    public CalcProgramBuilderTest(String name)
    {
        super(name);
    }

    //~ Methods ----------------------------------------------------------------

    public void setUp()
        throws Exception
    {
        builder = new CalcProgramBuilder();
        builder.setSeparator(CalcProgramBuilder.SEPARATOR_SEMICOLON);
    }

    /**
     * Tests that the empty program works.
     */
    public void testEmpty()
    {
        final String program = builder.getProgram();
        assertEquals("T;", program);
    }

    public void testReturnConstant()
    {
        CalcProgramBuilder.Register litReg = builder.newInt4Literal(100);
        CalcProgramBuilder.Register outReg =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.move.add(builder, outReg, litReg);
        final String program = builder.getProgram();
        final String expected =
            "O s4;" + "C s4;" + "V 100;" + "T;" + "MOVE O0, C0;";
        TestUtil.assertEqualsVerbose(expected, program);
    }

    public void testComments()
    {
        CalcProgramBuilder.Register lit0 = builder.newInt4Literal(100);
        CalcProgramBuilder.Register lit1 = builder.newVarcharLiteral("A");
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.move.add(builder, out0, lit0);
        builder.addComment("hej");
        CalcProgramBuilder.move.add(builder, out0, lit0);
        builder.addComment("ab");
        builder.addComment("c");
        CalcProgramBuilder.move.add(builder, out0, lit0);
        CalcProgramBuilder.move.add(builder, out0, lit0);
        builder.addComment("/*d*/");
        builder.setOutputComments(true);
        final String program = builder.getProgram();
        final String expected =
            "O s4;" + "C s4, vc,2;" + "V 100, 0x41 /* A */;" + "T;"
            + "MOVE O0, C0 /* 0: hej */;" + "MOVE O0, C0 /* 1: ab c */;"
            + "MOVE O0, C0 /* 2: */;" + "MOVE O0, C0 /* 3: \\*d*\\ */;";
        TestUtil.assertEqualsVerbose(expected, program);
    }

    public void testRef()
    {
        CalcProgramBuilder.Register litIntReg = builder.newInt4Literal(100);
        CalcProgramBuilder.Register outIntReg =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);

        builder.addRef(outIntReg, litIntReg);
        final String program = builder.getProgram();
        final String expected =
            "O s4;" + "C s4;" + "V 100;" + "T;" + "REF O0, C0;";
        TestUtil.assertEqualsVerbose(expected, program);
    }

    public void testRefFails()
    {
        CalcProgramBuilder.Register litIntReg0 = builder.newInt4Literal(100);
        CalcProgramBuilder.Register litIntReg1 = builder.newInt4Literal(100);
        CalcProgramBuilder.Register outIntReg =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.Register litStrReg =
            builder.newVarcharLiteral("Hello world");

        try {
            builder.addRef(outIntReg, litStrReg);
        } catch (Throwable e) {
            assertTrue(
                e.getMessage().matches(
                    ("(?s)(?i).*Type Mismatch. Tried to MOVE.*")));

            //Expecting another exception
            try {
                builder.addRef(litIntReg0, litIntReg1);
            } catch (Throwable ee) {
                assertTrue(
                    ee.getMessage().matches(
                        ("(?s)(?i).*Only output register allowed to reference "
                            + "other registers.*")));
                return;
            }
        }
    }

    public void testUseSameConstantTwice()
    {
        CalcProgramBuilder.Register longConst0 = builder.newInt4Literal(100);
        CalcProgramBuilder.Register longConst1 = builder.newInt4Literal(100);
        CalcProgramBuilder.Register strConst0 =
            builder.newVarcharLiteral("Hello world");
        CalcProgramBuilder.Register strConst1 =
            builder.newVarcharLiteral("Hello world");
        CalcProgramBuilder.Register strConst2 =
            builder.newVarcharLiteral("Hello worlds");

        CalcProgramBuilder.Register outReg =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.move.add(builder, outReg, longConst1);
        final String program = builder.getProgram();
        final String expected =
            "O s4;" + "C s4, vc,22, vc,24;"
            + "V 100, 0x48656C6C6F20776F726C64, 0x48656C6C6F20776F726C6473;"
            + "T;" + "MOVE O0, C0;";
        TestUtil.assertEqualsVerbose(expected, program);
    }

    public void testInconstentTypes()
    {
        // todo: Use the same register with different types. Should get an
        // error.
    }

    public void testCallSubstr()
    {
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Varchar, 10);
        CalcProgramBuilder.Register const0 =
            builder.newVarcharLiteral("Hello world");
        CalcProgramBuilder.Register const1 = builder.newInt4Literal(3);
        CalcProgramBuilder.Register const2 = builder.newInt4Literal(5);
        new CalcProgramBuilder.ExtInstrDef("SUBSTR", 4).add(
            builder,
            new CalcProgramBuilder.Register[] { out0, const0, const1, const2 });
        final String program = builder.getProgram();
        final String expected =
            "O vc,10;" + "C vc,22, s4, s4;"
            + "V 0x48656C6C6F20776F726C64, 3, 5;" + "T;"
            + "CALL 'SUBSTR(O0, C0, C1, C2);";
        TestUtil.assertEqualsVerbose(expected, program);
    }

    public void testJmpToNoWhere()
    {
        builder.addJump(1);
        builder.addJump(188);
        assertExceptionIsThrown("(?s)(?i).*line 188 doesn.t exist.*");
    }

    /**
     * Tests if the builder asserts when trying to call jump to a previous line.
     * If there is a need to have loops in the future. Remove the assert check
     * in the builder
     */
    public void testJumpingBack()
    {
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register const0 = builder.newBoolLiteral(true);
        CalcProgramBuilder.Register const1 = builder.newBoolLiteral(true);
        CalcProgramBuilder.boolAnd.add(builder, out0, const0, const1);
        CalcProgramBuilder.boolAnd.add(builder, out0, const0, const1);
        CalcProgramBuilder.boolAnd.add(builder, out0, const0, const1);
        builder.addJump(2); //jumping back
        assertExceptionIsThrown(
            "(?s)(?i).*Loops are forbidden. Cannot jump to a previous line.*");
    }

    public void testJumpingToItSelf()
    {
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register const0 = builder.newBoolLiteral(true);
        CalcProgramBuilder.Register const1 = builder.newBoolLiteral(true);
        CalcProgramBuilder.boolAnd.add(builder, out0, const0, const1);
        builder.addJump(1); //jumping to itself
        assertExceptionIsThrown(
            "(?s)(?i).*Cannot jump to the same line as the instruction.*");
    }

    public void testLabelJump()
    {
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register const0 = builder.newBoolLiteral(true);
        CalcProgramBuilder.Register const1 = builder.newBoolLiteral(false);
        CalcProgramBuilder.boolAnd.add(builder, out0, const0, const1);
        builder.addLabelJump("label$0");
        builder.addLabelJumpTrue("label$0", out0);
        builder.addLabelJumpFalse("label$1", out0);
        builder.addLabel("label$0");
        builder.addLabelJumpNull("label$1", out0);
        builder.addLabelJumpNotNull("label$1", out0);
        builder.addLabel("label$1");
        CalcProgramBuilder.boolAnd.add(builder, out0, const0, const1);

        final String program = builder.getProgram();
        String expected =
            "O bo;" + "C bo, bo;" + "V 1, 0;" + "T;" + "AND O0, C0, C1;"
            + "JMP @4;" + "JMPT @4, O0;" + "JMPF @6, O0;" + "JMPN @6, O0;"
            + "JMPNN @6, O0;" + "AND O0, C0, C1;";
        TestUtil.assertEqualsVerbose(expected, program);
    }

    public void testLabelJumpFails()
    {
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register const0 = builder.newBoolLiteral(true);
        CalcProgramBuilder.Register const1 = builder.newBoolLiteral(true);
        builder.move.add(builder, out0, const0);
        builder.addLabelJump("gone");
        CalcProgramBuilder.boolAnd.add(builder, out0, const0, const1);
        assertExceptionIsThrown("(?s)(?i).*label 'gone' not defined.*");
    }

    public void testLabelJumpToItself()
    {
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register const0 = builder.newBoolLiteral(true);
        CalcProgramBuilder.Register const1 = builder.newBoolLiteral(true);
        builder.addLabel("label$0");
        builder.addLabelJump("label$0");
        assertExceptionIsThrown(
            "(?s)(?i).*Cannot jump to the same line as the instruction.*");
    }

    public void testLabelMultipleTimes()
    {
        builder.addLabel("again");
        try {
            builder.addLabel("again");
        } catch (Throwable e) {
            assertTrue(
                e.getMessage().matches(
                    ("(?s)(?i).*label 'again' already defined.*")));
            return;
        }
        fail("Exception was not thrown as expected");
    }

    public void testJumpBooleanFails()
        throws Exception
    {
        Object [] args;
        Iterator it;

        //Testing the jumpBooleans with register type != Boolean, expecting
        //asserts
        CalcProgramBuilder.Register const0 = builder.newInt4Literal(3);
        args = new Object[] { new Integer(2), const0 };
        it = getMethods("(?i)addJump\\w+");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a register of Boolean type.*");
        }

        //Testing jumps with negative line nbr
        CalcProgramBuilder.Register input0 =
            builder.newInput(CalcProgramBuilder.OpType.Bool, -1);
        Object [] args1 = new Object[] { new Integer(-2) };
        Object [] args2 = new Object[] { new Integer(-2), input0 };
        args = args1;
        it = getMethods("addJump\\w+");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            if (method.getParameterTypes().length == 2) {
                args = args2;
            } else {
                args = args1;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Line can not be negative. Value=-2.*");
        }
    }

    public void testAddBoolInstructionFails()
        throws Exception
    {
        CalcProgramBuilder.Register const0 = builder.newBoolLiteral(true);
        CalcProgramBuilder.Register in0 =
            builder.newInput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register in1 =
            builder.newInput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register out1 =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.Register in2 =
            builder.newInput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.Register in3 =
            builder.newInput(CalcProgramBuilder.OpType.Uint4, -1);
        Object [] args;
        Object [] cb_ib = new Object[] { const0, in0 };
        Object [] cb_ib_ib = new Object[] { const0, in0, in1 };
        Object [] oi_ib = new Object[] { out1, in0 };
        Object [] ob_il = new Object[] { out0, in2 };
        Object [] ob_il_il = new Object[] { out0, in2, in3 };
        Object [] il_cb = new Object[] { in2, const0 };
        Object [] ib_cb = new Object[] { in0, const0 };
        Object [] ol_cb_cb = new Object[] { out1, const0, const0 };
        Object [] ib_cb_cb = new Object[] { in0, const0, const0 };
        Iterator it;

        // Attempting to store result in literal register
        it = getMethods("addBool\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            args = cb_ib;
            if (method.getParameterTypes().length == 3) {
                args = cb_ib_ib;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a non constant register.*");
        }

        it = getMethods("addBool\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            args = ib_cb;
            if (method.getParameterTypes().length == 3) {
                args = ib_cb_cb;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a non constant register.*");
        }

        it = getMethods("addBool\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            args = oi_ib;
            if (method.getParameterTypes().length == 3) {
                args = ol_cb_cb;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a register of Boolean type.*");
        }

        it = getMethods("addBool\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            if (method.getName().startsWith("addBoolNative")) {
                continue;
            }
            args = ob_il;
            if (method.getParameterTypes().length == 3) {
                args = ob_il_il;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a register of Boolean type.*");
        }
    }

    public void testAddDivideByZeroFail()
        throws Exception
    {
        CalcProgramBuilder.Register in0 =
            builder.newInput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.Register in1 =
            builder.newInput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.Register in2 = builder.newInt4Literal(0);
        assertExceptionIsThrown(
            "addNativeDiv",
            new Object[] { in0, in1, in2 },
            "(?s).*A literal register of Integer type and value=0 was found.*");
        assertExceptionIsThrown(
            "addIntegralNativeMod",
            new Object[] { in0, in1, in2 },
            "(?s).*A literal register of Integer type and value=0 was found.*");
    }

    public void testAddNativeInstructionsFail()
        throws Exception
    {
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register out1 =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.Register out2 =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);
        Object [] args;
        Object [] args2 = new Object[] { out0, out1 };
        Object [] args3 = new Object[] { out0, out1, out2 };

        Iterator it = getMethods("addNative\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            args = args2;
            if (method.getParameterTypes().length == 3) {
                args = args3;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Register is not of native OpType.*");
        }

        args2 = new Object[] { out1, out0 };
        args3 = new Object[] { out1, out0, out2 };
        it = getMethods("addNative\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            args = args2;
            if (method.getParameterTypes().length == 3) {
                args = args3;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Register is not of native OpType.*");
        }

        args3 = new Object[] { out1, out2, out0 };
        it = getMethods("addNative\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            if (method.getParameterTypes().length == 2) {
                continue;
            }
            assertExceptionIsThrown(method,
                args3,
                "(?s).*Register is not of native OpType.*");
        }
    }

    public void testIntegralNativeInstructionsFail()
        throws Exception
    {
        CalcProgramBuilder.Register const0 = builder.newInt4Literal(2);
        CalcProgramBuilder.Register in0 =
            builder.newInput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.Register in1 =
            builder.newInput(CalcProgramBuilder.OpType.Int4, -1);
        CalcProgramBuilder.Register in2 =
            builder.newInput(CalcProgramBuilder.OpType.Real, -1);
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Int4, -1);
        Object [] args;
        Object [] args2 = new Object[] { const0, in0 };
        Object [] args3 = new Object[] { const0, in0, in1 };

        Iterator it = getMethods("addIntegralNative\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            args = args2;
            if (method.getParameterTypes().length == 3) {
                args = args3;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a non constant register.*");
        }

        CalcProgramBuilder.Register const1 = builder.newInt4Literal(-1);
        args = new Object[] { out0, in1, const1 };
        assertExceptionIsThrown("addIntegralNativeShiftLeft",
            args,
            "(?s).*Cannot shift negative amout of steps. Value=-1.*");
        assertExceptionIsThrown("addIntegralNativeShiftRight",
            args,
            "(?s).*Cannot shift negative amout of steps. Value=-1.*");
    }

    public void testPointerBoolInstructionsFail()
        throws Exception
    {
        CalcProgramBuilder.Register const0 = builder.newBoolLiteral(true);
        CalcProgramBuilder.Register const1 = builder.newVarcharLiteral("hey");
        CalcProgramBuilder.Register in0 =
            builder.newInput(CalcProgramBuilder.OpType.Varbinary, 5);
        CalcProgramBuilder.Register in1 =
            builder.newInput(CalcProgramBuilder.OpType.Varchar, 8);
        CalcProgramBuilder.Register out0 =
            builder.newOutput(CalcProgramBuilder.OpType.Bool, -1);
        CalcProgramBuilder.Register out1 =
            builder.newOutput(CalcProgramBuilder.OpType.Varbinary, 6);
        CalcProgramBuilder.Register in2 =
            builder.newInput(CalcProgramBuilder.OpType.Real, -1);
        CalcProgramBuilder.Register in3 =
            builder.newInput(CalcProgramBuilder.OpType.Bool, -1);

        Object [] args;
        Object [] args2 = new Object[] { const0, in0 };
        Object [] args3 = new Object[] { in1, in0, in1 };

        Iterator it = getMethods("addPointer\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            args = args2;
            if (method.getParameterTypes().length == 3) {
                args = args3;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a non constant register.*");
        }

        // Testing PointerBoolean operators
        args2 = new Object[] { out1, in1 };
        args3 = new Object[] { out1, in1, in2 };
        it = getMethods("addPointer\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            if (method.getName().equals("addPointerMove")
                || (method.getName().equals("addPointerAdd"))) {
                continue;
            }

            args = args2;
            if (method.getParameterTypes().length == 3) {
                args = args3;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a register of Boolean type.*");
        }

        args2 = new Object[] { out0, in3 };
        args3 = new Object[] { out0, in3, in0 };
        it = getMethods("addPointer\\w*");
        while (it.hasNext()) {
            Method method = (Method) it.next();
            if (method.getName().equals("addPointerMove")
                || (method.getName().equals("addPointerAdd"))) {
                continue;
            }
            args = args2;
            if (method.getParameterTypes().length == 3) {
                args = args3;
            }
            assertExceptionIsThrown(method,
                args,
                "(?s).*Expected a register of Pointer type.*");
        }

        args3 = new Object[] { out1, out0, in3 };
        assertExceptionIsThrown("addPointerAdd",
            args3,
            "(?s).*Expected a register of Pointer type.*");

        args = new Object[] { out1, in1, in1 };
        assertExceptionIsThrown("addPointerAdd",
            args,
            "(?s).*Expected a register of Integer type.*");
    }

    //Helper methods-------------------------------------
    Iterator getMethods(String pattern)
    {
        ArrayList ret = new ArrayList();
        Method [] m = builder.getClass().getMethods();
        for (int i = 0; i < m.length; i++) {
            Method method = m[i];
            if (method.getName().matches(pattern)) {
                ret.add(method);
            }
        }
        return ret.iterator();
    }

    void assertExceptionIsThrown(
        Method method,
        Object [] args,
        String expectedMsg)
        throws Exception
    {
        assertNotNull(method);
        try {
            method.invoke(builder, args);
            fail(
                "Exception was not thrown while invoking method "
                + method.getName());
        } catch (Exception e) {
            String actualMsg = e.getMessage() + "\n";
            if (null != e.getCause()) {
                actualMsg += e.getCause().getMessage();
            }
            if (!actualMsg.matches(expectedMsg)) {
                fail(
                    "Unexpected message while invokig method "
                    + method.getName() + ". Actual:\n" + actualMsg
                    + "\n\nExpected:\n" + expectedMsg);
            }
        }
    }

    void assertExceptionIsThrown(
        String methodName,
        Object [] args,
        String expectedMsg)
        throws Exception
    {
        Class [] params = new Class[args.length];
        for (int i = 0; i < params.length; i++) {
            params[i] = args[i].getClass();
        }
        Method method = builder.getClass().getMethod(methodName, params);
        assertExceptionIsThrown(method, args, expectedMsg);
    }

    void assertExceptionIsThrown(String expectedMsg)
    {
        try {
            String program = builder.getProgram();
            fail("Exception was not thrown for program=" + program);
        } catch (Exception e) {
            String actualMsg = e.getMessage();
            if (!actualMsg.matches(expectedMsg)) {
                fail(
                    "Unexpected message. Actual:\n" + actualMsg
                    + "\n\nExpected:\n" + expectedMsg);
            }
        }
    }
}

// End CalcProgramBuilderTest.java
