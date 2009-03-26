/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
package net.sf.farrago.fennel.calc;

import java.io.*;

import java.math.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;

import org.eigenbase.util.*;


/**
 * Constructs a calculator assembly language program based upon a series of
 * calls made by the client.
 *
 * <p>If you want multi-line programs, call <code>
 * setSeparator(System.getProperty("line.separator")</code>.</p>
 *
 * <p>See {@link #tracer}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jan 11, 2004
 * @testcase
 */
public class CalcProgramBuilder
{
    //~ Static fields/initializers ---------------------------------------------

    /* Constants */
    private static final String NL = System.getProperty("line.separator");
    public static final String SEPARATOR_SEMICOLON = ";";
    public static final String SEPARATOR_NEWLINE = NL;
    public static final String SEPARATOR_SEMICOLON_NEWLINE = ";" + NL;
    private static final Logger tracer = FarragoTrace.getCalcTracer();
    private static final BigInteger Uint4_MAX = BigInteger.ONE.shiftLeft(32);
    private static final BigInteger Uint8_MAX = BigInteger.ONE.shiftLeft(64);

    // -- instructions -------------------------------------------------------

    public static final InstructionDef refInstruction =
        new InstructionDef("REF", 2);
    public static final JumpInstructionDef jumpInstruction =
        new JumpInstructionDef("JMP", 1);
    public static final JumpInstructionDef jumpTrueInstruction =
        new JumpInstructionDef("JMPT", 2);
    public static final JumpInstructionDef jumpFalseInstruction =
        new JumpInstructionDef("JMPF", 2);
    public static final JumpInstructionDef jumpNullInstruction =
        new JumpInstructionDef("JMPN", 2);
    public static final JumpInstructionDef jumpNotNullInstruction =
        new JumpInstructionDef("JMPNN", 2);
    public static final InstructionDef returnInstruction =
        new InstructionDef("RETURN", 0);

    public static final InstructionDef nativeAdd =
        new NativeInstructionDef("ADD", 3);
    public static final InstructionDef pointerAdd =
        new InstructionDef("ADD", 3) {
            void add(
                CalcProgramBuilder builder,
                CalcReg ... regs)
            {
                builder.assertRegisterNotConstant(regs[0]);
                builder.assertRegisterIsPointer(regs[0]);
                builder.assertRegisterIsPointer(regs[1]);
                builder.assertRegisterInteger(regs[2]);
                super.add(builder, regs);
            }
        };

    public static final InstructionDef boolAnd =
        new BoolInstructionDef("AND", 3);
    public static final InstructionDef integralNativeAnd =
        new IntegralNativeInstructionDef("AND", 3);
    public static final InstructionDef cast =
        new NativeInstructionDef("CAST", 2);
    public static final InstructionDef call =
        new InstructionDef("CALL", 1) {
            void add(
                CalcProgramBuilder builder,
                Operand ... operands)
            {
                addInternal(builder, operands);
            }
        };

    public static final InstructionDef nativeDiv =
        new NativeInstructionDef("DIV", 3) {
            void add(
                CalcProgramBuilder builder,
                CalcReg ... regs)
            {
                builder.assertNotDivideByZero(regs[2]);
                super.add(builder, regs);
            }
        };

    public static final InstructionDef boolNativeEqual =
        new BoolNativeInstructionDef("EQ", 3);
    public static final InstructionDef boolEqual =
        new ComparisonInstructionDef("EQ");
    public static final InstructionDef pointerBoolEqual =
        new PointerBoolInstructionDef("EQ", 3);
    public static final InstructionDef boolGreaterThan =
        new ComparisonInstructionDef("GT");
    public static final InstructionDef boolNativeGreaterThan =
        new BoolNativeInstructionDef("GT", 3);
    public static final InstructionDef pointerBoolGreaterThan =
        new PointerBoolInstructionDef("GT", 3);
    public static final InstructionDef boolGreaterOrEqualThan =
        new ComparisonInstructionDef("GE");
    public static final InstructionDef boolNativeGreaterOrEqualThan =
        new BoolNativeInstructionDef("GE", 3);
    public static final InstructionDef pointerBoolGreaterOrEqualThan =
        new PointerBoolInstructionDef("GE", 3);
    public static final InstructionDef boolNativeIsNull =
        new BoolNativeInstructionDef("ISNULL", 2);
    public static final InstructionDef boolIsNull =
        new BoolInstructionDef("ISNULL", 2);
    public static final InstructionDef pointerBoolIsNull =
        new PointerBoolInstructionDef("ISNULL", 2);
    public static final InstructionDef boolNativeIsNotNull =
        new BoolNativeInstructionDef("ISNOTNULL", 2);
    public static final InstructionDef boolIsNotNull =
        new BoolInstructionDef("ISNOTNULL", 2);
    public static final InstructionDef pointerBoolIsNotNull =
        new PointerBoolInstructionDef("ISNOTNULL", 2);
    public static final InstructionDef boolLessThan =
        new ComparisonInstructionDef("LT");
    public static final InstructionDef boolNativeLessThan =
        new BoolNativeInstructionDef("LT", 3);
    public static final InstructionDef pointerBoolLessThan =
        new PointerBoolInstructionDef("LT", 3);
    public static final InstructionDef boolLessOrEqualThan =
        new ComparisonInstructionDef("LE");
    public static final InstructionDef boolNativeLessOrEqualThan =
        new BoolNativeInstructionDef("LE", 3);
    public static final InstructionDef pointerBoolLessOrEqualThan =
        new PointerBoolInstructionDef("LE", 3);
    public static final InstructionDef nativeMinus =
        new NativeInstructionDef("SUB", 3);

    /**
     * Integral modulo instruction. Per C++, returns negative values when
     * applied to negative values. For example, 10 % 3 = 1, -10 % 3 = -1.
     */
    public static final InstructionDef integralNativeMod =
        new IntegralNativeInstructionDef("MOD", 3) {
            void add(
                CalcProgramBuilder builder,
                CalcReg ... regs)
            {
                //check if divide by zero if op2 is a constant
                builder.assertNotDivideByZero(regs[2]);
                super.add(builder, regs);
            }
        };

    public static final InstructionDef move =
        new InstructionDef("MOVE", 2) {
            void add(
                CalcProgramBuilder builder,
                CalcReg ... regs)
            {
                builder.compilationAssert(
                    regs[0].getOpType() == regs[1].getOpType(),
                    "Type Mismatch. Tried to MOVE "
                    + regs[1].getOpType() + " into a "
                    + regs[0].getOpType());
                super.add(builder, regs);
            }
        };

    public static final InstructionDef boolMove =
        new BoolInstructionDef("MOVE", 2);
    public static final InstructionDef nativeMove =
        new NativeInstructionDef("MOVE", 2);
    public static final InstructionDef pointerMove =
        new InstructionDef("MOVE", 2) {
            void add(
                CalcProgramBuilder builder,
                CalcReg ... regs)
            {
                builder.assertRegisterNotConstant(regs[0]);
                builder.assertRegisterIsPointer(regs[0]);
                builder.assertRegisterIsPointer(regs[1]);
                super.add(builder, regs);
            }
        };

    public static final InstructionDef integralNativeMul =
        new IntegralNativeInstructionDef("MUL", 3);
    public static final InstructionDef boolNot =
        new BoolInstructionDef("NOT", 2);
    public static final InstructionDef boolNativeNotEqual =
        new BoolNativeInstructionDef("NE", 3);
    public static final InstructionDef boolNotEqual =
        new ComparisonInstructionDef("NE");
    public static final InstructionDef pointerBoolNotEqual =
        new PointerBoolInstructionDef("NE", 3);
    public static final InstructionDef boolOr = new BoolInstructionDef("OR", 3);
    public static final InstructionDef integralNativeOr =
        new IntegralNativeInstructionDef("OR", 3);
    public static final InstructionDef nativeNeg =
        new NativeInstructionDef("NEG", 2);
    public static final InstructionDef raise =
        new InstructionDef("RAISE", 1) {
            void add(
                CalcProgramBuilder builder,
                CalcReg ... regs)
            {
                builder.assertRegisterLiteral(regs[0]);
                builder.assertIsVarchar(regs[0]);
                super.add(builder, regs);
            }
        };

    /**
     * Rounds approximate types to nearest integer but remains same type
     */
    public static final InstructionDef round =
        new NativeInstructionDef("ROUND", 2);
    public static final InstructionDef integralNativeShiftLeft =
        new IntegralNativeShift("SHFL");
    public static final InstructionDef integralNativeShiftRight =
        new IntegralNativeShift("SHFR");
    public static final InstructionDef integralNativeXor =
        new IntegralNativeInstructionDef("XOR", 3);

    public static final InstructionDef [] allInstrDefs =
    {
        boolAnd,
        boolEqual,
        boolGreaterOrEqualThan,
        boolGreaterThan,
        boolIsNotNull,
        boolIsNull,
        boolLessOrEqualThan,
        boolLessThan,
        boolMove,
        boolNativeEqual,
        boolNativeGreaterOrEqualThan,
        boolNativeGreaterThan,
        boolNativeIsNotNull,
        boolNativeIsNull,
        boolNativeLessOrEqualThan,
        boolNativeLessThan,
        boolNativeNotEqual,
        call,
        cast,
        integralNativeAnd,
        integralNativeMod,
        integralNativeMul,
        integralNativeOr,
        integralNativeShiftLeft,
        integralNativeShiftRight,
        integralNativeXor,
        jumpInstruction,
        jumpTrueInstruction,
        jumpFalseInstruction,
        jumpNotNullInstruction,
        jumpNullInstruction,
        move,
        nativeAdd,
        nativeDiv,
        nativeMinus,
        nativeMove,
        nativeNeg,
        refInstruction,
        nativeAdd,
        pointerAdd,
        pointerAdd,
        pointerBoolEqual,
        pointerBoolGreaterOrEqualThan,
        pointerBoolGreaterThan,
        pointerBoolIsNotNull,
        pointerBoolIsNull,
        pointerBoolLessOrEqualThan,
        pointerBoolLessThan,
        pointerBoolNotEqual,
        pointerMove,
        raise,
        returnInstruction,
        round,
    };

    //~ Enums ------------------------------------------------------------------

    /**
     * Enumeration of the types supported by the calculator. These types map
     * onto the {@link FennelStandardTypeDescriptor} values, and even have the
     * same names and ordinals.
     *
     * @see CalcProgramBuilder#Uint4_MAX
     * @see CalcProgramBuilder#Uint8_MAX
     */
    public enum OpType
    {
        Int1(FennelStandardTypeDescriptor.INT_8, "s1"),
        Uint1(FennelStandardTypeDescriptor.UINT_8, "u1"),
        Int2(FennelStandardTypeDescriptor.INT_16, "s2"),
        Uint2(FennelStandardTypeDescriptor.UINT_16, "u2"),
        Int4(FennelStandardTypeDescriptor.INT_32, "s4"),
        Uint4(FennelStandardTypeDescriptor.UINT_32, "u4"),
        Int8(FennelStandardTypeDescriptor.INT_64, "s8"),
        Uint8(FennelStandardTypeDescriptor.UINT_64, "u8"),
        Bool(FennelStandardTypeDescriptor.BOOL, "bo"),
        Real(FennelStandardTypeDescriptor.REAL, "r"),
        Double(FennelStandardTypeDescriptor.DOUBLE, "d"),
        Char(FennelStandardTypeDescriptor.CHAR, "c"),
        Varchar(FennelStandardTypeDescriptor.VARCHAR, "vc"),
        Binary(FennelStandardTypeDescriptor.BINARY, "b"),
        Varbinary(FennelStandardTypeDescriptor.VARBINARY, "vb");

        private FennelStandardTypeDescriptor type;
        private static final int ValueCount = values().length;
        private final String shortName;

        private OpType(FennelStandardTypeDescriptor type, String shortName)
        {
            this.shortName = shortName;

            // Discard the type. It is there for doc reasons, but because of
            // the class-loading process it may be null.
            Util.discard(type);
        }

        public String toString()
        {
            return shortName;
        }

        private FennelStandardTypeDescriptor getType()
        {
            if (type == null) {
                type =
                    (FennelStandardTypeDescriptor)
                    FennelStandardTypeDescriptor.enumeration.getValue(
                        ordinal() + 1);
                assert type.getName().equals(shortName);
            }
            return type;
        }

        public boolean isExact()
        {
            return getType().isExact();
        }

        public boolean isApprox()
        {
            return getType().isNumeric() && !getType().isExact();
        }

        public boolean isNumeric()
        {
            return getType().isNumeric();
        }
    }

    /**
     * Enumeration of register types
     */
    public enum RegisterSetType
    {
        Output('O'), Input('I'), Literal('C'), Local('L'), Status('S');

        final char prefix;
        static final int ValueCount = RegisterSetType.values().length;

        RegisterSetType(char prefix)
        {
            this.prefix = prefix;
        }
    }

    //~ Instance fields --------------------------------------------------------

    /* Member variables */
    protected String separator = SEPARATOR_SEMICOLON_NEWLINE;
    protected final List<Instruction> instructions =
        new ArrayList<Instruction>();
    protected RegisterSets registerSets = new RegisterSets();
    protected final Map<LiteralPair, CalcReg> literals =
        new HashMap<LiteralPair, CalcReg>();
    protected final Map<String, Integer> labels =
        new HashMap<String, Integer>();
    private boolean outputComments = false;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a CalcProgramBuilder.
     */
    public CalcProgramBuilder()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Re-initializes a CalcProgramBuilder.
     */
    public void clear()
    {
        instructions.clear();
        registerSets.clear();
        literals.clear();
        labels.clear();
    }

    // Methods -----------------------------------------------------------------

    /**
     * Sets the separator between instructions in the generated program. Can be
     * either {@link #SEPARATOR_SEMICOLON ';'} or {@link #SEPARATOR_NEWLINE '\n'
     * or ";\n"}
     */
    public void setSeparator(String separator)
    {
        if (!separator.equals(SEPARATOR_NEWLINE)
            && !separator.equals(SEPARATOR_SEMICOLON)
            && !separator.equals(SEPARATOR_SEMICOLON_NEWLINE))
        {
            throw FarragoResource.instance().ProgramCompilationError.ex(
                "Separator must be ';'[\\n] or '\\n'");
        }
        this.separator = separator;
    }

    public void setOutputComments(boolean outputComments)
    {
        this.outputComments = outputComments;
    }

    protected void compilationAssert(
        boolean cond,
        String msg)
    {
        if (!cond) {
            throw FarragoResource.instance().CompilationAssertionError.ex(msg);
        }
    }

    protected void compilationAssert(boolean cond)
    {
        compilationAssert(cond, null);
    }

    /**
     * A program consists of two parts, its instructions and the user defined
     * registers with its value. The string representation of the program will
     * be in the following format <Register Set Definitions> See {@link
     * #getRegisterSetsLayout}<br>
     * <"\n"><br>
     * <Instruction set> See {@link #getInstructions}<br>
     * <br>
     */
    public String getProgram()
    {
        bindReferences();
        validate();
        optimize();
        validate(); //validate again to check if the optimization broke
                    //something
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        getRegisterSetsLayout(writer);
        writer.print("T");
        writer.print(separator);
        getInstructions(writer);
        final String program = sw.toString().trim();
        if (tracer.isLoggable(Level.FINE)) {
            final String prettyProgram = prettyPrint(program);
            tracer.log(Level.FINE, prettyProgram);
        }
        return program;
    }

    /**
     * Introduces NL's for prettiness. Comments would be nice too.
     */
    private String prettyPrint(String program)
    {
        if (!separator.equals(SEPARATOR_SEMICOLON_NEWLINE)) {
            return program.replaceAll(separator, SEPARATOR_SEMICOLON_NEWLINE);
        }
        return program;
    }

    /**
     * Returns the register of a given ordinal and type.
     */
    CalcReg getRegister(
        int ordinal,
        CalcProgramBuilder.RegisterSetType registerType)
    {
        List<CalcReg> regList = registerSets.getRegisterList(registerType);
        return regList.get(ordinal);
    }

    /**
     * Replaces a label with a line number
     */
    private void bindReferences()
    {
        int i = 0;
        for (Instruction instruction : instructions) {
            // Look for instructions that have Line as operands.
            instruction.setLineNumber(i++);
            Operand [] operands = instruction.getOperands();
            for (int j = 0; (null != operands) && (j < operands.length); j++) {
                Operand operand = operands[j];
                if (operand instanceof Line) {
                    Line line = (Line) operand;
                    if (line.getLabel() != null) {
                        // We have a label; update the line number with what's
                        // in the labels map
                        compilationAssert(
                            null == line.getLine(),
                            "Line has already been bind.");
                        Integer lineNumberFromLabel =
                            labels.get(line.getLabel());
                        if (null == lineNumberFromLabel) {
                            throw FarragoResource.instance()
                            .ProgramCompilationError.ex(
                                "Label '"
                                + line.getLabel() + "' not defined");
                        }
                        line.setLine(lineNumberFromLabel.intValue());
                    }
                }
            }
        }
    }

    /**
     * Returns the number of existing lines of instructions - 1
     */
    public int getCurrentLineNumber()
    {
        return instructions.size() - 1;
    }

    /**
     * Outputs register declarations. The results look like this:
     *
     * <blockquote>
     * <pre>
     * O vc,20;
     * I vc,30;
     * C u1, u1, vc,22, vc,12, vc,30, s4;
     * V 1, 0, 'ISO-8859-1', 'WILMA', 'ISO-8859-1$en', 0;
     * L vc,30, u1, s4;
     * S u1;</pre>
     * </blockquote>
     *
     * @param writer
     */
    protected void getRegisterSetsLayout(PrintWriter writer)
    {
        generateRegDeclarations(writer, RegisterSetType.Output);
        generateRegDeclarations(writer, RegisterSetType.Input);
        generateRegDeclarations(writer, RegisterSetType.Local);
        generateRegDeclarations(writer, RegisterSetType.Status);
        generateRegDeclarations(writer, RegisterSetType.Literal);
        generateRegValues(writer);
    }

    private void generateRegDeclarations(
        PrintWriter writer,
        RegisterSetType registerSetType)
    {
        List<CalcReg> regList = registerSets.getRegisterList(registerSetType);
        if (regList.isEmpty()) {
            return;
        }
        writer.print(registerSetType.prefix);
        writer.print(" ");

        // Iterate over every register in the current set
        for (int j = 0; j < regList.size(); j++) {
            if (j > 0) {
                writer.print(", ");
            }
            CalcReg reg = regList.get(j);
            assert reg.getRegisterType() == registerSetType;
            writer.print(reg.getOpType());
            switch (reg.getOpType()) {
            case Binary:
            case Char:
            case Varbinary:
            case Varchar:
                assert reg.storageBytes >= 0;
                writer.print("," + reg.storageBytes);
                break;
            default:
                assert reg.storageBytes == -1 : reg.getOpType();
            }
            if (false) {
                // Assembler doesn't support this.
                writer.print('[');
                writer.print(reg.getIndex());
                writer.print(']');
            }

            if (reg.getValue() != null) {
                compilationAssert(
                    registerSetType == RegisterSetType.Literal,
                    "Only literals have values");
            }
        }
        writer.print(separator);
    }

    private void generateRegValues(PrintWriter writer)
    {
        List<CalcReg> regList =
            registerSets.getRegisterList(RegisterSetType.Literal);
        if (regList.isEmpty()) {
            return;
        }
        writer.print("V ");
        for (int j = 0; j < regList.size(); j++) {
            if (j > 0) {
                writer.print(", ");
            }
            CalcReg reg = regList.get(j);

            //final Object value = reg.getValue();
            //assert value != null;
            reg.printValue(writer, outputComments);
        }
        writer.print(separator);
    }

    /**
     * See {@link #getProgram}
     *
     * @param writer
     */
    protected void getInstructions(PrintWriter writer)
    {
        for (Instruction instruction : instructions) {
            instruction.print(writer);
        }
    }

    /**
     * Tries to optimize the program.
     */
    private void optimize()
    {
        //todo 1 look and remove unused registers todo 2 maybe dangersous but an
        //idea anyway, implement an algorithm that reduces and reuses local
        //registers todo 3 look for instruction like "literal operation
        //literal" and precalcuate if possible. todo    Probably wouldnt happen
        //too often. Who would query "not true"=false
    }

    /**
     * Validates the program. Some errors are:
     *
     * <ul>
     * <li>use of same register with different types</li>
     * <li>jumping to a line that does not exist</li>
     * </ul>
     */
    private void validate()
    {
        int i = -1;
        for (Instruction inst : instructions) {
            ++i;

            //this try-catch clause will pick up any compiler excpetions
            //messages and wrap it into a msg containg what line went wrong
            try {
                // Check if any jump instructions are jumping off the cliff
                if (inst.def instanceof JumpInstructionDef) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue() >= instructions.size()) {
                        throw FarragoResource.instance().ProgramCompilationError
                        .ex("Line "
                            + line.getLine() + " doesn't exist");
                    }
                }

                // Check if any jump instruction jumps to itself
                if (inst.def instanceof JumpInstructionDef) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue() == i) {
                        throw FarragoResource.instance().ProgramCompilationError
                        .ex(
                            "Cannot jump to the same line as the instruction");
                    }
                }

                // Forbidding loops. Check if any jump instruction jumps to a
                // previous line.
                if (inst.def instanceof JumpInstructionDef) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue() < i) {
                        throw FarragoResource.instance().ProgramCompilationError
                        .ex(
                            "Loops are forbidden. Cannot jump to a previous line");
                    }
                }

                // TODO add to see that all declared registers are beeing
                // referenced
                //
                // TODO add to see if output registers where declared

                // (?) TODO add to see if all output registers where written to

                // TODO add to see if a non constant register is used before
                // assigned to
            } catch (Throwable e) {
                StringWriter log = new StringWriter();
                inst.print(new PrintWriter(log));
                throw FarragoResource.instance().ProgramCompilationError.ex(
                    log.toString() + NL + e.getMessage(),
                    e);
            }
        }
    }

    private void printOperands(
        PrintWriter writer,
        Operand [] operands)
    {
        if (null != operands) {
            for (int i = 0; i < operands.length; i++) {
                operands[i].print(writer);
                if ((i + 1) < operands.length) {
                    writer.print(", ");
                }
            }
        }
    }

    //---------------------------------------
    //        Program Builder Methods
    //---------------------------------------
    // Register Creation --------------------

    /**
     * Generates a reference to an output register.
     */
    public CalcReg newOutput(
        OpType type,
        int storageBytes)
    {
        return registerSets.newRegister(
            type,
            null,
            RegisterSetType.Output,
            storageBytes);
    }

    /**
     * Generates a reference to an output register.
     */
    public CalcReg newOutput(RegisterDescriptor desc)
    {
        return newOutput(
            desc.getType(),
            desc.getBytes());
    }

    /**
     * Generates a reference to a constant register. If the the constant value
     * already has been defined, the existing reference for that value will
     * silently be returned instead of creating a new one
     *
     * @param type {@link OpType}  Type of value
     * @param value Value
     */
    private CalcReg newLiteral(
        OpType type,
        Object value,
        int storageBytes)
    {
        CalcReg ret;
        LiteralPair key = new LiteralPair(type, value);
        if (literals.containsKey(key)) {
            ret = literals.get(key);
        } else {
            ret =
                registerSets.newRegister(
                    type,
                    value,
                    RegisterSetType.Literal,
                    storageBytes);
            literals.put(key, ret);
        }
        return ret;
    }

    public CalcReg newLiteral(
        RegisterDescriptor desc,
        Object value)
    {
        return newLiteral(
            desc.getType(),
            value,
            desc.getBytes());
    }

    public CalcReg newBoolLiteral(boolean b)
    {
        return newLiteral(
            OpType.Bool,
            Boolean.valueOf(b),
            -1);
    }

    public CalcReg newInt4Literal(int i)
    {
        return newLiteral(
            OpType.Int4,
            i,
            -1);
    }

    public CalcReg newInt8Literal(long i)
    {
        return newLiteral(
            OpType.Int8,
            i,
            -1);
    }

    // arg is a BigInteger because large unsigned ints won't fit into an int
    public CalcReg newUint4Literal(BigInteger i)
    {
        compilationAssert(
            i.compareTo(BigInteger.ZERO) >= 0,
            "Unsigned value was found to be negative. Value=" + i);
        assert i.compareTo(Uint4_MAX) < 0;
        return newLiteral(
            OpType.Uint4,
            i,
            -1);
    }

    // arg is a BigInteger because a large unsigned long value won't fit into a
    // long
    public CalcReg newUint8Literal(BigInteger i)
    {
        compilationAssert(
            i.compareTo(BigInteger.ZERO) >= 0,
            "Unsigned value was found to be negative. Value=" + i);
        assert i.compareTo(Uint8_MAX) < 0;
        return newLiteral(
            OpType.Uint8,
            i,
            -1);
    }

    public CalcReg newFloatLiteral(float f)
    {
        return newLiteral(
            OpType.Real,
            f,
            -1);
    }

    public CalcReg newDoubleLiteral(double d)
    {
        return newLiteral(
            OpType.Double,
            d,
            -1);
    }

    public CalcReg newVarbinaryLiteral(byte [] bytes)
    {
        return newLiteral(OpType.Varbinary, bytes, bytes.length);
    }

    /**
     * Generates a reference to a string literal. The actual value will be a
     * pointer to an array of bytes.
     */
    public CalcReg newVarcharLiteral(String s)
    {
        return newLiteral(
            OpType.Varchar,
            s,
            stringByteCount(s));
    }

    /**
     * Generates a reference to a string literal. The actual value will be a
     * pointer to an array of bytes. This methods is just here temporary until
     * newVarcharLiteral(string) can distinguise between ascii and non-ascii
     * strings and should not be used in places of general code. Only very
     * specific code where you know for a dead fact the string is the right
     * length.
     */
    public CalcReg newVarcharLiteral(
        String s,
        int length)
    {
        //TODO this methods is just here temporary until
        //newVarcharLiteral(string) can distinguise between ascii and non-ascii
        //strings
        return newLiteral(OpType.Varchar, s, length);
    }

    /**
     * Returns the number of bytes storage required for a string. TODO: There is
     * already a utility function somewhere which does this.
     */
    public static int stringByteCount(String s)
    {
        return stringByteCount(s.length());
    }

    /**
     * Returns the number of bytes storage required for a string. TODO: There is
     * already a utility function somewhere which does this.
     */
    public static int stringByteCount(int i)
    {
        return i * 2;
    }

    /**
     * Creates a register in the input set and returns its reference
     */
    public CalcReg newInput(
        OpType type,
        int storageBytes)
    {
        return registerSets.newRegister(
            type,
            null,
            RegisterSetType.Input,
            storageBytes);
    }

    public CalcReg newInput(RegisterDescriptor desc)
    {
        return newInput(
            desc.getType(),
            desc.getBytes());
    }

    /**
     * Creates a register in the local set and returns its reference
     */
    public CalcReg newLocal(
        OpType type,
        int storageBytes)
    {
        return registerSets.newRegister(
            type,
            null,
            RegisterSetType.Local,
            storageBytes);
    }

    /**
     * Creates a register in the local set and returns its reference
     */
    public CalcReg newLocal(RegisterDescriptor desc)
    {
        return newLocal(
            desc.getType(),
            desc.getBytes());
    }

    /**
     * Creates a register in the status set and returns its reference
     */
    public CalcReg newStatus(
        OpType type,
        int storageBytes)
    {
        return registerSets.newRegister(
            type,
            null,
            RegisterSetType.Status,
            storageBytes);
    }

    // ---------------------------------------
    // Instruction Creation

    /**
     * For internal use only. Applications should call <code>
     * instruction.add(builder, operand0, operand1, ...)</code>.
     *
     * @param instrDef Instruction defn
     * @param operands Operands
     */
    protected void addInstruction(
        InstructionDef instrDef,
        Operand ... operands)
    {
        assertOperandsNotNull(operands);
        instructions.add(new Instruction(instrDef, operands));
    }

    /**
     * Adds an comment string to the last instruction. Some special character
     * sequences are modified, see {@link #formatComment}
     *
     * @param comment
     *
     * @pre at least one instruction needs to have been added prior to calling
     * this function.
     */
    protected void addComment(String comment)
    {
        if (0 == instructions.size()) {
            throw Util.needToImplement(
                "TODO need to handle case when  instruction list is empty");
        }
        Instruction inst = instructions.get(instructions.size() - 1);
        inst.setComment((inst.getComment() + " " + comment).trim());
    }

    /**
     * Formats a "logical" comment into a "physical" comment the calculator
     * recognises. E.g.<br>
     * <code>formatCommnet("yo wassup?")</code> outputs<br>
     * <code>' &#47;* yo wassup? *&#47;'</code> NB the inital space.<br>
     * If comment contains the character sequences '&#47;*' or '*&#47' they will
     * be replace by \* and *\ respectively.
     */
    static String formatComment(String comment)
    {
        return " /* "

            /* all 6 \'s are needed */
            + comment.replaceAll("/\\*", "\\\\\\*").replaceAll(
                "\\*/",
                "\\*\\\\")
            + " */";
    }

    /**
     * Asserts that the register is not declared as {@link
     * RegisterSetType#Literal} or {@link RegisterSetType#Input}.
     *
     * @param reg Register
     */
    protected void assertRegisterNotConstant(CalcReg reg)
    {
        compilationAssert(
            (reg.getRegisterType() != RegisterSetType.Literal)
            && (((CalcReg) reg).getRegisterType() != RegisterSetType.Input),
            "Expected a non constant register. Constant registers are Literals and Inputs");
    }

    /**
     * Asserts that the register is declared as {@link RegisterSetType#Literal}
     *
     * @param reg Register
     */
    protected void assertRegisterLiteral(CalcReg reg)
    {
        compilationAssert(
            reg.getRegisterType() == RegisterSetType.Literal,
            "Expected a Literal register.");
    }

    /**
     * Asserts that the register is declared as {@link OpType#Bool}
     *
     * @param reg Register
     */
    protected void assertRegisterBool(CalcReg reg)
    {
        compilationAssert(
            reg.getOpType() == OpType.Bool,
            "Expected a register of Boolean type. " + "Found "
            + reg.getOpType());
    }

    /**
     * Asserts that the register is declared as a pointer. Pointers are {@link
     * OpType#Varchar},{@link OpType#Varbinary}
     *
     * @param reg Register
     */
    protected void assertRegisterIsPointer(CalcReg reg)
    {
        compilationAssert(
            (reg.getOpType() == OpType.Varbinary)
            || (reg.getOpType() == OpType.Varchar),
            "Expected a register of Pointer type");
    }

    /**
     * Asserts that the register is declared as integer. Integers are {@link
     * OpType#Int4}, {@link OpType#Int8}, {@link OpType#Uint4},{@link
     * OpType#Uint8}
     *
     * @param reg Register
     */
    protected void assertRegisterInteger(CalcReg reg)
    {
        compilationAssert(
            (reg.getOpType() == OpType.Int4)
            || (reg.getOpType() == OpType.Int8)
            || (reg.getOpType() == OpType.Uint4)
            || (reg.getOpType() == OpType.Uint8),
            "Expected a register of Integer type");
    }

    /**
     * Asserts that Register is nothing of the following, all at once
     *
     * <ul>
     * <li>of Literal type</li>
     * <li>of Integer type</li>
     * <li>its value is zero</li>
     * </ul>
     *
     * @param reg Register
     */
    protected void assertNotDivideByZero(CalcReg reg)
    {
        if ((reg.getRegisterType() == RegisterSetType.Literal)
            && (reg.getValue() != null)
            && (reg.getValue() instanceof java.lang.Integer))
        {
            compilationAssert(
                !reg.getValue().equals(0),
                "A literal register of Integer type and value=0 was found");
        }
    }

    /**
     * Asserts input is of native type except booleans
     *
     * @param reg Register
     */
    protected void assertIsNativeType(CalcReg reg)
    {
        compilationAssert(
            (reg.getOpType() == OpType.Int1)
            || (reg.getOpType() == OpType.Uint1)
            || (reg.getOpType() == OpType.Int2)
            || (reg.getOpType() == OpType.Uint2)
            || (reg.getOpType() == OpType.Int4)
            || (reg.getOpType() == OpType.Int8)
            || (reg.getOpType() == OpType.Uint4)
            || (reg.getOpType() == OpType.Uint8)
            || (reg.getOpType() == OpType.Real)
            || (reg.getOpType() == OpType.Double),
            "Register is not of native OpType");
    }

    /**
     * Asserts input is of type {@link OpType#Varchar}
     *
     * @param reg Register
     */
    protected void assertIsVarchar(CalcReg reg)
    {
        compilationAssert(
            reg.getOpType() == OpType.Varchar,
            "Register is not of type Varchar");
    }

    /**
     * Asserts that all operands are not null.
     *
     * @param operands Operands
     */
    protected void assertOperandsNotNull(Operand [] operands)
    {
        compilationAssert(operands != null, "Operands can not be null");

        for (int i = 0; i < operands.length; i++) {
            compilationAssert(null != operands[i], "Operand can not be null");
            ;
        }
    }

    /**
     * Adds a REF instruction.
     */
    public void addRef(
        CalcReg outputRegister,
        CalcReg src)
    {
        compilationAssert(
            outputRegister.getOpType() == src.getOpType(),
            "Type Mismatch. Tried to MOVE " + src.getOpType()
            + " into a " + outputRegister.getOpType());
        compilationAssert(
            RegisterSetType.Output == outputRegister.getRegisterType(),
            "Only output register allowed to reference other registers");

        refInstruction.add(this, outputRegister, src);
    }

    // Jump-related instructions----------------------

    protected void addJumpBooleanWithCondition(
        JumpInstructionDef op,
        int line,
        CalcReg reg)
    {
        op.add(this, line, reg);
    }

    /**
     * Adds an condtional JMP instruction. Jumps to <code>line</code> if the
     * value in <code>reg</code> is TRUE.
     *
     * @pre reg of Boolean Type
     * @pre line>=0
     * @post line < number of calls to addXxxInstruction methods
     */
    public void addJumpTrue(
        int line,
        CalcReg reg)
    {
        addJumpBooleanWithCondition(jumpTrueInstruction, line, reg);
    }

    public void addJumpFalse(
        int line,
        CalcReg reg)
    {
        addJumpBooleanWithCondition(jumpFalseInstruction, line, reg);
    }

    public void addJumpNull(
        int line,
        CalcReg reg)
    {
        addJumpBooleanWithCondition(jumpNullInstruction, line, reg);
    }

    public void addJumpNotNull(
        int line,
        CalcReg reg)
    {
        addJumpBooleanWithCondition(jumpNotNullInstruction, line, reg);
    }

    public void addReturn()
    {
        returnInstruction.add(this);
    }

    public void addLabelJump(String label)
    {
        jumpInstruction.add(
            this,
            label);
    }

    public void addLabelJumpTrue(
        String label,
        CalcReg reg)
    {
        jumpTrueInstruction.add(
            this,
            new Line(label),
            reg);
    }

    public void addLabelJumpFalse(
        String label,
        CalcReg reg)
    {
        jumpFalseInstruction.add(
            this,
            label,
            reg);
    }

    public void addLabelJumpNull(
        String label,
        CalcReg reg)
    {
        jumpNullInstruction.add(
            this,
            new Line(label),
            reg);
    }

    public void addLabelJumpNotNull(
        String label,
        CalcReg reg)
    {
        jumpNotNullInstruction.add(
            this,
            new Line(label),
            reg);
    }

    public void addLabel(String label)
    {
        compilationAssert(
            !labels.containsKey(label),
            "Label '" + label + "' already defined");
        int line = instructions.size();
        labels.put(
            label,
            line);
    }

    // -- Inner classes -------------------------------------------------------

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Represents an Operand in an opereation e.g. a register, line number or a
     * call to a function
     */
    interface Operand
    {
        /**
         * Serializes itself or parts of itself for transport over the wire
         *
         * @param writer
         */
        void print(PrintWriter writer);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Holds and represents the parameters for a call to an operator
     */
    public static class FunctionCall
        implements Operand
    {
        private String functionName;
        private CalcReg [] registers;
        private CalcReg result;

        /**
         * @param functionName e.g. <code>SUBSTR</code>
         * @param registers arguments for the function, must not be null
         */
        FunctionCall(
            CalcReg result,
            String functionName,
            CalcReg [] registers)
        {
            assert registers != null;
            this.result = result;
            this.functionName = functionName;
            this.registers = registers;
        }

        /**
         * Outputs itself in the following format:
         *
         * <blockquote><code>CALL 'funName(result, arg1, arg2, ...)</code>
         * </blockquote>
         *
         * @param writer
         */
        public void print(PrintWriter writer)
        {
            writer.print("'");
            writer.print(functionName);
            writer.print('(');
            result.print(writer);
            for (int i = 0; i < registers.length; i++) {
                writer.print(", ");
                registers[i].print(writer);
            }
            writer.print(')');
        }
    }

    /**
     * Reference to a line number
     */
    public static class Line
        implements Operand
    {
        java.lang.Integer line = null;
        String label = null;

        Line(int line)
        {
            this.line = line;
        }

        Line(String label)
        {
            this.label = label;
        }

        public String toString()
        {
            return (label != null) ? label
                : ((line != null) ? line.toString() : "null");
        }

        final public String getLabel()
        {
            return label;
        }

        final public void setLine(int line)
        {
            assert null == this.line;
            this.line = line;
        }

        final public java.lang.Integer getLine()
        {
            return line;
        }

        final public void print(PrintWriter writer)
        {
            writer.print("@");
            writer.print(line.intValue());
        }
    }

    public static class RegisterDescriptor
    {
        private final OpType type;
        private final int bytes;

        public RegisterDescriptor(
            OpType type,
            int bytes)
        {
            assert type != null;
            this.type = type;
            this.bytes = bytes;
        }

        public OpType getType()
        {
            return type;
        }

        public int getBytes()
        {
            return bytes;
        }
    }

    /**
     * Represents an instruction and its operands
     */
    class Instruction
    {
        private InstructionDef def;
        private Operand [] operands;
        private String comment;
        private Integer lineNumber;

        public Instruction(
            InstructionDef opCode,
            Operand [] operands)
        {
            this.def = opCode;
            this.operands = operands;
            comment = null;
            lineNumber = null;
        }

        final void print(PrintWriter writer)
        {
            writer.print(def.name);
            if ((null != operands) && (operands.length > 0)) {
                writer.print(' ');
                printOperands(writer, operands);
            }

            if (outputComments) {
                assert (null != lineNumber);
                String comment = lineNumber + ":";
                if (null != this.comment) {
                    comment += (" " + this.comment);
                }
                writer.print(formatComment(comment));
            }
            writer.print(separator);
        }

        final public Operand [] getOperands()
        {
            return operands;
        }

        public String getComment()
        {
            if (null == comment) {
                return "";
            }
            return comment;
        }

        public void setComment(String comment)
        {
            this.comment = comment;
        }

        public void setLineNumber(int lineNumber)
        {
            this.lineNumber = lineNumber;
        }
    }

    /**
     * A place holder to hold defined register sets
     */
    protected class RegisterSets
    {
        private final List<List<CalcReg>> sets =
            new ArrayList<List<CalcReg>>(RegisterSetType.ValueCount);

        RegisterSets()
        {
            for (int i = 0; i < RegisterSetType.ValueCount; ++i) {
                sets.add(new ArrayList<CalcReg>());
            }
        }

        public void clear()
        {
            for (List<CalcReg> set : sets) {
                set.clear();
            }
        }

        /**
         * Returns the list of registers of a given type.
         *
         * @param registerSetType Type of register
         *
         * @return list of registers of given type, never null
         */
        public final List<CalcReg> getRegisterList(
            RegisterSetType registerSetType)
        {
            return sets.get(registerSetType.ordinal());
        }

        /**
         * Creates a register in a register set
         *
         * @param opType what type the value in the register should have
         * @param initValue initial value
         * @param registerType specifies in which register set the register
         * should live
         *
         * @return the newly created Register
         */
        public CalcReg newRegister(
            OpType opType,
            Object initValue,
            RegisterSetType registerType,
            int storageBytes)
        {
            compilationAssert(opType != null, "null is an invalid OpType");
            compilationAssert(
                registerType != null,
                "null is an invalid RegisterSetType");

            int ordinal = registerType.ordinal();
            final List<CalcReg> set = sets.get(ordinal);
            CalcReg newReg =
                new CalcReg(
                    opType,
                    initValue,
                    registerType,
                    storageBytes,
                    set.size());
            set.add(newReg);
            return newReg;
        }
    }

    /**
     * Definition for an instruction. An instruction can only do one thing --
     * add itself to a program -- so this class is basically just a functor. The
     * concrete derived class must provide a name, and implement the {@link
     * #add(CalcProgramBuilder, List)} method.
     */
    static class InstructionDef
    {
        public final String name;
        protected final int regCount;

        InstructionDef(
            String name,
            int regCount)
        {
            this.name = name;
            this.regCount = regCount;
        }

        /**
         * Convenience method which converts the register list into an array and
         * calls the {@link #add(CalcProgramBuilder,CalcReg[])} method.
         */
        final void add(
            CalcProgramBuilder builder,
            List<CalcReg> registers)
        {
            add(
                builder,
                registers.toArray(new CalcReg[registers.size()]));
        }

        /**
         * Adds this instruction with a set of operands to a program.
         *
         * <p>The default implementation casts each operand to a {@link
         * CalcReg}, and calls {@link #add(CalcProgramBuilder, CalcReg[])}. If
         * this instruction's operands are not registers, override this method.
         *
         * @param builder Program builder
         * @param operands Operands
         */
        void add(
            CalcProgramBuilder builder,
            Operand ... operands)
        {
            CalcReg [] regs;
            if (operands instanceof CalcReg []) {
                regs = (CalcReg []) operands;
            } else {
                regs = new CalcReg[operands.length];
                System.arraycopy(operands, 0, regs, 0, operands.length);
            }
            add(builder, regs);
        }

        protected final void addInternal(
            CalcProgramBuilder builder,
            Operand [] regs)
        {
            assert regs.length == regCount : "Wrong number of params for instruction "
                + name;
            builder.assertOperandsNotNull(regs);
            builder.addInstruction(this, regs);
        }

        /**
         * Adds this instruction to a program.
         */
        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            addInternal(builder, regs);
        }

        public String toString()
        {
            return name;
        }
    }

    static class IntegralNativeInstructionDef
        extends InstructionDef
    {
        IntegralNativeInstructionDef(
            String name,
            int regCount)
        {
            super(name, regCount);
        }

        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            assert (this.regCount == regs.length);
            builder.assertRegisterNotConstant(regs[0]);
            super.add(builder, regs);
        }
    }

    static class NativeInstructionDef
        extends InstructionDef
    {
        NativeInstructionDef(
            String name,
            int regCount)
        {
            super(name, regCount);
        }

        /**
         * @pre result is not constant
         * @pre result is not of pointer type
         */
        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            assert (this.regCount == regs.length);
            assert (this.regCount > 1);
            assert (this.regCount <= 3);
            builder.assertRegisterNotConstant(regs[0]);
            builder.assertIsNativeType(regs[0]); //todo need precision checking
            builder.assertIsNativeType(regs[1]);

            if (regCount > 2) {
                builder.assertIsNativeType(regs[2]); //todo need precision
                                                     //checking
            }
            super.add(builder, regs);
        }
    }

    static class ComparisonInstructionDef
        extends InstructionDef
    {
        ComparisonInstructionDef(String name)
        {
            super(name, 3);
        }

        /**
         * @pre result is not constant
         * @pre result/op1/op2 are of type Boolean
         */
        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            assert (this.regCount == regs.length);
            builder.assertRegisterBool(regs[0]);
            builder.assertRegisterNotConstant(regs[0]);
            builder.compilationAssert(
                regs[1].getOpType() == regs[2].getOpType(),
                "Operand types must match");

            super.add(builder, regs);
        }
    }

    static class BoolInstructionDef
        extends InstructionDef
    {
        BoolInstructionDef(
            String name,
            int regCount)
        {
            super(name, regCount);
        }

        /**
         * @pre result is not constant
         * @pre result/op1/op2 are of type Boolean
         */
        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            assert (this.regCount == regs.length);
            assert (this.regCount > 1);
            assert (this.regCount <= 3);
            builder.assertRegisterBool(regs[0]);
            builder.assertRegisterNotConstant(regs[0]);
            builder.assertRegisterBool(regs[1]);

            if (regCount > 2) {
                builder.assertRegisterBool(regs[2]);
            }
            super.add(builder, regs);
        }
    }

    static class BoolNativeInstructionDef
        extends InstructionDef
    {
        BoolNativeInstructionDef(
            String name,
            int regCount)
        {
            super(name, regCount);
        }

        /**
         * @pre result is not constant
         * @pre result is of type Boolean
         */
        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            assert (this.regCount == regs.length);
            builder.assertRegisterNotConstant(regs[0]);

            //result must always be boolean
            builder.assertRegisterBool(regs[0]);
            super.add(builder, regs);
        }
    }

    static class PointerBoolInstructionDef
        extends InstructionDef
    {
        PointerBoolInstructionDef(
            String name,
            int regCount)
        {
            super(name, regCount);
        }

        /**
         * @pre result is not constant
         * @pre result is of type Boolean
         * @pre op1/op2 are of pointer type
         */
        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            assert (this.regCount == regs.length);
            assert (this.regCount > 1);
            assert (this.regCount <= 3);
            builder.assertRegisterNotConstant(regs[0]);
            builder.assertRegisterBool(regs[0]);
            builder.assertRegisterIsPointer(regs[1]);

            if (regCount > 2) {
                builder.assertRegisterIsPointer(regs[2]);
            }
            super.add(builder, regs);
        }
    }

    /**
     * Defines an extended instruction.
     */
    public static class ExtInstrDef
        extends InstructionDef
    {
        public ExtInstrDef(
            String name,
            int regCount)
        {
            super(name, regCount);
        }

        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            assert this.regCount == regs.length : "Wrong number of params for instruction "
                + name
                + "; expected=" + this.regCount + " but was=" + regs.length;
            add(builder, regs, name);
        }

        protected void add(
            CalcProgramBuilder builder,
            CalcReg [] regs,
            String funName)
        {
            CalcReg result = regs[0];
            CalcReg [] registers = new CalcReg[regs.length - 1];
            System.arraycopy(regs, 1, registers, 0, registers.length);
            builder.compilationAssert(result != null, "Result can not be null");
            builder.assertOperandsNotNull(registers);
            call.add(
                builder,
                new FunctionCall(result, funName, registers));
        }
    }

    /**
     * Defines an extended instruction with name depending on the number or
     * operands.
     */
    public static class ExtInstrSizeDef
        extends ExtInstrDef
    {
        public ExtInstrSizeDef(String name)
        {
            super(name, -1);
        }

        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            add(builder, regs, name + regs.length);
        }
    }

    static class IntegralNativeShift
        extends IntegralNativeInstructionDef
    {
        IntegralNativeShift(String name)
        {
            super(name, 3);
        }

        /**
         * @pre result is not constant
         * @pre op2 is of type Integer
         * @pre op2 is not negative if it is a constant (cant shift negative
         * steps)
         */
        void add(
            CalcProgramBuilder builder,
            CalcReg ... regs)
        {
            builder.assertRegisterNotConstant((CalcReg) regs[0]);
            CalcReg op2 = (CalcReg) regs[2];

            // second operand can only be either long or ulong
            builder.assertRegisterInteger(op2);

            // smart check: if a constant value that could be negative IS
            // negative, complain
            if ((op2.getRegisterType()
                    == RegisterSetType.Literal)
                && ((op2.getOpType() == OpType.Int4)
                    || (op2.getOpType() == OpType.Int8))
                && (op2.getValue() != null)
                && (op2.getValue() instanceof java.lang.Integer))
            {
                builder.compilationAssert(
                    ((java.lang.Integer) op2.getValue()).intValue() >= 0,
                    "Cannot shift negative amout of steps. Value="
                    + ((java.lang.Integer) op2.getValue()).intValue());
            }
            super.add(builder, regs);
        }
    }

    static class JumpInstructionDef
        extends InstructionDef
    {
        JumpInstructionDef(
            String name,
            int regCount)
        {
            super(name, regCount);
        }

        void add(
            CalcProgramBuilder builder,
            Operand ... operands)
        {
            if (operands[0] instanceof Line) {
                Line line = (Line) operands[0];
                if (line.line != null) {
                    builder.compilationAssert(
                        line.line >= 0,
                        "Line can not be negative. Value=" + line);
                }
            }
            if (regCount == 2) {
                builder.assertRegisterBool((CalcReg) operands[1]);
            }
            addInternal(builder, operands);
        }

        void add(CalcProgramBuilder builder, int line)
        {
            add(builder, new Line(line));
        }

        void add(CalcProgramBuilder builder, String label)
        {
            add(builder, new Line(label));
        }

        void add(CalcProgramBuilder builder, int line, CalcReg reg)
        {
            builder.assertRegisterBool(reg);
            add(builder, new Line(line));
        }

        void add(CalcProgramBuilder builder, String label, CalcReg reg)
        {
            builder.assertRegisterBool(reg);
            add(builder, new Line(label), reg);
        }
    }

    /**
     * A key-value pair class to hold<br>
     * 1) Value of a literal and<br>
     * 2) Type of a literal<br>
     * For use in a hashtable
     */
    private static class LiteralPair
    {
        OpType type;
        Object value;

        /**
         * @pre type!=null
         */
        LiteralPair(
            OpType type,
            Object value)
        {
            Util.pre(type != null, "type!=null");
            this.type = type;
            this.value = value;
        }

        public int hashCode()
        {
            int valueHash;
            if (null == value) {
                valueHash = 0;
            } else {
                valueHash = value.hashCode();
            }
            return (valueHash * (OpType.ValueCount + 2))
                + type.ordinal();
        }

        public boolean equals(Object o)
        {
            if (o instanceof LiteralPair) {
                LiteralPair that = (LiteralPair) o;

                if ((null == this.value) && (null == that.value)) {
                    return this.type == that.type;
                }

                if (null != this.value) {
                    return this.value.equals(that.value)
                        && (this.type == that.type);
                }
            }
            return false;
        }
    }
}

// End CalcProgramBuilder.java
