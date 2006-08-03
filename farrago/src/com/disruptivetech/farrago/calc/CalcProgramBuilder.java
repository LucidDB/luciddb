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

import java.io.*;

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

    // -- instructions -------------------------------------------------------
    static final String refInstruction = "REF";
    static final String jumpInstruction = "JMP";
    static final String jumpTrueInstruction = "JMPT";
    static final String jumpFalseInstruction = "JMPF";
    static final String jumpNullInstruction = "JMPN";
    static final String jumpNotNullInstruction = "JMPNN";
    static final String returnInstruction = "RETURN";
    static final String orInstruction = "OR";
    static final String notEqualInstruction = "NE";
    static final String moveInstruction = "MOVE";
    static final String lessEqualThanInstruction = "LE";
    static final String lessThanInstruction = "LT";
    static final String isNotNullInstruction = "ISNOTNULL";
    static final String isNullInstruction = "ISNULL";
    static final String greaterEqualThanInstruction = "GE";
    static final String greaterThanInstruction = "GT";
    static final String equalInstruction = "EQ";
    static final String andInstruction = "AND";
    static final String addInstruction = "ADD";
    public static final InstructionDef nativeAdd =
        new NativeInstructionDef(addInstruction, 3);
    public static final InstructionDef pointerAdd =
        new InstructionDef(addInstruction, 3) {
            void add(
                CalcProgramBuilder builder,
                CalcReg [] regs)
            {
                builder.assertRegisterNotConstant(regs[0]);
                builder.assertRegisterIsPointer(regs[0]);
                builder.assertRegisterIsPointer(regs[1]);
                builder.assertRegisterInteger(regs[2]);
                super.add(builder, regs);
            }
        };
    public static final InstructionDef boolAnd =
        new BoolInstructionDef(andInstruction, 3);
    public static final InstructionDef integralNativeAnd =
        new IntegralNativeInstructionDef(andInstruction, 3);
    public static final InstructionDef Cast =
        new NativeInstructionDef("CAST", 2);
    public static final InstructionDef nativeDiv =
        new NativeInstructionDef("DIV", 3) {
            void add(
                CalcProgramBuilder builder,
                CalcReg [] regs)
            {
                builder.assertNotDivideByZero(regs[2]);
                super.add(builder, regs);
            }
        };
    public static final InstructionDef boolNativeEqual =
        new BoolNativeInstructionDef(equalInstruction, 3);
    public static final InstructionDef boolEqual =
        new BoolInstructionDef(equalInstruction, 3);
    public static final InstructionDef pointerBoolEqual =
        new PointerBoolInstructionDef(equalInstruction, 3);
    public static final InstructionDef boolGreaterThan =
        new BoolInstructionDef(greaterThanInstruction, 3);
    public static final InstructionDef boolNativeGreaterThan =
        new BoolNativeInstructionDef(greaterThanInstruction, 3);
    public static final InstructionDef pointerBoolGreaterThan =
        new PointerBoolInstructionDef(greaterThanInstruction, 3);
    public static final InstructionDef boolGreaterOrEqualThan =
        new BoolInstructionDef(greaterEqualThanInstruction, 3);
    public static final InstructionDef boolNativeGreaterOrEqualThan =
        new BoolNativeInstructionDef(greaterEqualThanInstruction, 3);
    public static final InstructionDef pointerBoolGreaterOrEqualThan =
        new PointerBoolInstructionDef(greaterEqualThanInstruction, 3);
    public static final InstructionDef boolNativeIsNull =
        new BoolNativeInstructionDef(isNullInstruction, 2);
    public static final InstructionDef boolIsNull =
        new BoolInstructionDef(isNullInstruction, 2);
    public static final InstructionDef pointerBoolIsNull =
        new PointerBoolInstructionDef(isNullInstruction, 2);
    public static final InstructionDef boolNativeIsNotNull =
        new BoolNativeInstructionDef(isNotNullInstruction, 2);
    public static final InstructionDef boolIsNotNull =
        new BoolInstructionDef(isNotNullInstruction, 2);
    public static final InstructionDef pointerBoolIsNotNull =
        new PointerBoolInstructionDef(isNotNullInstruction, 2);
    public static final InstructionDef boolLessThan =
        new BoolInstructionDef(lessThanInstruction, 3);
    public static final InstructionDef boolNativeLessThan =
        new BoolNativeInstructionDef(lessThanInstruction, 3);
    public static final InstructionDef pointerBoolLessThan =
        new PointerBoolInstructionDef(lessThanInstruction, 3);
    public static final InstructionDef boolLessOrEqualThan =
        new BoolInstructionDef(lessEqualThanInstruction, 3);
    public static final InstructionDef boolNativeLessOrEqualThan =
        new BoolNativeInstructionDef(lessEqualThanInstruction, 3);
    public static final InstructionDef pointerBoolLessOrEqualThan =
        new PointerBoolInstructionDef(lessEqualThanInstruction, 3);
    public static final InstructionDef nativeMinus =
        new NativeInstructionDef("SUB", 3);
    public static final InstructionDef integralNativeMod =
        new IntegralNativeInstructionDef("MOD", 3) {
            void add(
                CalcProgramBuilder builder,
                CalcReg [] regs)
            {
                //check if divide by zero if op2 is a constant
                builder.assertNotDivideByZero(regs[2]);
                super.add(builder, regs);
            }
        };
    public static final InstructionDef move =
        new InstructionDef(moveInstruction, 2) {
            void add(
                CalcProgramBuilder builder,
                CalcReg [] regs)
            {
                builder.compilationAssert(
                    regs[0].getOpType() == regs[1].getOpType(),
                    "Type Mismatch. Tried to MOVE "
                    + regs[1].getOpType().getName() + " into a "
                    + regs[0].getOpType().getName());
                super.add(builder, regs);
            }
        };
    public static final InstructionDef boolMove =
        new BoolInstructionDef(moveInstruction, 2);
    public static final InstructionDef nativeMove =
        new NativeInstructionDef(moveInstruction, 2);
    public static final InstructionDef pointerMove =
        new InstructionDef(moveInstruction, 2) {
            void add(
                CalcProgramBuilder builder,
                CalcReg [] regs)
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
        new BoolNativeInstructionDef(notEqualInstruction, 3);
    public static final InstructionDef boolNotEqual =
        new BoolInstructionDef(notEqualInstruction, 3);
    public static final InstructionDef pointerBoolNotEqual =
        new PointerBoolInstructionDef(notEqualInstruction, 3);
    public static final InstructionDef boolOr =
        new BoolInstructionDef(orInstruction, 3);
    public static final InstructionDef integralNativeOr =
        new IntegralNativeInstructionDef(orInstruction, 3);
    public static final InstructionDef nativeNeg =
        new NativeInstructionDef("NEG", 2);
    public static final InstructionDef raise =
        new InstructionDef("RAISE", 1) {
            void add(
                CalcProgramBuilder builder,
                CalcReg [] regs)
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

    //~ Instance fields --------------------------------------------------------

    /* Member variables */
    protected String separator = SEPARATOR_SEMICOLON_NEWLINE;
    protected final List<Instruction> instructions =
        new ArrayList<Instruction>();
    protected RegisterSets registerSets = new RegisterSets();
    protected final Map<LiteralPair,CalcReg> literals =
        new HashMap<LiteralPair, CalcReg>();
    protected final Map<String,Integer> labels =
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
            && !separator.equals(SEPARATOR_SEMICOLON_NEWLINE)) {
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
        List<CalcReg> registerSet = registerSets.getSet(registerType.getOrdinal());
        return registerSet.get(ordinal);
    }

    /**
     * Replaces a label with a line number
     */
    private void bindReferences()
    {
        Iterator<Instruction> it = instructions.iterator();
        for (int i = 0; it.hasNext(); i++) {
            //Look for instructions that have Line as operands
            Instruction instruction = it.next();
            instruction.setLineNumber(i);
            Operand [] operands = instruction.getOperands();
            for (int j = 0; (null != operands) && (j < operands.length); j++) {
                Operand operand = operands[j];
                if (operand instanceof Line) {
                    Line line = (Line) operand;
                    if (line.getLabel() != null) //we have a label, update the
                                                 //line number with whats in the
                                                 //labels map
                    {
                        compilationAssert(null == line.getLine(),
                            "Line has already been bind.");
                        java.lang.Integer lineNumberFromLabel =
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
        List<CalcReg> list = registerSets.getSet(registerSetType.getOrdinal());
        if (null == list) {
            return;
        }
        writer.print(registerSetType.prefix);
        writer.print(" ");

        // Iterate over every register in the current set
        for (int j = 0; j < list.size(); j++) {
            if (j > 0) {
                writer.print(", ");
            }
            CalcReg reg = list.get(j);
            assert reg.getRegisterType() == registerSetType;
            writer.print(reg.getOpType().getName());
            switch (reg.getOpType().getOrdinal()) {
            case OpType.Binary_ordinal:
            case OpType.Char_ordinal:
            case OpType.Varbinary_ordinal:
            case OpType.Varchar_ordinal:
                assert reg.storageBytes >= 0;
                writer.print("," + reg.storageBytes);
                break;
            default:
                assert reg.storageBytes == -1 : reg.getOpType().getName();
            }
            if (false) {
                // Assembler doesn't support this.
                writer.print('[');
                writer.print(reg.getIndex());
                writer.print(']');
            }

            if (reg.getValue() != null) {
                compilationAssert(registerSetType == RegisterSetType.Literal,
                    "Only literals have values");
            }
        }
        writer.print(separator);
    }

    private void generateRegValues(PrintWriter writer)
    {
        List<CalcReg> list = registerSets.getSet(RegisterSetType.LiteralORDINAL);
        if (null == list) {
            return;
        }
        writer.print("V ");
        for (int j = 0; j < list.size(); j++) {
            if (j > 0) {
                writer.print(", ");
            }
            CalcReg reg = list.get(j);

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
        Iterator<Instruction> it = instructions.iterator();
        for (int i = 0; it.hasNext(); i++) {
            Instruction inst = it.next();
            String op = inst.getOpCode();

            //this try-catch clause will pick up any compiler excpetions
            //messages and wrap it into a msg containg what line went wrong
            try {
                //-----------Check if any jump instructions are jumping off the
                //cliff
                if (op.startsWith(jumpInstruction)) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue() >= instructions.size()) {
                        throw FarragoResource.instance().ProgramCompilationError
                        .ex("Line "
                            + line.getLine() + " doesn't exist");
                    }
                }

                //-----------Check if any jump instructions jumps to itself
                if (op.startsWith(jumpInstruction)) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue() == i) {
                        throw FarragoResource.instance().ProgramCompilationError
                        .ex(
                            "Cannot jump to the same line as the instruction");
                    }
                }

                //-----------Forbidding loops. Check if any jump instructions
                //jumps to a previous line.
                if (op.startsWith(jumpInstruction)) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue() < i) {
                        throw FarragoResource.instance().ProgramCompilationError
                        .ex(
                            "Loops are forbidden. Cannot jump to a previous line");
                    }
                }

                //TODO add to see that all declared registers are beeing
                //referenced TODO add to see if output registers where declared
                //(?) TODO add to see if all output registers where written to
                //TODO add to see if a non constant register is used before
                //assigned to ----------- Other post validation goes here ...
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
        return
            registerSets.newRegister(type,
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
     * @param type {@link OpType} Type of value
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
                registerSets.newRegister(type,
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
                new java.lang.Integer(i),
                -1);
    }

    public CalcReg newInt8Literal(int i) //REVIEW shouldnt this be a long type?

    {
        return newLiteral(
                OpType.Int8,
                new java.lang.Integer(i),
                -1);
    }

    public CalcReg newUint4Literal(int i)
    {
        compilationAssert(i >= 0,
            "Unsigned value was found to be negative. Value=" + i);
        return newLiteral(
                OpType.Uint4,
                new java.lang.Integer(i),
                -1);
    }

    public CalcReg newUint8Literal(int i) //REVIEW shouldnt this be a long
                                           //type?

    {
        compilationAssert(i >= 0,
            "Unsigned value was found to be negative. Value=" + i);
        return newLiteral(
                OpType.Uint8,
                new java.lang.Integer(i),
                -1);
    }

    public CalcReg newFloatLiteral(float f)
    {
        return newLiteral(
                OpType.Real,
                new java.lang.Float(f),
                -1);
    }

    public CalcReg newDoubleLiteral(double d)
    {
        return newLiteral(
                OpType.Double,
                new java.lang.Double(d),
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
        return
            registerSets.newRegister(type,
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
        return
            registerSets.newRegister(type,
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
        return
            registerSets.newRegister(type,
                null,
                RegisterSetType.Status,
                storageBytes);
    }

    //---------------------------------------
    //Instruction Creation
    protected void addInstruction(
        String operator,
        Operand op1)
    {
        addInstruction(
            operator,
            new Operand[] { op1 });
    }

    protected void addInstruction(
        String operator,
        Operand op1,
        Operand op2)
    {
        addInstruction(
            operator,
            new Operand[] { op1, op2 });
    }

    protected void addInstruction(
        String operator,
        Operand op1,
        Operand op2,
        Operand op3)
    {
        addInstruction(
            operator,
            new Operand[] { op1, op2, op3 });
    }

    protected void addInstruction(
        String operator,
        Operand [] operands)
    {
        assertOperandsNotNull(operands);
        addInstruction(new Instruction(operator, operands));
    }

    protected void addInstruction(String operator)
    {
        addInstruction(new Instruction(operator, null));
    }

    protected void addInstruction(Instruction inst)
    {
        instructions.add(inst);
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
        Instruction inst =
            (Instruction) instructions.toArray()[instructions.size() - 1];
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
        return
            " /* "

            /* all 6 \'s are needed */
            + comment.replaceAll("/\\*", "\\\\\\*").replaceAll("\\*/",
                "\\*\\\\")
            + " */";
    }

    // assert helper functions---

    /**
     * Asserts that the register is not declared as {@link
     * RegisterSetType#Literal} or {@link RegisterSetType#Input}
     *
     * @param result
     */
    protected void assertRegisterNotConstant(CalcReg result)
    {
        compilationAssert(
            (
                result.getRegisterType().getOrdinal()
                != RegisterSetType.LiteralORDINAL
            )
            && (
                result.getRegisterType().getOrdinal()
                != RegisterSetType.InputORDINAL
               ),
            "Expected a non constant register. Constant registers are Literals and Inputs");
    }

    /**
     * Asserts that the register is declared as {@link RegisterSetType#Literal}
     *
     * @param result
     */
    protected void assertRegisterLiteral(CalcReg result)
    {
        compilationAssert(
            result.getRegisterType().getOrdinal()
            == RegisterSetType.LiteralORDINAL,
            "Expected a Literal register.");
    }

    /**
     * Asserts that the register is declared as {@link OpType#Bool}
     *
     * @param reg
     */
    protected void assertRegisterBool(CalcReg reg)
    {
        compilationAssert(reg.getOpType().getOrdinal() == OpType.Bool_ordinal,
            "Expected a register of Boolean type. " + "Found "
            + reg.getOpType().getName());
    }

    /**
     * Asserts that the register is declared as a pointer. Pointers are {@link
     * OpType#Varchar},{@link OpType#Varbinary}
     *
     * @param reg
     */
    protected void assertRegisterIsPointer(CalcReg reg)
    {
        compilationAssert(
            (reg.getOpType().getOrdinal() == OpType.Varbinary_ordinal)
            || (reg.getOpType().getOrdinal() == OpType.Varchar_ordinal),
            "Expected a register of Pointer type");
    }

    /**
     * Asserts that the register is declared as integer. Integers are {@link
     * OpType#Int4}, {@link OpType#Int8}, {@link OpType#Uint4},{@link
     * OpType#Uint8}
     *
     * @param reg
     */
    protected void assertRegisterInteger(CalcReg reg)
    {
        compilationAssert(
            (reg.getOpType().getOrdinal() == OpType.Int4_ordinal)
            || (reg.getOpType().getOrdinal() == OpType.Int8_ordinal)
            || (reg.getOpType().getOrdinal() == OpType.Uint4_ordinal)
            || (reg.getOpType().getOrdinal() == OpType.Uint8_ordinal),
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
     * @param reg
     */
    protected void assertNotDivideByZero(CalcReg reg)
    {
        if ((
                reg.getRegisterType().getOrdinal()
                == RegisterSetType.LiteralORDINAL
            )
            && (reg.getValue() != null)
            && (reg.getValue() instanceof java.lang.Integer)) {
            compilationAssert(!reg.getValue().equals(new Integer(0)),
                "A literal register of Integer type and value=0 was found");
        }
    }

    /**
     * Asserts input is of native type except booleans
     *
     * @param register
     */
    protected void assertIsNativeType(CalcReg register)
    {
        compilationAssert(
            (register.getOpType().getOrdinal() == OpType.Int1_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Uint1_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Int2_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Uint2_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Int4_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Int8_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Uint4_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Uint8_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Real_ordinal)
            || (register.getOpType().getOrdinal() == OpType.Double_ordinal),
            "Register is not of native OpType");
    }

    /**
     * Asserts input is of type {@link OpType#Varchar}
     *
     * @param register
     */
    protected void assertIsVarchar(CalcReg register)
    {
        compilationAssert(
            register.getOpType().getOrdinal() == OpType.Varchar_ordinal,
            "Register is not of type Varchar");
    }

    /**
     * Asserts that all operands are not null
     *
     * @param operands
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
     * Adds a move instruction.
     *
     * @deprecated
     */
    public void addMove(
        CalcReg target,
        CalcReg src)
    {
        move.add(
            this,
            new CalcReg[] { target, src });
    }

    /**
     * Adds a REF instruction.
     */
    public void addRef(
        CalcReg outputRegister,
        CalcReg src)
    {
        compilationAssert(outputRegister.getOpType() == src.getOpType(),
            "Type Mismatch. Tried to MOVE " + src.getOpType().getName()
            + " into a " + outputRegister.getOpType().getName());
        compilationAssert(
            RegisterSetType.Output == outputRegister.getRegisterType(),
            "Only output register allowed to reference other registers");

        addInstruction(refInstruction, outputRegister, src);
    }

    // Jump related instructions----------------------

    /**
     * Adds an uncondtional JMP instruction
     *
     * @param line
     */
    public void addJump(int line)
    {
        compilationAssert(line >= 0, "Line can not be negative. Value=" + line);
        addInstruction(
            jumpInstruction,
            new Operand[] { new Line(line) });
    }

    protected void addJumpBooleanWithCondition(
        String op,
        int line,
        CalcReg reg)
    {
        compilationAssert(line >= 0, "Line can not be negative. Value=" + line);
        addJumpBooleanWithCondition(
            op,
            new Line(line),
            reg);
    }

    protected void addJumpBooleanWithCondition(
        String op,
        Line line,
        CalcReg reg)
    {
        assertRegisterBool(reg);

        //reg != null is checked for in addInstuction
        addInstruction(op, line, reg);
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
        addInstruction(returnInstruction);
    }

    public void addLabelJump(String label)
    {
        addInstruction(
            jumpInstruction,
            new Operand[] { new Line(label) });
    }

    public void addLabelJumpTrue(
        String label,
        CalcReg reg)
    {
        addJumpBooleanWithCondition(
            jumpTrueInstruction,
            new Line(label),
            reg);
    }

    public void addLabelJumpFalse(
        String label,
        CalcReg reg)
    {
        addJumpBooleanWithCondition(
            jumpFalseInstruction,
            new Line(label),
            reg);
    }

    public void addLabelJumpNull(
        String label,
        CalcReg reg)
    {
        addInstruction(
            jumpNullInstruction,
            new Line(label),
            reg);
    }

    public void addLabelJumpNotNull(
        String label,
        CalcReg reg)
    {
        addInstruction(
            jumpNotNullInstruction,
            new Line(label),
            reg);
    }

    public void addLabel(String label)
    {
        compilationAssert(!labels.containsKey(label),
            "Label '" + label + "' already defined");
        int line = instructions.size();
        labels.put(
            label,
            new java.lang.Integer(line));
    }

    // Bool related instructions----------------------

    /**
     * @deprecated
     */
    public void addBoolAnd(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolAnd.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolOr(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolOr.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolEqual.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolNotEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolNotEqual.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolGreaterThan(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolGreaterThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolLessThan(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolLessThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolNot(
        CalcReg result,
        CalcReg op1)
    {
        boolNot.add(
            this,
            new CalcReg[] { result, op1 });
    }

    /**
     * @deprecated
     */
    public void addBoolMove(
        CalcReg result,
        CalcReg op1)
    {
        boolMove.add(
            this,
            new CalcReg[] { result, op1 });
    }

    /**
     * @deprecated
     */
    public void addBoolIsNull(
        CalcReg result,
        CalcReg op1)
    {
        boolIsNull.add(
            this,
            new CalcReg[] { result, op1 });
    }

    /**
     * @deprecated
     */
    public void addBoolIsNotNull(
        CalcReg result,
        CalcReg op1)
    {
        boolIsNotNull.add(
            this,
            new CalcReg[] { result, op1 });
    }

    // Native instructions----------------------

    /**
     * @deprecated
     */
    public void addNativeAdd(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        nativeAdd.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addNativeSub(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        nativeMinus.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addNativeDiv(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        //smart check, check if divide by zero if op2 is a constant
        nativeDiv.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addNativeNeg(
        CalcReg result,
        CalcReg op1)
    {
        nativeNeg.add(
            this,
            new CalcReg[] { result, op1 });
    }

    /**
     * @deprecated
     */
    public void addNativeMove(
        CalcReg result,
        CalcReg op1)
    {
        nativeMove.add(
            this,
            new CalcReg[] { result, op1 });
    }

    //Integral Native Instructions ---------------------------------

    /**
     * @deprecated
     */
    public void addIntegralNativeMod(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        integralNativeMod.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addIntegralNativeShiftLeft(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        integralNativeShiftLeft.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addIntegralNativeShiftRight(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        integralNativeShiftRight.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addIntegralNativeAnd(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        integralNativeAnd.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addIntegralNativeOr(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        integralNativeOr.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addIntegralNativeXor(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        integralNativeXor.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    //Bool Native Instructions --------------------------------

    /**
     * @deprecated
     */
    public void addBoolNativeEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolNativeEqual.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolNativeNotEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolNativeNotEqual.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolNativeGreaterThan(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolNativeGreaterThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolNativeGreaterOrEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolNativeGreaterOrEqualThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolNativeLessThan(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolNativeLessThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolNativeLessOrEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        boolNativeLessOrEqualThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addBoolNativeIsNull(
        CalcReg result,
        CalcReg op1)
    {
        boolNativeIsNull.add(
            this,
            new CalcReg[] { result, op1 });
    }

    /**
     * @deprecated
     */
    public void addBoolNativeIsNotNull(
        CalcReg result,
        CalcReg op1)
    {
        boolNativeIsNotNull.add(
            this,
            new CalcReg[] { result, op1 });
    }

    //Pointer Instructions --------------------------------

    /**
     * @deprecated
     */
    public void addPointerMove(
        CalcReg result,
        CalcReg op1)
    {
        pointerMove.add(
            this,
            new CalcReg[] { result, op1 });
    }

    /**
     * @deprecated
     */
    public void addPointerAdd(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        pointerAdd.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addPointerEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        pointerBoolEqual.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addPointerNotEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        pointerBoolNotEqual.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addPointerGreaterThan(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        pointerBoolGreaterThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addPointerGreaterOrEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        pointerBoolGreaterOrEqualThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addPointerLessThan(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        pointerBoolLessThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addPointerLessOrEqual(
        CalcReg result,
        CalcReg op1,
        CalcReg op2)
    {
        pointerBoolLessOrEqualThan.add(
            this,
            new CalcReg[] { result, op1, op2 });
    }

    /**
     * @deprecated
     */
    public void addPointerIsNull(
        CalcReg result,
        CalcReg op1)
    {
        pointerBoolIsNull.add(
            this,
            new CalcReg[] { result, op1 });
    }

    /**
     * @deprecated
     */
    public void addPointerIsNotNull(
        CalcReg result,
        CalcReg op1)
    {
        pointerBoolIsNotNull.add(
            this,
            new CalcReg[] { result, op1 });
    }

    //~ Inner Interfaces -------------------------------------------------------

    // -- Inner classes -------------------------------------------------------

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
    public class Line
        implements Operand
    {
        java.lang.Integer line = null;
        String label = null;

        Line(int line)
        {
            this.line = new java.lang.Integer(line);
        }

        Line(String label)
        {
            this.label = label;
        }

        final public String getLabel()
        {
            return label;
        }

        final public void setLine(int line)
        {
            compilationAssert(null == this.line);
            this.line = new java.lang.Integer(line);
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

    /**
     * Enumeration of the types supported by the calculator. These types map
     * onto the {@link FennelStandardTypeDescriptor} values, and even have the
     * same names and ordinals.
     */
    public static class OpType
        implements EnumeratedValues.Value
    {
        public static final int Bool_ordinal =
            FennelStandardTypeDescriptor.BOOL_ORDINAL;
        public static final OpType Bool =
            new OpType(FennelStandardTypeDescriptor.BOOL);
        public static final int Int1_ordinal =
            FennelStandardTypeDescriptor.INT_8_ORDINAL;
        public static final OpType Int1 =
            new OpType(FennelStandardTypeDescriptor.INT_8);
        public static final int Uint1_ordinal =
            FennelStandardTypeDescriptor.UINT_8_ORDINAL;
        public static final OpType Uint1 =
            new OpType(FennelStandardTypeDescriptor.UINT_8);
        public static final int Int2_ordinal =
            FennelStandardTypeDescriptor.INT_16_ORDINAL;
        public static final OpType Int2 =
            new OpType(FennelStandardTypeDescriptor.INT_16);
        public static final int Uint2_ordinal =
            FennelStandardTypeDescriptor.UINT_16_ORDINAL;
        public static final OpType Uint2 =
            new OpType(FennelStandardTypeDescriptor.UINT_16);
        public static final int Int4_ordinal =
            FennelStandardTypeDescriptor.INT_32_ORDINAL;
        public static final OpType Int4 =
            new OpType(FennelStandardTypeDescriptor.INT_32);
        public static final int Uint4_ordinal =
            FennelStandardTypeDescriptor.UINT_32_ORDINAL;
        public static final OpType Uint4 =
            new OpType(FennelStandardTypeDescriptor.UINT_32);
        public static final int Real_ordinal =
            FennelStandardTypeDescriptor.REAL_ORDINAL;
        public static final OpType Real =
            new OpType(FennelStandardTypeDescriptor.REAL);
        public static final int Int8_ordinal =
            FennelStandardTypeDescriptor.INT_64_ORDINAL;
        public static final OpType Int8 =
            new OpType(FennelStandardTypeDescriptor.INT_64);
        public static final int Uint8_ordinal =
            FennelStandardTypeDescriptor.UINT_64_ORDINAL;
        public static final OpType Uint8 =
            new OpType(FennelStandardTypeDescriptor.UINT_64);
        public static final int Double_ordinal =
            FennelStandardTypeDescriptor.DOUBLE_ORDINAL;
        public static final OpType Double =
            new OpType(FennelStandardTypeDescriptor.DOUBLE);
        public static final int Varbinary_ordinal =
            FennelStandardTypeDescriptor.VARBINARY_ORDINAL;
        public static final OpType Varbinary =
            new OpType(FennelStandardTypeDescriptor.VARBINARY);
        public static final int Varchar_ordinal =
            FennelStandardTypeDescriptor.VARCHAR_ORDINAL;
        public static final OpType Varchar =
            new OpType(FennelStandardTypeDescriptor.VARCHAR);
        public static final int Binary_ordinal =
            FennelStandardTypeDescriptor.BINARY_ORDINAL;
        public static final OpType Binary =
            new OpType(FennelStandardTypeDescriptor.BINARY);
        public static final int Char_ordinal =
            FennelStandardTypeDescriptor.CHAR_ORDINAL;
        public static final OpType Char =
            new OpType(FennelStandardTypeDescriptor.CHAR);

        public static final EnumeratedValues enumeration =
            new EnumeratedValues(
                new OpType[] {
                    Int1, Uint1, Int2, Uint2, Bool, Int4, Uint4, Real, Int8,
                Uint8, Double, Varbinary, Varchar, Binary, Char,
                });
        private final FennelStandardTypeDescriptor type;

        private OpType(FennelStandardTypeDescriptor type)
        {
            this.type = type;
        }

        public String getDescription()
        {
            return null;
        }

        public String getName()
        {
            return type.getName();
        }

        public int getOrdinal()
        {
            return type.getOrdinal();
        }

        public String toString()
        {
            return getName();
        }

        /**
         * Returns an exception indicating that we didn't expect to find this
         * value here.
         */
        public Error unexpected()
        {
            return enumeration.unexpected(this);
        }

        public boolean isExact()
        {
            return type.isExact();
        }

        public boolean isApprox()
        {
            return type.isNumeric() && !type.isExact();
        }

        public boolean isNumeric()
        {
            return type.isNumeric();
        }

        // implement Comparable
        public int compareTo(Object other)
        {
            assert (other instanceof OpType);
            OpType otherValue = (OpType) other;
            return getOrdinal() - otherValue.getOrdinal();
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

    /* Represents an instruction and its operands */
    class Instruction
    {
        private String opCode;
        private Operand [] operands;
        private String comment;
        private Integer lineNumber;

        public Instruction(
            String opCode,
            Operand [] operands)
        {
            this.opCode = opCode;
            this.operands = operands;
            comment = null;
            lineNumber = null;
        }

        final void print(PrintWriter writer)
        {
            writer.print(opCode);
            if (null != operands) {
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

        final public String getOpCode()
        {
            return opCode;
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

        public void setLineNumber(int line)
        {
            lineNumber = new Integer(line);
        }
    }

    /**
     * Enumeration of register types
     */
    public static class RegisterSetType
        extends EnumeratedValues.BasicValue
    {
        public static final int OutputORDINAL = 0;
        public static final RegisterSetType Output =
            new RegisterSetType("output", OutputORDINAL, 'O');
        public static final int InputORDINAL = 1;
        public static final RegisterSetType Input =
            new RegisterSetType("input", InputORDINAL, 'I');
        public static final int LiteralORDINAL = 2;
        public static final RegisterSetType Literal =
            new RegisterSetType("literal", LiteralORDINAL, 'C');
        public static final int LocalORDINAL = 3;
        public static final RegisterSetType Local =
            new RegisterSetType("local", LocalORDINAL, 'L');
        public static final int StatusORDINAL = 4;
        public static final RegisterSetType Status =
            new RegisterSetType("status", StatusORDINAL, 'S');
        public static final EnumeratedValues enumeration =
            new EnumeratedValues(
                new RegisterSetType[] {
                    Output, Input, Literal, Local, Status
                });
        final char prefix;

        private RegisterSetType(
            String name,
            int ordinal,
            char prefix)
        {
            super(name, ordinal, null);
            this.prefix = prefix;
        }

        public static RegisterSetType get(int ordinal)
        {
            return (RegisterSetType) enumeration.getValue(ordinal);
        }
    }

    /**
     * A place holder to hold defined register sets
     */
    protected class RegisterSets
    {
        private final List<CalcReg> [] sets =
            new ArrayList[RegisterSetType.enumeration.getSize()];

        public void clear()
        {
            for (int i = 0; i < sets.length; i++) {
                if (sets[i] != null) {
                    sets[i].clear();
                }
            }
        }

        public final List<CalcReg> getSet(int set)
        {
            return sets[set];
        }

        /**
         * Creates a register in a register set
         *
         * @param opType what type the value in the register should have
         * @param initValue
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
            compilationAssert(registerType != null,
                "null is an invalid RegisterSetType");

            int set = registerType.getOrdinal();
            if (null == sets[set]) {
                sets[set] = new ArrayList<CalcReg>();
            }

            CalcReg newReg =
                new CalcReg(opType,
                    initValue,
                    registerType,
                    storageBytes,
                    sets[set].size());
            sets[set].add(newReg);
            return newReg;
        }
    }

    /**
     * Definition for an instruction. An instruction can only do one thing --
     * add itself to a program -- so this class is basically just a functor. The
     * concrete derived class must provide a name, and implement the {@link
     * #add(CalcProgramBuilder,Register[])} method.
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
            add(builder,
                (CalcReg []) registers.toArray(
                    new CalcReg[registers.size()]));
        }

        final void add(
            CalcProgramBuilder builder,
            CalcReg reg0)
        {
            add(
                builder,
                new CalcReg[] { reg0 });
        }

        final void add(
            CalcProgramBuilder builder,
            CalcReg reg0,
            CalcReg reg1)
        {
            add(
                builder,
                new CalcReg[] { reg0, reg1 });
        }

        final void add(
            CalcProgramBuilder builder,
            CalcReg reg0,
            CalcReg reg1,
            CalcReg reg2)
        {
            add(
                builder,
                new CalcReg[] { reg0, reg1, reg2 });
        }

        /**
         * Adds this instruction to a program.
         */
        void add(
            CalcProgramBuilder builder,
            CalcReg [] regs)
        {
            assert regs.length == regCount : "Wrong number of params for instruction "
                + name;
            builder.assertOperandsNotNull(regs);
            builder.addInstruction(name, regs);
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
            CalcReg [] regs)
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
            CalcReg [] regs)
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
            CalcReg [] regs)
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
            CalcReg [] regs)
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
            CalcReg [] regs)
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
    static class ExtInstrDef
        extends InstructionDef
    {
        ExtInstrDef(
            String name,
            int regCount)
        {
            super(name, regCount);
        }

        void add(
            CalcProgramBuilder builder,
            CalcReg [] regs)
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
            builder.addInstruction(
                "CALL",
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
            CalcReg [] regs)
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
            CalcReg [] regs)
        {
            builder.assertRegisterNotConstant(regs[0]);
            CalcReg op2 = regs[2];

            //second operand can only be either long or ulong
            builder.assertRegisterInteger(op2);

            //smart check, if a constant value that could be negative IS
            //negative, complain
            if ((
                    op2.getRegisterType().getOrdinal()
                    == RegisterSetType.LiteralORDINAL
                )
                && (
                    (op2.getOpType().getOrdinal() == OpType.Int4_ordinal)
                    || (op2.getOpType().getOrdinal() == OpType.Int8_ordinal)
                   )
                && (op2.getValue() != null)
                && (op2.getValue() instanceof java.lang.Integer)) {
                builder.compilationAssert(
                    ((java.lang.Integer) op2.getValue()).intValue() >= 0,
                    "Cannot shift negative amout of steps. Value="
                    + ((java.lang.Integer) op2.getValue()).intValue());
            }
            super.add(builder, regs);
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
            return
                (valueHash * (OpType.enumeration.getMax() + 2))
                + type.getOrdinal();
        }

        public boolean equals(Object o)
        {
            if (o instanceof LiteralPair) {
                LiteralPair that = (LiteralPair) o;

                if ((null == this.value) && (null == that.value)) {
                    return this.type.getOrdinal() == that.type.getOrdinal();
                }

                if (null != this.value) {
                    return
                        this.value.equals(that.value)
                        && (
                            this.type.getOrdinal()
                            == that.type.getOrdinal()
                           );
                }
            }
            return false;
        }
    }
}

// End CalcProgramBuilder.java
