/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004 Disruptive Technologies, Inc.
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
package net.sf.saffron.calc;

import net.sf.saffron.util.EnumeratedValues;
import net.sf.saffron.util.Util;
import net.sf.saffron.resource.SaffronResource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Constructs a calculator assembly language program based upon a series
 * of calls made by the client.
 *
 * <p>If you want multi-line programs, call
 * <code>setSeparator(System.getProperty("line.separator")</code>.</p>

 * @testcase {@link CalcProgramBuilder}
 * @author jhyde
 * @since Jan 11, 2004
 * @version $Id$
 **/
public class CalcProgramBuilder {

   /* Constants */
    private static final String NL = System.getProperty("line.separator");
    public static final String SEPARATOR_SEMICOLON = ";";
    public static final String SEPARATOR_NEWLINE = NL;
    public static final String SEPARATOR_SEMICOLON_NEWLINE = ";"+NL;

    /* Member variables */
    protected String m_separator = SEPARATOR_SEMICOLON_NEWLINE;
    protected final ArrayList m_instructions = new ArrayList();
    protected RegisterSets m_registerSets = new RegisterSets();
    protected final HashMap m_literals = new HashMap();
    protected final HashMap m_labels = new HashMap();

    public CalcProgramBuilder() {

    }

    // -- operators -------------------------------------------------------
    static final String moveOperator = "MOVE";
    static final String extendedOperator = "EXT";
    static final String jumpOperator = "JMP";
    static final String jumpTrueOperator = "JMPT";
    static final String jumpFalseOperator = "JMPF";
    static final String jumpNullOperator = "JMPN";
    static final String jumpNotNullOperator = "JMPNN";
    static final String andOperator = "AND";
    static final String orOperator = "OR";
    static final String xorOperator = "XOR";
    static final String equalOperator = "EQ";
    static final String notEqualOperator = "NE";
    static final String greaterThanOperator = "GT";
    static final String greaterOrEqualOperator = "GE";
    static final String lessThenOperator = "LT";
    static final String lessOrEqualOperator = "LE";
    static final String notOperator = "NOT";
    static final String isNullOperator = "ISNULL";
    static final String isNotNullOperator = "ISNOTNULL";
    static final String addOperator = "ADD";
    static final String subOperator = "SUB";
    static final String mulOperator = "MUL";
    static final String divOperator = "DIV";
    static final String negOperator = "NEG";
    static final String modOperator = "MOD";
    static final String shflOperator = "SHFL";
    static final String shfrOperator = "SHFR";
    static final String returnOperation = "RETURN";


    // -- Inner classes -------------------------------------------------------

    /**
     * Represents an Operand in an opereation
     * e.g. a register, line number or a call to a function
     * */
    interface Operand
    {
        /**
         * Serializes itself or parts of itself for transport over the wire
         * @param writer
         */
        void print(PrintWriter writer);
    }


    /**
     * Holds and represents the parameters for the {@link #callOperator} operator
     */
    public class FunctionCall implements Operand
    {
        private String m_functionName;
        private Register[] m_registers;
        private Register m_result;

        /**
         * @param functionName e.g. <code>SUBSTR</code>
         * @param registers parameters for the e.g. <code>SUBSTR</code> function
         */
        FunctionCall(Register result, String functionName, Register[] registers)
        {
            m_result = result;
            m_functionName=functionName;
            m_registers=registers;
        }

        /**
         * Outputs itself in the following format:<br>
         * <code>result, 'function name<paramType1, paramType2, ...>', param1, param2, ...</code>
         * @param writer
         */
        public void print(PrintWriter writer)
        {
            printOperands(writer, new Operand[]{m_result});
            writer.print(", '");
            writer.print(m_functionName);
            writer.print('<');
            if (null!=m_registers)
            {
                for(int i=0;i<m_registers.length;i++)
                {
                    writer.print(m_registers[i].getOpType().getName());
                    if (i+1<m_registers.length)
                    {
                        writer.print(", ");
                    }
                }
            }
            writer.print(">'");
            if (null!=m_registers && m_registers.length>0)
            {
                writer.print(", ");
                printOperands(writer, m_registers);
            }
        }
    }

    /**
     * Represents a virtual register.
     * Each register lives in a register set of type {@link RegisterSetType}<br>
     * Each register is of type {@link OpType}
     */
    public class Register implements Operand
    {
        OpType m_opType;
        Object m_value;
        RegisterSetType m_registerType;
        int m_index;

        final public OpType getOpType() { return m_opType; }
        final RegisterSetType getRegisterType() { return m_registerType; }
        final int getIndex() { return m_index; }
        final Object getValue() { return m_value; }

        /**
         * Serializes the {@link #m_value} in the virtual register if not null<br>
         * <b>NOTE</b> See also {@link #print} which serializes the "identity" of the register
         * @param writer
         */
        void printValue(PrintWriter writer)
        {
            if (m_value instanceof String)
            {
                writer.print("'");
                writer.print(m_value);
                writer.print("'");
            }
            else if (m_value instanceof byte[])
            {
                writer.print("'");
                writer.print(Util.toStringFromByteArray((byte[]) m_value,16));
                writer.print("'");
            }
            else if (null!=m_value)
            {
                writer.print(m_value.toString());
            }
        }

        Register(OpType opType, Object value, RegisterSetType registerType, int index)
        {
            m_opType=opType;
            m_value=value;
            m_registerType=registerType;
            m_index=index;
        }

        /**
         * Serializes the identity of the register. It does not attempt to serialize its value
         * see {@link #printValue} for that
         * @param writer
         */
        final public void print(PrintWriter writer)
        {
            writer.print(m_registerType.getPrefix());
            //writer.print(m_type.getTypeCode());
            writer.print(m_index);
        }
    }

    /**
     * Reference to a line number
     */
    public class Line implements Operand
    {
        java.lang.Integer m_line=null;
        String m_label = null;

        Line(int line)
        {
            m_line=new java.lang.Integer(line);
        }

        Line(String label)
        {
            m_label=label;
        }

        final public String getLabel()
        {
            return m_label;
        }

        final public void setLine(int line)
        {
            compilationAssert(null==m_line);
            m_line = new java.lang.Integer(line);
        }

        final public java.lang.Integer getLine()
        {
            return m_line;
        }

        final public void print(PrintWriter writer)
        {
            writer.print(m_line.intValue());
        }
    }

    /**
     * Enumeration of the types supported by the calculator.
     */
    public static class OpType extends EnumeratedValues.BasicValue {

        private OpType(String name, int ordinal) {
            super(name, ordinal, null);
        }

        private static final int BooleanORDINAL = 0;
        public static final OpType Boolean = new OpType("u1", BooleanORDINAL);

        public static final int IntORDINAL = 1;
        public static final OpType Int = new OpType("s4", IntORDINAL);

        private static final int UIntORDINAL = 2;
        public static final OpType UInt = new OpType("u4", UIntORDINAL);

        private static final int RealORDINAL = 3;
        public static final OpType Real = new OpType("r", RealORDINAL);

        private static final int LongLongORDINAL = 4;
        public static final OpType LongLong = new OpType("s8", LongLongORDINAL);

        private static final int ULongLongORDINAL = 5;
        public static final OpType ULongLong = new OpType("u8", ULongLongORDINAL);

        private static final int DoubleORDINAL = 6;
        public static final OpType Double= new OpType("d", DoubleORDINAL);

        private static final int VoidPointerORDINAL = 7;
        public static final OpType VoidPointer = new OpType("vb", VoidPointerORDINAL);

        private static final int VarCharPointerORDINAL = 8;
        public static final OpType VarCharPointer = new OpType("vc", VarCharPointerORDINAL);

        public static final EnumeratedValues enumeration =
                new EnumeratedValues(new OpType[] {
                    Boolean, Int, UInt, Real, LongLong, ULongLong, Double, VoidPointer, VarCharPointer
                });
    }



    /* Represents an instruction and its operands */
    class Instruction {
        private String m_opCode;
        private Operand[] m_operands;


        public Instruction(String opCode, Operand[] operands) {
            m_opCode = opCode;
            m_operands = operands;
        }

        final void print(PrintWriter writer) {
            writer.print(m_opCode);
            if (null!=m_operands) {
                writer.print(' ');
                printOperands(writer, m_operands);
            }
            writer.print(m_separator);
        }

        final public String getOpCode() {
            return m_opCode;
        }

        final public Operand[] getOperands()
        {
            return m_operands;
        }
    }



    /**
     *  An inner class describing the different registers
     */
    public static class RegisterSetType extends EnumeratedValues.BasicValue {
        private char m_prefix;
        private RegisterSetType(String name, int ordinal, char prefix) {
            super(name, ordinal, null);
            m_prefix=prefix;
        }

         final public char getPrefix() {
            return m_prefix;
        }

        public static final int OutputORDINAL = 0;
        public static final RegisterSetType Output = new RegisterSetType("output", OutputORDINAL,'O');

        public static final int InputORDINAL = 1;
        public static final RegisterSetType Input = new RegisterSetType("input", InputORDINAL,'I');

        public static final int LiteralORDINAL = 2;
        public static final RegisterSetType Literal = new RegisterSetType("literal", LiteralORDINAL,'L');

        public static final int LocalORDINAL = 3;
        public static final RegisterSetType Local = new RegisterSetType("local", LocalORDINAL,'T');

        public static final int StatusORDINAL = 4;
        public static final RegisterSetType Status = new RegisterSetType("status", StatusORDINAL,'S');

        public static final EnumeratedValues enumeration =
                new EnumeratedValues(new RegisterSetType[] {Output, Input, Literal, Local, Status});
    }

    /**
     * A place holder to hold defined register sets
     */
    protected class RegisterSets
    {
        //Members variables -----------------
        private ArrayList[] m_sets =
                new ArrayList[RegisterSetType.enumeration.getSize()];


        //Methods ---------------------------
        final public ArrayList getSet(int set)
        {
            return m_sets[set];
        }

        /**
         * Creates a register in a register set
         * @param opType what type the value in the register should have
         * @param initValue
         * @param registerType specifies in which register set the register should live
         * @return the newly created Register
         */
        public Register newRegister(OpType opType, Object initValue, RegisterSetType registerType)
        {
            compilationAssert(opType!=null,"null is an invalid OpType");
            compilationAssert(registerType!=null,"null is an invalid RegisterSetType");

            int set =  registerType.getOrdinal();
            if (null == m_sets[set]) {
                m_sets[set] = new ArrayList();
            }

            Register newReg = new Register(opType, initValue, registerType, m_sets[set].size());
            m_sets[set].add(newReg);
            return newReg;
        }


    }

    // Methods -----------------------------------------------------------------

    /**
     * Sets the separator between instructions in the generated program.
     * Can be either {@link #SEPARATOR_SEMICOLON ';'} or {@link #SEPARATOR_NEWLINE '\n' or ";\n"}
     */
    public void setSeparator(String separator)
    {
        if (!separator.equals(SEPARATOR_NEWLINE) &&
            !separator.equals(SEPARATOR_SEMICOLON) &&
            !separator.equals(SEPARATOR_SEMICOLON_NEWLINE))
        {
            throw SaffronResource.instance().newProgramCompilationError("Separator must be ';'[\\n] or '\\n'");
        }
        this.m_separator = separator;
    }

    protected void compilationAssert(boolean cond, String msg) {
        if (!cond) {
            throw SaffronResource.instance().newCompilationAssertionError(msg);
        }
    }

    protected void compilationAssert(boolean cond) {
        compilationAssert(cond,null);
    }

    /**
     * A program consists of two parts, its instructions and the user defined registers with its value.
     * The string representation of the program will be in the following format
     * <Register Set Definitions> See {@link #getRegisterSetsLayout}<br>
     * <"\n"><br>
     * <Instruction set> See {@link #getInstructions}<br>
     * <br>
     * @return
     */
    public String getProgram()
    {
        bindReferences();
        validate();
        optimize();
        validate(); //validate again to check if the optimization broke something
        StringWriter sw = new StringWriter();
        PrintWriter writer = new PrintWriter(sw);
        getRegisterSetsLayout(writer);
        writer.print("T");
        writer.print(m_separator);
        getInstructions(writer);
        return sw.toString().trim();
    }

    /**
     * Replaces a label with a line number
     */
    private void bindReferences() {
        for (int i = 0; i < m_instructions.size(); i++) {
            //Look for instructions that have Line as operands
            Instruction instruction = (Instruction) m_instructions.get(i);
            Operand[] operands = instruction.getOperands();
            for (int j = 0; (null!=operands) && (j < operands.length); j++) {
                Operand operand = operands[j];
                if (operand instanceof Line) {
                    Line line = (Line) operand;
                    if (line.getLabel()!=null) //we have a label, update the line number with whats in the m_labels map
                    {
                        compilationAssert(null==line.getLine(),"Line has already been bind.");
                        java.lang.Integer lineNbrFromLabel = (java.lang.Integer) m_labels.get(line.getLabel());
                        if (null == lineNbrFromLabel)
                        {
                            throw SaffronResource.instance().newProgramCompilationError(
                                    "Label '"+line.getLabel()+"' not defined");
                        }
                        line.setLine(lineNbrFromLabel.intValue());
                    }
                }
            }
        }
    }

    /**
     * Returns the number of existing lines of instructions - 1
     * @return
     */
    public int getCurrentLineNumber()
    {
        return m_instructions.size()-1;
    }
    /**
     * Output of the register set will be in the following <i>order</i> and <i>format</i>:
     * <code>
     * output:  TBD
     * input:
     * local:
     * literal:
     * status:
     * </code>
     * @param writer
     */
    protected void getRegisterSetsLayout(PrintWriter writer)
    {
        ArrayList list;
        Register reg;
        //iterate over register sets
        for(int i=0;i<RegisterSetType.enumeration.getSize();i++)
        {
            list=m_registerSets.getSet(i);
            if (null!=list) {
                writer.print(RegisterSetType.enumeration.getName(i));
                writer.print(": ");
                //iterate over every register in the current set
                for (int j=0;j<list.size();j++) {
                    reg = (Register) list.get(j);
                    writer.print(reg.getOpType().getName());
                    writer.print('[');
                    writer.print(((Register) list.get(j)).getIndex());
                    writer.print(']');
                    if (reg.getValue()!=null) {
                        compilationAssert (reg.getRegisterType().getOrdinal()==
                                RegisterSetType.LiteralORDINAL,"Only literals can have values");
                        writer.print('=');
                        reg.printValue(writer);
                    }

                    if (j+1<list.size()) {
                        writer.print(',');
                    }
                    else {
                        writer.print(m_separator);
                    }
                }
            }
        }
    }

    /**
     * See {@link #getProgram}
     * @param writer
     */
    protected void getInstructions(PrintWriter writer)
    {
        Iterator it = m_instructions.iterator();
        while(it.hasNext()) {
            ((Instruction) it.next()).print(writer);
        }
    }

    /**
     * Tries to optimize the program
     */
    private void optimize()
    {
        //todo 1 look and remove unused registers

        //todo 2 maybe dangersous but an idea anyway, implement an algorithm that reduces and reuses local registers

        //todo 3 look for instruction like "literal operation literal" and precalcuate if possible.
        //todo    Probably wouldnt happen too often. Who would query "not true"=false
    }

    /**
     * Validates the program. Some errors are:<ul>
     * <li>use of same register with different types</li>
     * <li>jumping to a line that does not exist</li>
     * </ul>
     */
    private void validate()
    {
        for(int i=0;i<m_instructions.size();i++) {
            Instruction inst = (Instruction) m_instructions.get(i);
            String op = inst.getOpCode();
            //this try-catch clause will pick up any compiler excpetions messages and wrap it into a msg containg
            //what line went wrong
            try
            {
                //-----------Check if any jump instructions are jumping off the cliff
                if (op.startsWith(jumpOperator)) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue()>=m_instructions.size()) {
                        throw SaffronResource.instance().newProgramCompilationError(
                                "Line "+line.getLine()+" doesn't exist");
                    }
                }

                //-----------Check if any jump instructions jumps to itself
                if (op.startsWith(jumpOperator)) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue()==i) {
                        throw SaffronResource.instance().newProgramCompilationError(
                                "Can not jump to the same line as the instruction");
                    }
                }

                //-----------Forbidding loops. Check if any jump instructions jumps to a previous line.
                if (op.startsWith(jumpOperator)) {
                    Line line = (Line) inst.getOperands()[0];
                    if (line.getLine().intValue()<i) {
                        throw SaffronResource.instance().newProgramCompilationError(
                                "Loops are forbidden. Can not jump to a previous line");
                    }
                }

                //TODO add to see that all declared registers are beeing referenced
                //TODO add to see if output registers where declared (?)
                //TODO add to see if all output registers where written to
                //TODO add to see if a non constant register is used before assigned to

                //----------- Other post validation goes here
                //...
            }
            catch(Throwable e) {
                StringWriter log = new StringWriter();
                inst.print(new PrintWriter(log));
                throw SaffronResource.instance().newProgramCompilationError(log.toString()
                                                                      +NL
                                                                      +e.getMessage(),e);
            }
        }

    }

    private void printOperands(PrintWriter writer, Operand[] operands)
    {
        if (null!=operands) {
            for(int i=0;i<operands.length;i++) {
                operands[i].print(writer);
                if (i+1<operands.length) {
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
    public Register newOutput(OpType type) {
        return m_registerSets.newRegister(type, null, RegisterSetType.Output);
    }

    /**
     * Genererates a reference to a constant register.
     * If the the constant value already has been defined,
     * the existing reference for that value will silently be returned instead of creating a new one
     * @param type {@link OpType} Type of value
     * @param value Value
     * @return
     */
    public Register newLiteral(OpType type, Object value)
    {
        Register ret;
        if (m_literals.containsKey(value))
        {
            ret = (Register) m_literals.get(value);
        }
        else
        {
            ret = m_registerSets.newRegister(type,value,RegisterSetType.Literal);
            m_literals.put(value,ret);
        }
        return ret;
    }

    public Register newBoolLiteral(boolean b)
    {
        return newLiteral(OpType.Boolean, new java.lang.Boolean(b));
    }

    public Register newLongLiteral(int i)
    {
        return newLiteral(OpType.Int, new java.lang.Integer(i));
    }

    public Register newLongLongLiteral(int i)
    {
        return newLiteral(OpType.LongLong, new java.lang.Integer(i));
    }

    public Register newULongLiteral(int i)
    {
        compilationAssert(i>=0,"Unsigned value was found to be negative. Value="+i);
        return newLiteral(OpType.UInt, new java.lang.Integer(i));
    }

    public Register newULongLongLiteral(int i)
    {
        compilationAssert(i>=0,"Unsigned value was found to be negative. Value="+i);
        return newLiteral(OpType.ULongLong, new java.lang.Integer(i));
    }

    public Register newFloatLiteral(float f)
    {
        return newLiteral(OpType.Real, new java.lang.Float(f));
    }

    public Register newLongDoubleLiteral(double d)
    {
        return newLiteral(OpType.Double, new java.lang.Double(d));
    }

    public Register newVoidPointerLiteral(byte[] bytes)
    {
        return newLiteral(OpType.VoidPointer, bytes);
    }

    /**
     * Generates a reference to a string literal. The actual value will be
     * a pointer to an array of bytes.
     */
    public Register newStringLiteral(String s)
    {
        return newLiteral(OpType.VarCharPointer, s);
    }

    /**
     * Creates a register in the input set and returns its reference
     */
    public Register newInput(OpType type)
    {
        return m_registerSets.newRegister(type,null,RegisterSetType.Input);
    }

    /**
     * Creates a register in the local set and returns its reference
     */
    public Register newLocal(OpType type)
    {
        return m_registerSets.newRegister(type,null,RegisterSetType.Local);
    }

    /**
     * Creates a register in the status set and returns its reference
     */
    public Register newStatus(OpType type)
    {
        return m_registerSets.newRegister(type,null,RegisterSetType.Status);
    }

    //---------------------------------------
    //Instruction Creation


    protected void addInstruction(String operator, Operand op1)
    {
        addInstruction(operator, new Operand[] { op1});
    }

    protected void addInstruction(String operator, Operand op1, Operand op2)
    {
        addInstruction(operator, new Operand[] { op1,op2});
    }

    protected void addInstruction(String operator, Operand op1, Operand op2, Operand op3)
    {
        addInstruction(operator, new Operand[] { op1,op2,op3});
    }

    protected void addInstruction(String operator, Operand[] operands)
    {
        assertOperandsNotNull(operands);
        m_instructions.add(new Instruction(operator, operands));
    }

    protected void addInstruction(String operator)
    {
        m_instructions.add(new Instruction(operator, null));
    }

    // assert helper functions---

    /**
     * Asserts that the register is not declared as {@link RegisterSetType#Literal} or {@link RegisterSetType#Input}
     * @param result
     */
    protected void assertRegisterNotConstant(Register result)
    {
        compilationAssert( (result.getRegisterType().getOrdinal()!=RegisterSetType.LiteralORDINAL) &&
                (result.getRegisterType().getOrdinal()!=RegisterSetType.InputORDINAL),
                 "Expected a non constant register. Constant registers are Literals and Inputs");
    }

    /**
     * Asserts that the register is declared as {@link OpType#Boolean}
     * @param reg
     */
    protected void assertRegisterBool(Register reg)
    {
        compilationAssert( reg.getOpType().getOrdinal()==
                OpType.BooleanORDINAL,"Expected a register of Boolean type");
    }

    /**
     * Asserts that the register is declared as a pointer.
     * Pointers are {@link OpType#VarCharPointer},{@link OpType#VoidPointer}
     * @param reg
     */
    protected void assertRegisterIsPointer(Register reg)
    {
        compilationAssert( reg.getOpType().getOrdinal()==OpType.VoidPointerORDINAL ||
                reg.getOpType().getOrdinal()==OpType.VarCharPointerORDINAL,"Expected a register of Pointer type");
    }

    /**
     * Asserts that the register is declared as integer.
     * Integers are {@link OpType#Int}, {@link OpType#LongLong},
     * {@link OpType#UInt},{@link OpType#ULongLong}
     * @param reg
     */
    protected void assertRegisterInteger(Register reg)
    {
        compilationAssert( reg.getOpType().getOrdinal()==OpType.IntORDINAL ||
                reg.getOpType().getOrdinal()==OpType.LongLongORDINAL ||
                reg.getOpType().getOrdinal()==OpType.UIntORDINAL||
                reg.getOpType().getOrdinal()==OpType.ULongLongORDINAL,"Expected a register of Integer type");
    }

    /**
     * Asserts that Register is nothing of the following, all at once<ul>
     * <li>of Literal type</li>
     * <li>of Integer type</li>
     * <li>its value is zero</li>
     * </ul>
     * @param reg
     */
    protected void assertNotDivideByZero(Register reg)
    {
        if (    reg.getRegisterType().getOrdinal()==RegisterSetType.LiteralORDINAL &&
                (reg.getValue() != null) &&
                (reg.getValue() instanceof java.lang.Integer))
        {
            compilationAssert (!reg.getValue().equals(new Integer(0)),
                    "A literal register of Integer type and value=0 was found");
        }
    }

    /**
     * Asserts input is of native type except booleans
     * @param register
     */
    protected void assertIsNativeType(Register register) {
        compilationAssert( register.getOpType().getOrdinal()==OpType.IntORDINAL||
                register.getOpType().getOrdinal()==OpType.LongLongORDINAL ||
                register.getOpType().getOrdinal()==OpType.UIntORDINAL||
                register.getOpType().getOrdinal()==OpType.ULongLongORDINAL||
                register.getOpType().getOrdinal()==OpType.RealORDINAL||
                register.getOpType().getOrdinal()==OpType.DoubleORDINAL,"Register is not of native OpType");
    }

    /**
     * Asserts that all operands are not null
     * @param operands
     */
    protected void assertOperandsNotNull(Operand[] operands)
    {
       compilationAssert(operands!=null,"Operands can not be null");

       for(int i=0;i<operands.length;i++)
       {
           compilationAssert(null!=operands[i],"Operand can not be null");;
       }
    }

    /**
     * Creates a call to a external function/instruction
     * @param functionName
     * @param registers
     */
    public void addExtendedInstructionCall(Register result, String functionName, Register[] registers) {
        compilationAssert(result!=null,"Result can not be null");
        assertOperandsNotNull(registers);
        addInstruction(extendedOperator, new FunctionCall(result, functionName, registers));
    }

    /**
     * Adds a move instruction.
     */
    public void addMove(Register target, Register src)
    {
        compilationAssert(target.getOpType()==src.getOpType(),
                "Type Mismatch. Tried to MOVE "+src.getOpType().getName()+" into a "+target.getOpType().getName());
        addInstruction(moveOperator, target, src );
    }

    // Jump related instructions----------------------
    /**
     * Adds an uncondtional JMP instruction
     * @param line
     */
    public void addJump(int line)
    {
        compilationAssert(line>=0,"Line can not be negative. Value="+line);
        addInstruction(jumpOperator, new Operand[]{new Line(line)});
    }

    protected void addJumpBooleanWithCondition(String op, int line, Register reg)
    {
        compilationAssert(line>=0,"Line can not be negative. Value="+line);
        addJumpBooleanWithCondition(op, new Line(line), reg);
    }

    protected void addJumpBooleanWithCondition(String op, Line line, Register reg)
    {
        assertRegisterBool(reg);
        assertRegisterNotConstant(reg);
        //reg != null is checked for in addInstuction
        addInstruction(op, line, reg);
    }

    /**
     * Adds an condtional JMP instruction. Jumps to {@param line} if the value in {@param reg} is TRUE
     * @pre reg of Boolean Type
     * @pre line>=0
     * @post line < number of calls to addXxxInstruction methods
     */
    public void addJumpTrue(int line, Register reg)
    {
        addJumpBooleanWithCondition(jumpTrueOperator,line, reg);
    }

    public void addJumpFalse(int line, Register reg)
    {
        addJumpBooleanWithCondition(jumpFalseOperator,line, reg);
    }

    public void addJumpNull(int line, Register reg)
    {
        addJumpBooleanWithCondition(jumpNullOperator, line, reg);
    }

    public void addJumpNotNull(int line, Register reg)
    {
        addJumpBooleanWithCondition(jumpNotNullOperator, line, reg);
    }

    // Bool related instructions----------------------

    /**
     * @pre result is not constant
     * @pre result/op1/op2 are of type Boolean
     */
    protected void addBoolInstruction(String op, Register result, Register op1, Register op2)
    {
        assertRegisterBool(result);
        assertRegisterNotConstant(result);
        assertRegisterBool(op1);

        if (null!=op2)
        {
            assertRegisterBool(op2);
            addInstruction(op, result, op1, op2);
        }
        else
        {
            addInstruction(op, result, op1);
        }
    }

    public void addBoolAnd(Register result, Register op1, Register op2)
    {
        addBoolInstruction(andOperator, result, op1, op2);
    }

    public void addBoolOr(Register result, Register op1, Register op2)
    {
        addBoolInstruction(orOperator, result, op1, op2);
    }

    public void addBoolEqual(Register result, Register op1, Register op2)
    {
        addBoolInstruction(equalOperator, result, op1, op2);
    }

    public void addBoolNotEqual(Register result, Register op1, Register op2)
    {
        addBoolInstruction(notEqualOperator, result, op1, op2);
    }

    public void addBoolGreaterThan(Register result, Register op1, Register op2)
    {
        addBoolInstruction(greaterThanOperator, result, op1, op2);
    }

    public void addBoolLessThan(Register result, Register op1, Register op2)
    {
        addBoolInstruction(lessThenOperator, result, op1, op2);
    }

    public void addBoolNot(Register result, Register op1)
    {
        addBoolInstruction(notOperator, result, op1, null);
    }

    public void addBoolMove(Register result, Register op1)
    {
        addBoolInstruction(moveOperator, result, op1, null);
    }

    public void addBoolIsNull(Register result, Register op1)
    {
        addBoolInstruction(isNullOperator, result, op1, null);
    }

    public void addBoolIsNotNull(Register result, Register op1)
    {
        addBoolInstruction(isNotNullOperator, result, op1, null);
    }


    // Native instructions----------------------

    /**
     * @pre result is not constant
     * @pre result is not of pointer type
     * @param op
     * @param result
     * @param op1
     * @param op2
     */
    protected void addNativeInstruction(String op, Register result, Register op1, Register op2)
    {
        assertRegisterNotConstant(result);
        assertIsNativeType(result);  //todo need precision checking
        assertIsNativeType(op1);


        if (null!=op2)
        {
            assertIsNativeType(op2); //todo need precision checking
            addInstruction(op, result, op1, op2);
        }
        else
        {
            addInstruction(op, result, op1);
        }
    }

    public void addNativeAdd(Register result, Register op1, Register op2)
    {
        addNativeInstruction(addOperator,result,op1,op2);
    }

    public void addNativeSub(Register result, Register op1, Register op2)
    {
        addNativeInstruction(subOperator,result,op1,op2);
    }

    public void addNativeMul(Register result, Register op1, Register op2)
    {
        addNativeInstruction(mulOperator,result,op1,op2);
    }

    public void addNativeDiv(Register result, Register op1, Register op2)
    {
        //smart check, check if divide by zero if op2 is a constant
        assertNotDivideByZero(op2);
        addNativeInstruction(divOperator,result,op1,op2);
    }

    public void addNativeNeg(Register result, Register op1)
    {
        addNativeInstruction(negOperator,result,op1, null);
    }

    public void addNativeMove(Register result, Register op1)
    {
        addNativeInstruction(moveOperator,result,op1,null);
    }

    //Integral Native Instructions ---------------------------------
    /**
     * @pre result is not constant
     * @pre op2 is of type Integer
     * @pre op2 is not negative if it is a constant (cant shift negative steps)
     * @param op
     * @param result
     * @param op1
     * @param op2
     */
    protected void addIntergalNativeShift(String op,Register result, Register op1, Register op2)
    {
        assertRegisterNotConstant(result);
        compilationAssert(op.equals(shflOperator) || op.equals(shfrOperator),"This function only accepts shft & shfr");

        //second operand can only be either long or ulong
        assertRegisterInteger(op2);

        //smart check, if a constant value that could be negative IS negative, complain
        if (    op2.getRegisterType().getOrdinal()==RegisterSetType.LiteralORDINAL &&
                (   op2.getOpType().getOrdinal()==OpType.IntORDINAL ||
                    op2.getOpType().getOrdinal()==OpType.LongLongORDINAL
                ) &&
                (op2.getValue()!=null) &&
                (op2.getValue() instanceof java.lang.Integer))
        {
            compilationAssert (((java.lang.Integer) op2.getValue()).intValue()>=0,
                    "Cannot shift negative amout of steps. Value="+((java.lang.Integer) op2.getValue()).intValue());
        }
        addInstruction(op, result, op1, op2);
    }

    protected void addIntergalNativeInstruction(String op,Register result, Register op1, Register op2)
    {
        assertRegisterNotConstant(result);
        addInstruction(op,result,op1,op2);
    }

    public void addIntegralNativeMod(Register result, Register op1, Register op2)
    {
        //check if divide by zero if op2 is a constant
        assertNotDivideByZero(op2);
        addIntergalNativeInstruction(modOperator,result,op1,op2);
    }

    public void addIntegralNativeShiftLeft(Register result, Register op1, Register op2)
    {
        addIntergalNativeShift(shflOperator,result,op1,op2);
    }

    public void addIntegralNativeShiftRight(Register result, Register op1, Register op2)
    {
        addIntergalNativeShift(shfrOperator,result,op1,op2);
    }

    public void addIntegralNativeAnd(Register result, Register op1, Register op2)
    {
        addIntergalNativeInstruction(andOperator,result,op1,op2);
    }

    public void addIntegralNativeOr(Register result, Register op1, Register op2)
    {
        addIntergalNativeInstruction(orOperator,result,op1,op2);
    }

    public void addIntegralNativeXor(Register result, Register op1, Register op2)
    {
        addIntergalNativeInstruction(xorOperator,result,op1,op2);
    }

    //Bool Native Instructions --------------------------------

    /**
     * @pre result is not constant
     * @pre result is of type Boolean
     * @param op
     * @param result
     * @param op1
     * @param op2
     */
    protected void addIBoolNativeInstruction(String op,Register result, Register op1, Register op2)
    {
        assertRegisterNotConstant(result);
        //result must always be boolean
        assertRegisterBool(result);

        if (null!=op2)
        {
            addInstruction(op, result, op1, op2);
        }
        else
        {
            addInstruction(op, result, op1);
        }
    }

    public void addBoolNativeEqual(Register result, Register op1, Register op2)
    {
        addIBoolNativeInstruction(equalOperator,result,op1,op2);
    }

    public void addBoolNativeNotEqual(Register result, Register op1, Register op2)
    {
        addIBoolNativeInstruction(notEqualOperator,result,op1,op2);
    }

    public void addBoolNativeGreaterThan(Register result, Register op1, Register op2)
    {
        addIBoolNativeInstruction(greaterThanOperator,result,op1,op2);
    }

    public void addBoolNativeGreaterOrEqual(Register result, Register op1, Register op2)
    {
        addIBoolNativeInstruction(greaterOrEqualOperator,result,op1,op2);
    }

    public void addBoolNativeLessThan(Register result, Register op1, Register op2)
    {
        addIBoolNativeInstruction(lessThenOperator,result,op1,op2);
    }

    public void addBoolNativeLessOrEqual(Register result, Register op1, Register op2)
    {
        addIBoolNativeInstruction(lessOrEqualOperator,result,op1,op2);
    }

    public void addBoolNativeIsNull(Register result, Register op1)
    {
        addIBoolNativeInstruction(isNullOperator,result,op1,null);
    }

    public void addBoolNativeIsNotNull(Register result, Register op1)
    {
        addIBoolNativeInstruction(isNotNullOperator,result,op1,null);
    }

    //Pointer Instructions --------------------------------

    /**
     * @pre result is not constant
     * @pre result is of type Boolean
     * @pre op1/op2 are of pointer type
     * @param op
     * @param result
     * @param op1
     * @param op2
     */
    protected void addPointerBoolInstruction(String op,Register result, Register op1, Register op2)
    {
        assertRegisterNotConstant(result);
        assertRegisterBool(result);
        assertRegisterIsPointer(op1);

        if (null!=op2)
        {
            assertRegisterIsPointer(op2);
            addInstruction(op, result, op1, op2);
        }
        else
        {
            addInstruction(op, result, op1);
        }
    }

    public void addPointerMove(Register result, Register op1)
    {
        assertRegisterNotConstant(result);
        assertRegisterIsPointer(result);
        assertRegisterIsPointer(op1);
        addInstruction(moveOperator,result,op1);
    }

    public void addPointerAdd(Register result, Register op1, Register op2)
    {
        assertRegisterNotConstant(result);
        assertRegisterIsPointer(result);
        assertRegisterIsPointer(op1);
        assertRegisterInteger(op2);
        addInstruction(addOperator,result,op1,op2);
    }

    public void addPointerEqual(Register result, Register op1, Register op2)
    {
        addPointerBoolInstruction(equalOperator,result,op1,op2);
    }

    public void addPointerNotEqual(Register result, Register op1, Register op2)
    {
        addPointerBoolInstruction(notEqualOperator,result,op1,op2);
    }

    public void addPointerGreaterThan(Register result, Register op1, Register op2)
    {
        addPointerBoolInstruction(greaterThanOperator,result,op1,op2);
    }

    public void addPointerGreaterOrEqual(Register result, Register op1, Register op2)
    {
        addPointerBoolInstruction(greaterOrEqualOperator,result,op1,op2);
    }

    public void addPointerLessTham(Register result, Register op1, Register op2)
    {
        addPointerBoolInstruction(lessThenOperator,result,op1,op2);
    }

    public void addPointerLessOrEqual(Register result, Register op1, Register op2)
    {
        addPointerBoolInstruction(lessOrEqualOperator,result,op1,op2);
    }

    public void addPointerIsNull(Register result, Register op1)
    {
        addPointerBoolInstruction(isNullOperator,result,op1,null);
    }

    public void addPointerIsNotNull(Register result, Register op1)
    {
        addPointerBoolInstruction(isNotNullOperator,result,op1,null);
    }

    public void addReturn() {
        addInstruction(returnOperation);
    }

    public void addLabelJump(String label) {
        addInstruction(jumpOperator, new Operand[]{new Line(label)});
    }

    public void addLabelJumpTrue(String label, Register reg) {
        addJumpBooleanWithCondition(jumpTrueOperator, new Line(label), reg);
    }

    public void addLabelJumpFalse(String label, Register reg) {
        addJumpBooleanWithCondition(jumpFalseOperator, new Line(label), reg);
    }

    public void addLabelJumpNull(String label, Register reg) {
        addJumpBooleanWithCondition(jumpNullOperator, new Line(label), reg);
    }

    public void addLabelJumpNotNull(String label, Register reg) {
        addJumpBooleanWithCondition(jumpNotNullOperator, new Line(label), reg);
    }

    public void addLabel(String label) {
        compilationAssert(!m_labels.containsKey(label),"Label '"+label+"' already defined");
        int line = m_instructions.size();
        m_labels.put(label, new java.lang.Integer(line));
    }
}

// End CalcProgramBuilder.java