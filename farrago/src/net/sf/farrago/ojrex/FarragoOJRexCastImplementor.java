/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.ojrex;

import net.sf.farrago.resource.*;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoOJRexCastImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for CAST expressions. 
 * 
 * <p>A cast is described in terms of an assignment: lhs = rhs, with 
 * "lhs" meaning the variable on the left hand side and "rhs" meaning 
 * the value right hand side. The general arguments are as follows:
 * 
 * <p><ul>
 *   <li>The left hand side always has a target type
 *   <li>The left hand side optionally specifies a target variable. If 
 *     one is not specified, a new variable will usually be created.
 *   <li>The right hand side usually has a source type, but may not 
 *     in the case of a UDX. If so, it can only be used for AssignableValues.
 *   <li>The right hand side usually has a value, but may be omitted for 
 *     dynamic parameters, such as those used for a UDX.
 *  </ul>
 * 
 * <p>The two main kinds of assignment are to "primitive" types and to 
 * "AssignableValue" types. If the target type is "primitive", it is 
 * represented by a primitive Java value in generated code, or a thin 
 * wrapper around one. Non-primitive types are expected to fulfill the 
 * {@link AssignableValue} interface and are assigned using that interface.
 * 
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexCastImplementor
    extends FarragoOJRexImplementor
{

    //~ Static fields/initializers ---------------------------------------------

    private static StatementList throwOverflowStmtList =
        new StatementList(
            new ThrowStatement(
                new MethodCall(
                    new Literal(
                        Literal.STRING,
                        "net.sf.farrago.resource.FarragoResource.instance().Overflow"),
                    "ex",
                    new ExpressionList())));

    /**
     * Gets a list of statements that throw an overflow exception
     */
    private static StatementList getThrowStmtList()
    {
        return throwOverflowStmtList;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        RelDataType lhsType = call.getType();
        RelDataType rhsType = call.operands[0].getType();
        Expression rhsExp = operands[0];

        if (lhsType.getSqlTypeName() == SqlTypeName.Cursor) {
            // Conversion should already have been taken care of outside.
            return rhsExp;
        }

        // Normally the validator will report the error.
        // but when do insert into t values (...)
        // somehow, it slipped in.
        // TODO: should it be done by the validator even
        // for insert into table?
        if ((lhsType != null) && (rhsType != null)) {
            // in the case of set catalog 'sys_cwm'
            // select "name" from Relational"."Schema";
            // somehow java String datatype slipped in.
            // we need to filter it out.
            if ((lhsType.getSqlTypeName() != null)
                && (rhsType.getSqlTypeName() != null)) {
                if (!SqlTypeUtil.canCastFrom(lhsType, rhsType, true)) {
                    // REVIEW jvs 27-Dec-2005:  Need a better error
                    // message here:  this is during code generation, but
                    // the message is intended for execution.
                    throw FarragoResource.instance().Overflow.ex();
                }
            }
        }

        CastHelper helper = 
            new CastHelper(
                translator,
                null,
                call.toString(),
                lhsType,
                rhsType,
                null,
                rhsExp);
        
        return helper.implement();
    }

    /**
     * Generates code to cast an OJ expression as another type. See class
     * description for an explanation of arguments.
     * 
     * @return resulting expression. If lhsExp was provided, assigns 
     * this expression to lhsExp.
     */
    public Expression convertCastOrAssignment(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        String targetName,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        CastHelper helper =
            new CastHelper(
                translator, 
                stmtList, 
                targetName, 
                lhsType, 
                rhsType, 
                lhsExp,
                rhsExp);

        return helper.implement();
    }

    /**
     * Generates code to cast an OJ expression as another type.
     * The target type is limited to being an AssignableValue. See class
     * description for an explanation of the arguments.
     * 
     * @return resulting expression. If lhsExp was provided, assigns 
     * this expression to lhsExp.
     */
    public Expression convertCastToAssignableValue(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        CastHelper helper =
            new CastHelper(
                translator, 
                stmtList, 
                null, 
                lhsType, 
                rhsType, 
                lhsExp,
                rhsExp);

        return helper.castToAssignableValue();
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Helps to implement the CAST operator for a specific cast node.
     */
    private class CastHelper {
        private FarragoRexToOJTranslator translator;
        private StatementList stmtList;
        private String targetName;
        private RelDataType lhsType;
        private RelDataType rhsType;
        private Expression lhsExp;
        private Expression rhsExp;
        private OJClass lhsClass;
        
        /**
         * Constructs a new CastHelper
         * 
         * @param translator translator for implementing Rex as Java code
         * @param stmtList statement list in which to insert statements
         * @param targetName the name of the target column or expression
         * @param lhsType the left hand side type
         * @param rhsType the right hand side type
         * @param lhsExp the left hand side expression
         * @param rhsExp the right hand side expression
         */
        public CastHelper(
            FarragoRexToOJTranslator translator,
            StatementList stmtList,
            String targetName,
            RelDataType lhsType,
            RelDataType rhsType,
            Expression lhsExp,
            Expression rhsExp)
        {
            assert(lhsType != null);
            
            this.translator = translator;
            this.stmtList = stmtList;
            this.targetName = targetName;
            this.lhsType = lhsType;
            this.rhsType = rhsType;
            this.lhsExp = lhsExp;
            this.rhsExp = rhsExp;
        }
        
        /**
         * Implement the cast expression.
         * 
         * <p>TODO: check for overflow
         * 
         * @return the rhs expression casted as the lhs type
         */
        public Expression implement()
        {
            // Check for invalid null assignment. Code generated afterwards 
            // can assume null will never be assigned to a not null value.
            checkNotNull();

            // Check for an explicit rhs null value. Code generated 
            // afterwards need never check for an explicit null.
            if (rhsType.getSqlTypeName() == SqlTypeName.Null) {
                if (lhsType.isNullable()) {
                    return castFromNull();
                } else {
                    // NOTE jvs 27-Jan-2005:  this code will never actually
                    // be executed do to previous checkNotNull test, but
                    // it still has to compile!
                    return rhsAsValue();
                }
            }

            // Case when left hand side is a nullable primitive
            if (translator.isNullablePrimitive(lhsType)) {
                if (SqlTypeUtil.isJavaPrimitive(rhsType)
                    && (!rhsType.isNullable()
                        || translator.isNullablePrimitive(rhsType))) {
                    return castPrimitiveToNullablePrimitive();
                }
                return castToAssignableValue();
            }
            
            // Case when left hand side is a not nullable primitive
            if (SqlTypeUtil.isJavaPrimitive(lhsType)) {
                return castToNotNullPrimitive();
            }
            
            // Case when left hand side is a structure
            if (lhsType.isStruct()) {
                assert (rhsType.isStruct());

                // TODO jvs 27-May-2004:  relax this assert and deal with
                // conversions, null checks, etc.
                assert (lhsType.equals(rhsType));

                return getDirectAssignment();
            }
            
            // Default is to treat non-primitives as AssignableValue
            return castToAssignableValue();
        }

        /**
         * Generates code to throw an exception when a NULL value is casted 
         * to a NOT NULL type
         */
        private void checkNotNull()
        {
            if (!lhsType.isNullable() && rhsType.isNullable()) {
                rhsExp = rhsAsJava();
                addStatement(
                    new ExpressionStatement(
                        new MethodCall(
                            translator.getRelImplementor().getConnectionVariable(),
                            "checkNotNull",
                            new ExpressionList(
                                Literal.makeLiteral(targetName),
                                rhsExp))));
            }
        }

        /**
         * Gets the right hand expression as a simple Java 
         * value. If the rhs is a more complex expression, then creates a 
         * scratch variable and assigns the right hand expression to it. 
         * Then returns the scratch variable.
         */
        private Expression rhsAsJava()
        {
            if (!RelDataTypeFactoryImpl.isJavaType(rhsType)) {
                Variable variable = translator.createScratchVariable(rhsType);
                addStatement(
                    new ExpressionStatement(
                        new AssignmentExpression(
                            variable,
                            AssignmentExpression.EQUALS,
                            rhsExp)));
                return variable;
            }
            return rhsExp;
        }
        
        /**
         * Gets the right hand expression as a valid value to be 
         * assigned to the left hand side. Usually returns the original rhs. 
         * However, if the lhs is of a primitive type, and the rhs is an 
         * explicit null, returns a primitive value instead.
         */
        private Expression rhsAsValue()
        {
            if (SqlTypeUtil.isJavaPrimitive(lhsType)
                && rhsType.getSqlTypeName() == SqlTypeName.Null) 
            {
                if (lhsType.getSqlTypeName() == SqlTypeName.Boolean) {
                    return Literal.constantFalse();
                } else {
                    return Literal.constantZero();
                }
            }
            return rhsExp;
        }
        
        /**
         * Implements a cast from NULL. Creates a scratch variable if 
         * one was not provided and assigns NULL to it.
         */
        private Expression castFromNull()
        {
            ensureLhs();
            addStatement(
                translator.createSetNullStatement(lhsExp, true));
            return lhsExp;
        }

        /**
         * Implements a cast from any Java primitive to a nullable Java 
         * primitive as a simple assignment. i.e.
         * 
         * <pre>
         * [NullablePrimitiveType] lhs;
         * lhs.[nullIndicator] = ...;
         * lhs.[value] = ...;
         * </pre>
         */
        private Expression castPrimitiveToNullablePrimitive()
        {
            ensureLhs();
            Expression rhsIsNull;
            if (rhsType.isNullable()) {
                rhsIsNull = getNullIndicator(rhsExp);
                rhsExp = getValue(rhsExp);
            } else {
                rhsIsNull = Literal.constantFalse();
            }
            
            checkOverflow();
            addStatement(
                assign(
                    getNullIndicator(lhsExp),
                    rhsIsNull));
            roundAsNeeded();
            addStatement(
                assign(
                    getValue(lhsExp),
                    new CastExpression(
                        getLhsClass(),
                        rhsExp)));
            return lhsExp;
        }

        /**
         * Casts the rhs to an {@link AssignableValue} using that interface's 
         * standard assignment method. i.e.
         * 
         * <pre>
         * [AssignableValueType] lhs;
         * lhs.[assignMethod](rhs);
         * </pre>
         * 
         * or perhaps a type-specific cast:
         * 
         * <pre>
         * [AssignableValueType] lhs;
         * lhs.[castMethod](rhs, lhs.getPrecision());
         * </pre>
         * 
         * <p>Code is also generated to pad and truncate values which need 
         * special handling, such as date and time types.
         */
        private Expression castToAssignableValue()
        {
            ensureLhs();
            if ((rhsType != null)
                && (
                    SqlTypeUtil.isNumeric(rhsType)
                    || (rhsType.getSqlTypeName() == SqlTypeName.Boolean)
                   )
                && SqlTypeUtil.inCharOrBinaryFamilies(lhsType)
                && !SqlTypeUtil.isLob(lhsType)) {
                // Boolean or Numeric to String.
                // sometimes the Integer got slipped by.
                if (rhsType.isNullable()
                    && (!SqlTypeUtil.isDecimal(rhsType))) {
                    rhsExp = getValue(rhsExp);
                }
                addStatement(
                    new ExpressionStatement(
                        new MethodCall(
                            lhsExp,
                            "cast",
                            new ExpressionList(
                                rhsExp,
                                Literal.makeLiteral(
                                    lhsType.getPrecision())))));
            } else {
                addStatement(
                    new ExpressionStatement(
                        new MethodCall(
                            lhsExp,
                            AssignableValue.ASSIGNMENT_METHOD_NAME,
                            new ExpressionList(rhsExp))));
            }

            boolean mayNeedPadOrTruncate = false;
            if (SqlTypeUtil.inCharOrBinaryFamilies(lhsType)
                && !SqlTypeUtil.isLob(lhsType)) {
                mayNeedPadOrTruncate = true;
            }
            if (mayNeedPadOrTruncate) {
                // check overflow if it is datetime.
                // TODO: should check it at the run time.
                // so, it should be in the
                // cast(SqlDateTimeWithTZ, int precision);
                if ((rhsType != null) && (rhsType.getSqlTypeName() != null)) {
                    SqlTypeName typeName = rhsType.getSqlTypeName();
                    int precision = 0;
                    int ord = typeName.getOrdinal();
                    if (ord == SqlTypeName.Date_ordinal) {
                        precision = 10;
                    } else if (ord == SqlTypeName.Time_ordinal) {
                        precision = 8;
                    } else if (ord == SqlTypeName.Timestamp_ordinal) {
                        precision = 19;
                    }
                    if ((precision != 0) && (precision > lhsType.getPrecision())) {
                        addStatement(
                            new IfStatement(
                                new BinaryExpression(
                                    Literal.makeLiteral(precision),
                                    BinaryExpression.GREATER,
                                    Literal.makeLiteral(lhsType.getPrecision())),
                                getThrowStmtList()));
                    }
                }
                if ((rhsType != null)
                    && (rhsType.getFamily() == lhsType.getFamily())
                    && !SqlTypeUtil.isLob(rhsType)) {
                    // we may be able to skip pad/truncate based on
                    // known facts about source and target precisions
                    if (SqlTypeUtil.isBoundedVariableWidth(lhsType)) {
                        if (lhsType.getPrecision() >= rhsType.getPrecision()) {
                            // target precision is greater than source
                            // precision, so truncation is impossible
                            // and we can skip adjustment
                            return lhsExp;
                        }
                    } else {
                        if ((lhsType.getPrecision() == rhsType.getPrecision())
                            && !SqlTypeUtil.isBoundedVariableWidth(rhsType)) {
                            // source and target are both fixed-width, and
                            // precisions are the same, so there's no adjustment
                            // needed
                            return lhsExp;
                        }
                    }
                }

                // determine target precision
                Expression precisionExp =
                    Literal.makeLiteral(lhsType.getPrecision());

                // need to pad only for fixed width
                Expression needPadExp =
                    Literal.makeLiteral(
                        !SqlTypeUtil.isBoundedVariableWidth(lhsType));

                // pad character is 0 for binary, space for character
                Expression padByteExp;
                if (!SqlTypeUtil.inCharFamily(lhsType)) {
                    padByteExp =
                        new CastExpression(
                            OJSystem.BYTE,
                            Literal.makeLiteral(0));
                } else {
                    padByteExp =
                        new CastExpression(
                            OJSystem.BYTE,
                            Literal.makeLiteral(' '));
                }

                // generate the call to do the job
                addStatement(
                    new ExpressionStatement(
                        new MethodCall(
                            lhsExp,
                            BytePointer.ENFORCE_PRECISION_METHOD_NAME,
                            new ExpressionList(precisionExp,
                                needPadExp,
                                padByteExp))));
            }
            return lhsExp;
        }

        /**
         * Casts the rhs to a non nullable primitive value. Non nullable 
         * primitive values only have a single value field.
         */
        private Expression castToNotNullPrimitive()
        {
            // If the left and the right types are the same, perform a 
            // trivial cast.
            if (lhsType == rhsType) {
                return getDirectAssignment();
            }
            
            // Retrieve the value of the right side if it is a nullable 
            // primitive or a Datetime or an Interval type.
            // TODO: is Decimal a nullable primitive?
            if (translator.isNullablePrimitive(rhsType) ||
                SqlTypeUtil.isDatetime(rhsType) ||
                SqlTypeUtil.isInterval(rhsType)) 
            {
                rhsExp = getValue(rhsExp);
            }

            // Get the name of the numeric class such as Byte, Short, etc.
            String numClassName = SqlTypeUtil.getNumericJavaClassName(lhsType);
            OJClass lhsClass = getLhsClass();

            // When casting from a string (or binary) to a number, trim the 
            // value and perform the cast by calling a class-specific parsing 
            // function.
            if ((numClassName != null)
                && SqlTypeUtil.inCharOrBinaryFamilies(rhsType)
                && !SqlTypeUtil.isLob(rhsType)) {
                //TODO: toString will cause too much garbage collection.
                rhsExp =
                    new MethodCall(
                        rhsExp,
                        "toString",
                        new ExpressionList());
                rhsExp = new MethodCall(
                        rhsExp,
                        "trim",
                        new ExpressionList());
                String methodName = "parse" + numClassName;
                if (lhsType.getSqlTypeName().getOrdinal()
                    == SqlTypeName.Integer_ordinal) {
                    methodName = "parseInt";
                }
                rhsExp =
                    new MethodCall(
                        new Literal(
                            Literal.STRING,
                            numClassName),
                        methodName,
                        new ExpressionList(rhsExp));

                Variable outTemp = translator.getRelImplementor().newVariable();
                translator.addStatement(
                    new VariableDeclaration(
                        TypeName.forOJClass(lhsClass),
                        new VariableDeclarator(
                            outTemp.toString(),
                            rhsExp)));
                rhsExp = outTemp;

                // Note: this check for overflow should only be required 
                // when the string conversion does not perform a check.
                checkOverflow();
            }
            
            // Casting from string to boolean relies on the runtime type.
            // Note: string is trimmed by conversion method.
            else if ((lhsType.getSqlTypeName() == SqlTypeName.Boolean)
                && SqlTypeUtil.inCharOrBinaryFamilies(rhsType)
                && !SqlTypeUtil.isLob(rhsType)) {
                //TODO: toString will cause too much garbage collection.
                Expression str =
                    new MethodCall(
                        rhsExp,
                        "toString",
                        new ExpressionList());

                rhsExp =
                    new MethodCall(
                        OJClass.forClass(
                            NullablePrimitive.NullableBoolean.class),
                        "convertString",
                        new ExpressionList(str));
            } 
            
            // In general, check for overflow
            else {
                checkOverflow();
            }
            
            roundAsNeeded();
            
            rhsExp = new CastExpression(lhsClass, rhsExp);
            return getDirectAssignment();
        }

        /**
         * Directly assigns the right hand side to to an lhs variable and 
         * returns the lhs variable. If no variable was provided, returns 
         * the original rhs.
         */
        private Expression getDirectAssignment()
        {
            if (lhsExp == null) {
                return rhsExp;
            }
            
            addStatement(assign(lhsExp, rhsExp));
            return lhsExp;
        }

        /**
         * Checks for overflow when assigning one primitive type to another. 
         * Non-primitive types check for overflow during assignment.
         */
        private void checkOverflow()
        {
            String maxLiteral = null;
            String minLiteral = null;
            if (lhsType == null) {
                return;
            }
            // Assume that equivalent types can be assigned without overflow
            if (lhsType.getSqlTypeName() == rhsType.getSqlTypeName()) {
                return;
            }
            // Approximate numerics have a wider range than exact numerics
            if (SqlTypeUtil.isApproximateNumeric(lhsType) 
                && SqlTypeUtil.isExactNumeric(rhsType))
            {
                return;
            }
            // We can skip an error check if the left type is "larger"
            if (SqlTypeUtil.isIntType(lhsType)
                && SqlTypeUtil.isIntType(rhsType)
                && (SqlTypeUtil.maxValue(lhsType) 
                    >= SqlTypeUtil.maxValue(rhsType))) 
            {
                return;
            }
            if (SqlTypeUtil.isExactNumeric(lhsType)) {
                String numClassName = SqlTypeUtil.getNumericJavaClassName(lhsType);
                minLiteral = numClassName + ".MIN_VALUE";
                maxLiteral = numClassName + ".MAX_VALUE";
            } else if (SqlTypeUtil.isApproximateNumeric(lhsType)) {
                String numClassName = SqlTypeUtil.getNumericJavaClassName(lhsType);
                maxLiteral = numClassName + ".MAX_VALUE";
                minLiteral = "-" + maxLiteral;
            }
            if (maxLiteral == null) {
                return;
            }
            Statement ifstmt =
                new IfStatement(
                    new BinaryExpression(
                        new BinaryExpression(
                            rhsExp,
                            BinaryExpression.LESS,
                            new Literal(
                                Literal.STRING,
                                minLiteral)),
                        BinaryExpression.LOGICAL_OR,
                        new BinaryExpression(
                            rhsExp,
                            BinaryExpression.GREATER,
                            new Literal(
                                Literal.STRING,
                                maxLiteral))),
                    getThrowStmtList());
            addStatement(ifstmt);
        }

        /**
         * Creates a left hand side variable if one was not provided.
         */
        private void ensureLhs()
        {
            if (lhsExp == null) {
                lhsExp = translator.createScratchVariable(lhsType);
            }
        }

        /**
         * Gets the OJ class for the left hand side (target) type
         */
        private OJClass getLhsClass()
        {
            if (lhsClass == null) {
                lhsClass = getClass(lhsType);
            }

            return lhsClass;
        }

        /**
         * Gets the OJ class for a RelDataType
         */
        private OJClass getClass(RelDataType type)
        {
            // TODO: is this code any better?
            // OJUtil.typeToOJClass(
            //     rhsType,
            //     translator.getFarragoTypeFactory());

            FarragoTypeFactory factory = translator.getFarragoTypeFactory();
            return OJClass.forClass(
                factory.getClassForPrimitive(type));
        }

        /**
         * Adds the statement to the statement list if it is not null. 
         * Otherwise, adds the statement to the translator list.
         * 
         * @param stmt the statement to be added
         */
        private void addStatement(Statement stmt) 
        {
            if (stmtList == null) {
                translator.addStatement(stmt);
            } else {
                stmtList.add(stmt);
            }
        }
        
        /**
         * Creates a simple assignment statement as in a = b.
         */
        private ExpressionStatement assign(Expression a, Expression b)
        {
            return 
                new ExpressionStatement(
                    new AssignmentExpression(
                        a,
                        AssignmentExpression.EQUALS,
                        b));
        }
        
        /**
         * Creates a field access, as in expr.[nullIndicator]
         */
        private FieldAccess getNullIndicator(Expression expr)
        {
            return new FieldAccess(
                expr, 
                NullablePrimitive.NULL_IND_FIELD_NAME);
        }
        
        /**
         * Creates a field access, as in expr.[value]
         */
        private FieldAccess getValue(Expression expr) {
            return 
                new FieldAccess(
                    expr, 
                    NullablePrimitive.VALUE_FIELD_NAME);
        }
        
        /**
         * Rounds right hand side, if required. Rounding is required when 
         * casting from an approximate numeric to an exact numeric.
         */
        private void roundAsNeeded()
        {
            if (SqlTypeUtil.isExactNumeric(lhsType)
                && SqlTypeUtil.isApproximateNumeric(rhsType)) 
            {
                rhsExp = roundAway();
            }
        }
        
        /**
         * Generates code to round an expression according to the Farrago 
         * convention. The Farrago convention is to round away from zero.
         * Rounding is performed with the following algorithm.
         * 
         * <pre>
         * in = rhs;
         * if (value < 0) {
         *     in = -in;
         *     out = Math.round(in);
         *     out = -out;
         * } else {
         *     out = Math.round(in);
         * }
         * </pre>
         * 
         * <p>PRECONDITION: rhsExp must be an unwrapped (not null) Java 
         * primitive
         * 
         * <p>TODO: account for overflow in both unary minus and round.
         */
        private Expression roundAway()
        {
            // Get the primitive part of right hand side
            RelDataType inType = 
                translator.getTypeFactory()
                    .createTypeWithNullability(rhsType, false);

            // TODO: is there any preference between stack and instance var?
            OJClass inClass = getClass(inType);
            Variable inTemp = translator.getRelImplementor().newVariable();
            translator.addStatement(
                declareStackVar(inClass, inTemp, rhsExp));

            OJClass outClass = getLhsClass();
            Variable outTemp = translator.getRelImplementor().newVariable();
            translator.addStatement(
                declareStackVar(outClass, outTemp, null));

            addStatement(
                new IfStatement(
                    new BinaryExpression(
                        inTemp,
                        BinaryExpression.LESS,
                        Literal.constantZero()),
                    new StatementList(
                        assign(inTemp, minus(inClass, inTemp)),
                        assign(outTemp, round(lhsClass, inTemp)),
                        assign(outTemp, minus(outClass, outTemp))),
                    new StatementList(
                        assign(outTemp, round(lhsClass, inTemp)))));
            return outTemp;
        }
        
        /**
         * Creates a unary minus, as in -expr. The type of the expression 
         * returned is the same type as the the original expression. This 
         * is required for situations such as byte:
         * 
         * <pre>
         * byte b;
         * b = -b; // cannot cast int to byte
         * </pre>
         * 
         * The unary minus operator returns an integer value. Note that 
         * unary minus potentially causes an overflow, because in most cases,
         * |MIN_VALUE| > |MAX_VALUE| (ex. |-128| > |127| for byte)
         */
        private Expression minus(OJClass clazz, Expression expr)
        {
            return 
                new CastExpression(
                    clazz,
                    new UnaryExpression(
                        expr,
                        UnaryExpression.MINUS));
        }

        /**
         * Generates code to round an expression up and cast it. Rounding 
         * may cause can overflow.
         * 
         * @param clazz type to cast rounded expression as
         * @param expr expression to be rounded
         */
        private Expression round(OJClass clazz, Expression expr) 
        {
            return
                new CastExpression(
                    clazz,
                    new MethodCall(
                        new Literal(
                            Literal.STRING,
                            "java.lang.Math"),
                    "round",
                    new ExpressionList(expr)));
        }
        
        /**
         * Makes a statement to declare a stack variable
         * 
         * @param clazz OJ class of the variable to declare
         * @param var the variable to be declared
         * @param init initial value for the declaration.
         */
        private VariableDeclaration declareStackVar(
            OJClass clazz, Variable var, Expression init)
        {
            return new VariableDeclaration(
                TypeName.forOJClass(clazz),
                new VariableDeclarator(
                    var.toString(),
                    init));
        }
    }
}

// End FarragoOJRexCastImplementor.java
