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

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.resource.FarragoResource;

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
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoOJRexCastImplementor extends FarragoOJRexImplementor
{
    //~ Methods ---------------------------------------------------------------
    private static StatementList throwOverflowStmtList =
        new StatementList(
            new ThrowStatement(
                new MethodCall(
                    new Literal(
                        Literal.STRING,
                        "net.sf.farrago.resource.FarragoResource.instance().Overflow"),
                    "ex",
                    new ExpressionList())));

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        RelDataType lhsType = call.getType();
        RelDataType rhsType = call.operands[0].getType();
        Expression rhsExp = operands[0];
        // Normally the validator will report the error.
        // but when do insert into t values (...)
        // somehow, it slipped in.
        // TODO: should it be done by the validator even
        // for insert into table?
        if (lhsType != null && rhsType != null) {
            // in the case of set catalog 'sys_cwm'
            // select "name" from Relational"."Schema";
            // somehow java String datatype slipped in.
            // we need to filter it out.
            if (lhsType.getSqlTypeName() != null && 
                rhsType.getSqlTypeName() != null) 
            {
                if (!SqlTypeUtil.canCastFrom(lhsType, rhsType, true)) {
                    // REVIEW jvs 27-Dec-2005:  Need a better error
                    // message here:  this is during code generation, but
                    // the message is intended for execution.
                    throw FarragoResource.instance().Overflow.ex();
                }
            }
        }
        return convertCastOrAssignment(
            translator,
            null,
            call.toString(),
            lhsType, rhsType, null,
            rhsExp);
    }

    private void addStatement(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        Statement stmt)
    {
        if (stmtList == null) {
            translator.addStatement(stmt);
        } else {
            stmtList.add(stmt);
        }
    }

    private Expression convertCastNull(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            lhsExp = translator.createScratchVariable(lhsType);
        }
        addStatement(translator, stmtList, 
            translator.createSetNullStatement(lhsExp, true));
        return lhsExp;
    }

    private Expression convertCastPrimitiveToNullablePrimitive(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            lhsExp = translator.createScratchVariable(lhsType);
        }
        Expression rhsIsNull;
        if (rhsType.isNullable()) {
            rhsIsNull =
                new FieldAccess(rhsExp, NullablePrimitive.NULL_IND_FIELD_NAME);
            rhsExp =
                new FieldAccess(rhsExp, NullablePrimitive.VALUE_FIELD_NAME);
        } else {
            rhsIsNull = Literal.constantFalse();
        }
        addStatement(translator, stmtList, 
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(lhsExp,
                        NullablePrimitive.NULL_IND_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    rhsIsNull)));
        FarragoTypeFactory factory = translator.getFarragoTypeFactory();
        addStatement(translator, stmtList, 
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(lhsExp, NullablePrimitive.VALUE_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    new CastExpression(
                        OJClass.forClass(
                            factory.getClassForPrimitive(lhsType)),
                        rhsExp))));
        return lhsExp;
    }

    Expression convertCastToNotNullPrimitive(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (translator.isNullablePrimitive(rhsType)) {
            rhsExp =
                new FieldAccess(rhsExp, NullablePrimitive.VALUE_FIELD_NAME);
        }
        if (lhsType != rhsType) {
            String numClassName = SqlTypeUtil.getNumericJavaClassName(lhsType);
            OJClass lhsClass = OJUtil.typeToOJClass(
                lhsType, translator.getFarragoTypeFactory());
            if (numClassName != null
                &&  SqlTypeUtil.inCharOrBinaryFamilies(rhsType)
                && !SqlTypeUtil.isLob(rhsType))
            {
                //TODO: toString will cause too much garbage collection.
                rhsExp = new MethodCall(
                            rhsExp,
                            "toString",
                            new ExpressionList());
                rhsExp = new MethodCall(
                            rhsExp,
                            "trim",
                            new ExpressionList());
                String methodName = "parse" + numClassName;
                if (lhsType.getSqlTypeName().getOrdinal() ==
                       SqlTypeName.Integer_ordinal)
                {
                    methodName = "parseInt";
                }
                rhsExp = new MethodCall(
                            new Literal(
                                Literal.STRING,
                                numClassName),
                            methodName,
                            new ExpressionList(rhsExp));

                Variable outTemp = translator.getRelImplementor().newVariable();
                translator.addStatement(
                         new VariableDeclaration(TypeName.forOJClass(lhsClass),
                         new VariableDeclarator(outTemp.toString(), rhsExp)));
                rhsExp = outTemp;
                
                checkOverflow(translator, stmtList, lhsType, rhsExp);
            }  else if ((lhsType.getSqlTypeName() == SqlTypeName.Boolean)
                    &&  SqlTypeUtil.inCharOrBinaryFamilies(rhsType)
                    && !SqlTypeUtil.isLob(rhsType)) {

                //TODO: toString will cause too much garbage collection.
                Expression str = new MethodCall(
                            rhsExp,
                            "toString",
                            new ExpressionList());

                rhsExp = new MethodCall(
                            OJClass.forClass(
                                    NullablePrimitive.NullableBoolean.class),
                            "convertString",
                            new ExpressionList(str));
            }  else {
                checkOverflow(translator, stmtList, lhsType, rhsExp);
            }
            if (SqlTypeUtil.isExactNumeric(lhsType)
                && SqlTypeUtil.isApproximateNumeric(rhsType)) {
                OJClass inClass = OJUtil.typeToOJClass(
                     rhsType, translator.getFarragoTypeFactory());

                Variable inTemp = translator.getRelImplementor().newVariable();
                translator.addStatement(
                         new VariableDeclaration(TypeName.forOJClass(inClass),
                         new VariableDeclarator( inTemp.toString(), null)));

                OJClass outClass = OJUtil.typeToOJClass(
                     lhsType, translator.getFarragoTypeFactory());
                Variable outTemp = translator.getRelImplementor().newVariable();
                translator.addStatement(
                         new VariableDeclaration(TypeName.forOJClass(outClass),
                         new VariableDeclarator(outTemp.toString(), null)));

                addStatement(translator, stmtList,
                    new ExpressionStatement(
                        new AssignmentExpression(
                            inTemp,
                            AssignmentExpression.EQUALS,
                            rhsExp)));

                addStatement(translator, stmtList,
                    new IfStatement(
                        new BinaryExpression(
                            inTemp,
                            BinaryExpression.LESS,
                            Literal.constantZero()),
                        new StatementList(
                            new ExpressionStatement(
                                new AssignmentExpression(inTemp,
                                    AssignmentExpression.EQUALS,
                                    new UnaryExpression(
                                        inTemp,
                                        UnaryExpression.MINUS))),
                            new ExpressionStatement(
                                new AssignmentExpression(
                                    outTemp,
                                    AssignmentExpression.EQUALS,
                                    new CastExpression(lhsClass,
                                        new MethodCall(
                                            new Literal(
                                                Literal.STRING,
                                                "java.lang.Math"),
                                            "round",
                                            new ExpressionList(inTemp))))),
                            new ExpressionStatement(
                                new AssignmentExpression(outTemp,
                                    AssignmentExpression.EQUALS,
                                    new UnaryExpression(
                                        outTemp,
                                        UnaryExpression.MINUS)))),
                        new StatementList(
                            new ExpressionStatement(
                                new AssignmentExpression(
                                    outTemp,
                                    AssignmentExpression.EQUALS,
                                    new CastExpression(lhsClass,
                                        new MethodCall(
                                            new Literal(
                                                Literal.STRING,
                                                "java.lang.Math"),
                                            "round",
                                            new ExpressionList(inTemp))))))));
                rhsExp = outTemp;
            }
            rhsExp = new CastExpression(lhsClass, rhsExp);
        }
        return convertDirectAssignment(translator, stmtList, lhsExp, rhsExp);
    }

    private void checkOverflow(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        RelDataType lhsType,
        Expression rhsExp)
    {
        String maxLiteral = null;
        String minLiteral = null;
        if (lhsType == null) {
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
        addStatement(translator, stmtList, ifstmt);
    }

    private StatementList getThrowStmtList()
    {
        return throwOverflowStmtList;
    }

    private Expression convertDirectAssignment(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            return rhsExp;
        } else {
            addStatement(translator, stmtList, 
                new ExpressionStatement(
                    new AssignmentExpression(lhsExp,
                        AssignmentExpression.EQUALS, rhsExp)));
            return lhsExp;
        }
    }

    Expression convertCastToAssignableValue(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            lhsExp = translator.createScratchVariable(lhsType);
        }
        if (rhsType != null 
            && (SqlTypeUtil.isNumeric(rhsType) ||
                rhsType.getSqlTypeName() == SqlTypeName.Boolean)
            && SqlTypeUtil.inCharOrBinaryFamilies(lhsType)
            && !SqlTypeUtil.isLob(lhsType))
        {
            // Boolean or Numeric to String.
            // sometimes the Integer got slipped by.
            if (rhsType.isNullable()
                && (! SqlTypeUtil.isDecimal(rhsType))) {
                rhsExp = new FieldAccess(
                                rhsExp, 
                                NullablePrimitive.VALUE_FIELD_NAME);
            }
            addStatement(translator, stmtList, 
                new ExpressionStatement(
                    new MethodCall(
                        lhsExp,
                        "cast",
                        new ExpressionList(
                            rhsExp,
                            Literal.makeLiteral(
                                lhsType.getPrecision())))));
            
        } else {
            addStatement(translator, stmtList, 
                new ExpressionStatement(
                    new MethodCall(
                        lhsExp,
                        AssignableValue.ASSIGNMENT_METHOD_NAME,
                        new ExpressionList(rhsExp))));
        }

        boolean mayNeedPadOrTruncate = false;
        if (SqlTypeUtil.inCharOrBinaryFamilies(lhsType)
            && !SqlTypeUtil.isLob(lhsType))
        {
            mayNeedPadOrTruncate = true;
        }
        if (mayNeedPadOrTruncate) {
            // check overflow if it is datetime.
            // TODO: should check it at the run time.
            // so, it should be in the 
            // cast(SqlDateTimeWithTZ, int precision);
            if (rhsType != null && rhsType.getSqlTypeName() != null) {
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
                if (precision != 0 && precision > lhsType.getPrecision()) {
                    addStatement(translator, stmtList,
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
                && !SqlTypeUtil.isLob(rhsType))
            {
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
                            && !SqlTypeUtil.isBoundedVariableWidth(rhsType))
                    {
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
            addStatement(translator, stmtList, 
                new ExpressionStatement(
                    new MethodCall(
                        lhsExp,
                        BytePointer.ENFORCE_PRECISION_METHOD_NAME,
                        new ExpressionList(precisionExp, needPadExp, padByteExp))));
        }
        return lhsExp;
    }


    Expression convertCastOrAssignment(
        FarragoRexToOJTranslator translator,
        StatementList stmtList,
        String targetName,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        // TODO:  overflow etc.
        if (!lhsType.isNullable() && rhsType.isNullable()) {
            // generate code which will throw an exception whenever an attempt
            // is made to cast a null value to a NOT NULL type
            if (!RelDataTypeFactoryImpl.isJavaType(rhsType)) {
                Variable variable = translator.createScratchVariable(rhsType);
                addStatement(translator, stmtList, 
                    new ExpressionStatement(
                        new AssignmentExpression(variable,
                            AssignmentExpression.EQUALS, rhsExp)));
                rhsExp = variable;
            }
            
            addStatement(translator, stmtList, 
                new ExpressionStatement(
                    new MethodCall(
                        translator.getRelImplementor()
                        .getConnectionVariable(),
                        "checkNotNull",
                        new ExpressionList(
                            Literal.makeLiteral(targetName),
                            rhsExp))));
        }

        // special case for source explicit null
        OJClass rhsClass = OJUtil.typeToOJClass(
            rhsType,
            translator.getFarragoTypeFactory());
        if (rhsType.getSqlTypeName() == SqlTypeName.Null) {
            if (lhsType.isNullable()) {
                return convertCastNull(translator, stmtList, lhsType, rhsType, lhsExp,
                    rhsExp);
            } else {
                // NOTE jvs 27-Jan-2005:  this code will never actually
                // be executed do to previous checkNotNull test, but
                // it still has to compile!
                if (SqlTypeUtil.isJavaPrimitive(lhsType)) {
                    if (lhsType.getSqlTypeName() == SqlTypeName.Boolean) {
                        rhsExp = Literal.constantFalse();
                    } else {
                        rhsExp = Literal.constantZero();
                    }
                }
                return rhsExp;
            }
        }

        if (translator.isNullablePrimitive(lhsType)) {
            if (SqlTypeUtil.isJavaPrimitive(rhsType)
                && (!rhsType.isNullable()
                    || translator.isNullablePrimitive(rhsType)))
            {
                return convertCastPrimitiveToNullablePrimitive(translator, 
                    stmtList, lhsType, rhsType, lhsExp, rhsExp);
            } else {
                return convertCastToAssignableValue(translator, 
                    stmtList, lhsType, rhsType, lhsExp, rhsExp);
            }
        } else if (SqlTypeUtil.isJavaPrimitive(lhsType)) {
            return convertCastToNotNullPrimitive(translator, 
                    stmtList, lhsType, rhsType, lhsExp, rhsExp);
        } else if (lhsType.isStruct()) {
            assert (rhsType.isStruct());

            // TODO jvs 27-May-2004:  relax this assert and deal with
            // conversions, null checks, etc.
            assert (lhsType.equals(rhsType));

            return convertDirectAssignment(translator, stmtList, lhsExp, rhsExp);
        } else {
            return convertCastToAssignableValue(translator, 
                    stmtList, lhsType, rhsType, lhsExp, rhsExp);
        }
    }
}


// End FarragoOJRexCastImplementor.java
