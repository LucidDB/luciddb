/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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
package net.sf.farrago.ojrex;

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
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

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        RelDataType lhsType = call.getType();
        RelDataType rhsType = call.operands[0].getType();
        Expression rhsExp = operands[0];
        return convertCastOrAssignment(translator, lhsType, rhsType, null,
            rhsExp);
    }

    private Expression convertCastNull(
        FarragoRexToOJTranslator translator,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            lhsExp = translator.createScratchVariable(lhsType);
        }
        translator.addStatement(
            translator.createSetNullStatement(lhsExp, true));
        return lhsExp;
    }

    private Expression convertCastPrimitiveToNullablePrimitive(
        FarragoRexToOJTranslator translator,
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
        translator.addStatement(
            new ExpressionStatement(
                new AssignmentExpression(
                    new FieldAccess(lhsExp,
                        NullablePrimitive.NULL_IND_FIELD_NAME),
                    AssignmentExpression.EQUALS,
                    rhsIsNull)));
        FarragoTypeFactory factory = translator.getFarragoTypeFactory();
        translator.addStatement(
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
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (translator.isNullablePrimitive(rhsType)) {
            rhsExp =
                new FieldAccess(rhsExp, NullablePrimitive.VALUE_FIELD_NAME);
        }
        OJClass lhsClass = OJUtil.typeToOJClass(
            lhsType, translator.getFarragoTypeFactory());
        rhsExp = new CastExpression(lhsClass, rhsExp);
        return convertDirectAssignment(translator, lhsExp, rhsExp);
    }

    private Expression convertDirectAssignment(
        FarragoRexToOJTranslator translator,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            return rhsExp;
        } else {
            translator.addStatement(
                new ExpressionStatement(
                    new AssignmentExpression(lhsExp,
                        AssignmentExpression.EQUALS, rhsExp)));
            return lhsExp;
        }
    }

    Expression convertCastToAssignableValue(
        FarragoRexToOJTranslator translator,
        RelDataType lhsType,
        RelDataType rhsType,
        Expression lhsExp,
        Expression rhsExp)
    {
        if (lhsExp == null) {
            lhsExp = translator.createScratchVariable(lhsType);
        }
        translator.addStatement(
            new ExpressionStatement(
                new MethodCall(
                    lhsExp,
                    AssignableValue.ASSIGNMENT_METHOD_NAME,
                    new ExpressionList(rhsExp))));

        boolean mayNeedPadOrTruncate = false;
        if (SqlTypeUtil.inCharOrBinaryFamilies(lhsType)
            && !SqlTypeUtil.isLob(lhsType))
        {
            mayNeedPadOrTruncate = true;
        }
        if (mayNeedPadOrTruncate) {
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
            translator.addStatement(
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
                translator.addStatement(
                    new ExpressionStatement(
                        new AssignmentExpression(variable,
                            AssignmentExpression.EQUALS, rhsExp)));
                rhsExp = variable;
            }
            
            // TODO:  provide exception context
            translator.addStatement(
                new ExpressionStatement(
                    new MethodCall(
                        translator.getRelImplementor()
                        .getConnectionVariable(),
                        "checkNotNull",
                        new ExpressionList(rhsExp))));
        }

        // special case for source explicit null
        OJClass rhsClass = OJUtil.typeToOJClass(
            rhsType,
            translator.getFarragoTypeFactory());
        if (rhsType.getSqlTypeName() == SqlTypeName.Null) {
            if (lhsType.isNullable()) {
                return convertCastNull(translator, lhsType, rhsType, lhsExp,
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
                    lhsType, rhsType, lhsExp, rhsExp);
            } else {
                return convertCastToAssignableValue(translator, lhsType,
                    rhsType, lhsExp, rhsExp);
            }
        } else if (SqlTypeUtil.isJavaPrimitive(lhsType)) {
            return convertCastToNotNullPrimitive(translator, lhsType, rhsType,
                lhsExp, rhsExp);
        } else if (lhsType.isStruct()) {
            assert (rhsType.isStruct());

            // TODO jvs 27-May-2004:  relax this assert and deal with
            // conversions, null checks, etc.
            assert (lhsType.equals(rhsType));

            return convertDirectAssignment(translator, lhsExp, rhsExp);
        } else {
            return convertCastToAssignableValue(translator, lhsType, rhsType,
                lhsExp, rhsExp);
        }
    }

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        RelDataType lhsType = call.getType();
        RelDataType rhsType = call.operands[0].getType();
        
        RelDataTypeFamily lhsTypeFamily = lhsType.getFamily();
        RelDataTypeFamily rhsTypeFamily = rhsType.getFamily();

        // casting between numeric and non-numeric types is
        // not yet implemented
        if ((lhsTypeFamily == SqlTypeFamily.Numeric)
            && (rhsTypeFamily != SqlTypeFamily.Numeric)) {
            return rhsType.getSqlTypeName() == SqlTypeName.Null;
        }
        if ((rhsTypeFamily == SqlTypeFamily.Numeric)
            && (lhsTypeFamily != SqlTypeFamily.Numeric)) {
            return false;
        }

        // TODO jvs 11-Aug-2004:  think through other cases
        return true;
    }
}


// End FarragoOJRexCastImplementor.java
