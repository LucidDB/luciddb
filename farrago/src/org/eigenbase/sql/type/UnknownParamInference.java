/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;

/**
 * Strategy to infer unknown types of the operands of an operator call.
 *
 * @author Wael Chatila
 * @since Sept 8, 2004
 * @version $Id$
 **/
public interface UnknownParamInference
{
    //~ INTERFACE -------------------------------------------

    /**
     * Infers any unknown operand types.
     *
     * @param validator the validator context
     * @param scope the the validator scope context
     * @param call the call being analyzed
     * @param returnType the type known or inferred for the
     * result of the call
     * @param operandTypes receives the inferred types for all operands
     */
    void inferOperandTypes(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call,
        RelDataType returnType,
        RelDataType [] operandTypes);


    //~ IMPLEMENTATIONS -------------------------------------------

    /**
     * Parameter type-inference strategy where an unknown operand
     * type is derived from the first operand with a known type.
     */
    UnknownParamInference useFirstKnown =
        new UnknownParamInference() {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                SqlNode [] operands = call.getOperands();
                final RelDataType unknownType = validator.getUnknownType();
                RelDataType knownType = unknownType;
                for (int i = 0; i < operands.length; ++i) {
                    knownType = validator.deriveType(scope, operands[i]);
                    if (!knownType.equals(unknownType)) {
                        break;
                    }
                }
                assert !knownType.equals(unknownType);
                for (int i = 0; i < operandTypes.length; ++i) {
                    operandTypes[i] = knownType;
                }
            }
        };
    
    /**
     * Parameter type-inference strategy where an unknown operand
     * type is derived from the call's return type.  If the return type is
     * a record, it must have the same number of fields as the number
     * of operands.
     */
    UnknownParamInference useReturnType =
        new UnknownParamInference() {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                for (int i = 0; i < operandTypes.length; ++i) {
                    if (returnType.isStruct()) {
                        operandTypes[i] = returnType.getFields()[i].getType();
                    } else {
                        operandTypes[i] = returnType;
                    }
                }
            }
        };
    
    /**
     * Parameter type-inference strategy where an unknown operand
     * type is assumed to be boolean.
     */
    UnknownParamInference useBoolean =
        new UnknownParamInference() {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                RelDataTypeFactory typeFactory = validator.getTypeFactory();
                for (int i = 0; i < operandTypes.length; ++i) {
                    operandTypes[i] =
                        typeFactory.createSqlType(SqlTypeName.Boolean);
                }
            }
        };
}

// End UnknownParamInference.java
