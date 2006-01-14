/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import net.sf.farrago.type.runtime.*;

import openjava.ptree.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

/**
 * An {@link OJRexImplementor} for reinterpret casts. Reinterpret 
 * casts are important for generated Java code, because while 
 * Sql types may share the same primitive types, they may require 
 * different wrappers. Currently this class can only handle 
 * conversions between Decimals and Bigints.
 *
 * @author John Pham
 * @version $Id$
 */
public class FarragoOJRexReinterpretImplementor
	extends FarragoOJRexCastImplementor
{
    /** Constructs an OJRexReinterpretCastImplementor */
    public FarragoOJRexReinterpretImplementor()
    {
    }

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        return call.isA(RexKind.Reinterpret);
    }
    
    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        Util.pre(call.isA(RexKind.Reinterpret),
            "call.isA(RexKind.Reinterpret)");

        RelDataType retType = call.getType();
        if (SqlTypeUtil.isDecimal(retType)) {
            // assignment from long to decimal
            Variable varResult = translator.createScratchVariable(retType);
            translator.addStatement(
                new ExpressionStatement(
                    new MethodCall(
                        varResult,
                        EncodedSqlDecimal.REINTERPRET_METHOD_NAME, 
                        new ExpressionList(operands[0], operands[1]))));
            checkNullability(
                translator, retType, call.operands[0], operands[0]);
            return varResult;
        }
        
        Util.pre(SqlTypeUtil.isIntType(retType),
            "SqlTypeUtil.isIntType(retType)");
        Util.pre(SqlTypeUtil.isDecimal(call.operands[0].getType()),
            "SqlTypeUtil.isDecimal(call.operands[0].getType())");
        if (retType.isNullable()) {
            // assignment from decimal to nullable long
            Variable varResult = translator.createScratchVariable(retType);
            translator.addStatement(
                new ExpressionStatement(
                    new MethodCall(
                        operands[0],
                        EncodedSqlDecimal.ASSIGN_TO_METHOD_NAME, 
                        new ExpressionList(varResult))));
            return varResult;
        }
        return super.implementFarrago(translator, call, operands);
    }

    /**
     * Inserts a null test if a value is nullable, but a target type 
     * is not.
     */
    private void checkNullability(
        FarragoRexToOJTranslator translator,
        RelDataType targetType,
        RexNode rexValue,
        Expression javaValue)
    {
        ExpressionStatement nullTest = null;
        if (!targetType.isNullable() && rexValue.getType().isNullable()) {
            nullTest = 
                new ExpressionStatement(
                    new MethodCall(
                        translator.getRelImplementor()
                        .getConnectionVariable(),
                        "checkNotNull",
                        new ExpressionList(
                            Literal.makeLiteral(rexValue.toString()),
                            javaValue)));
            translator.addStatement(nullTest);
        }
    }
}

// End OJRexReinterpretCastImplementor.java
