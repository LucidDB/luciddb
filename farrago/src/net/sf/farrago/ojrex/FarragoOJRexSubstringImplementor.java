/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 Xiaoyang
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

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * FarragoOJRexSubstringImplementor implements Farrago specifics of {@link
 * OJRexImplementor} for SUBSTRING expressions.
 *
 * @author Xiaoyang Luo
 * @version $Id$
 */
public class FarragoOJRexSubstringImplementor extends FarragoOJRexImplementor
{
    //~ Methods ---------------------------------------------------------------

    // implement FarragoOJRexImplementor
    public Expression implementFarrago(
        FarragoRexToOJTranslator translator,
        RexCall call,
        Expression [] operands)
    {
        boolean bHasLength = false;
        int i;
        
        if (operands.length > 2) {
            bHasLength = true;
        }
        
        Variable varResult = translator.createScratchVariable(call.getType());

        Expression nullTest = null;
        for (i = 0; i < operands.length; i++) {
            nullTest = translator.createNullTest(
                call.operands[i], operands[i], nullTest);
        }

        StatementList stmtList = new StatementList();
        translator.convertCastOrAssignmentWithStmtList(
                        stmtList,
                        call.toString(),
                        call.getType(),
                        call.operands[0].getType(),
                        varResult,
                        operands[0]);
        // if any of the operants is null, the result is null.
        //
        // now. no null possibility.
        //
        //
        Expression intS = translator.convertPrimitiveAccess(
            operands[1], call.operands[1]);
        Expression intL = Literal.constantZero();
        if (bHasLength) {
            intL = translator.convertPrimitiveAccess(
                        operands[2], call.operands[2]);
        }

        ExpressionList expList = new ExpressionList(
                        new BinaryExpression(
                            intS,
                            BinaryExpression.MINUS, 
                            Literal.constantOne()), 
                        intL);;
        if (bHasLength) {
            expList.add(Literal.constantTrue());
        } else {
            expList.add(Literal.constantFalse());
        }
        Statement callStmt = new ExpressionStatement(
                        new MethodCall(
                            varResult,
                            "setSubstring",
                            expList));
        stmtList.add(callStmt);

        if (nullTest != null) {
            translator.addStatement(
                new IfStatement(
                    nullTest,
                    new StatementList(
                        translator.createSetNullStatement(varResult, true)),
                    stmtList));
        } else {
            for (i = 0; i < stmtList.size(); i++) {
                translator.addStatement(stmtList.get(i));
            }
        }
        return varResult;
    }

    // implement OJRexImplementor
    public boolean canImplement(RexCall call)
    {
        return true;
    }
}

// End FarragoOJRexSubstringImplementor.java
