/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.query;

import net.sf.farrago.type.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.cwm.behavioral.*;

import org.eigenbase.util.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.reltype.*;

import java.util.*;

/**
 * FarragoRexBuilder refines JavaRexBuilder with Farrago-specific details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoRexBuilder extends JavaRexBuilder
{
    private final FarragoPreparingStmt preparingStmt;
    
    //~ Constructors ----------------------------------------------------------

    FarragoRexBuilder(FarragoPreparingStmt preparingStmt)
    {
        super(preparingStmt.getFarragoTypeFactory());

        this.preparingStmt = preparingStmt;
    }

    //~ Methods ---------------------------------------------------------------

    // override JavaRexBuilder
    public RexLiteral makeLiteral(String s)
    {
        return makePreciseStringLiteral(s);
    }

    // override RexBuilder
    public RexNode makeCall(
        SqlOperator op,
        RexNode [] exprs)
    {
        if (!(op instanceof FarragoUserDefinedRoutine)) {
            return super.makeCall(op, exprs);
        }

        FarragoUserDefinedRoutine routine = (FarragoUserDefinedRoutine) op;
        FemRoutine femRoutine = routine.getFemRoutine();
        FarragoRoutineInvocation invocation = new FarragoRoutineInvocation(
            routine, exprs);

        RexNode returnNode;
        if (femRoutine.getBody().getLanguage().equals("SQL")) {
            // replace calls to SQL-defined routines by
            // inline expansion of body
            String bodyString = femRoutine.getBody().getBody();
            bodyString = FarragoUserDefinedRoutine.removeReturnPrefix(
                bodyString);
            SqlParser parser = new SqlParser(bodyString);
            SqlNode sqlExpr;
            try {
                sqlExpr = parser.parseExpression();
            } catch (SqlParseException e) {
                throw Util.newInternal(e,
                    "Error while parsing routine definition:  " + bodyString);
            }
            returnNode = preparingStmt.expandInvocationExpression(
                sqlExpr,
                invocation);
        } else {
            // leave calls to external functions alone
            returnNode = super.makeCall(
                op,
                invocation.getArgCastExprs());
        }

        RelDataType [] paramTypes = routine.getParamTypes();
        if (!femRoutine.isCalledOnNullInput()
            && (paramTypes.length > 0))
        {
            // To honor RETURNS NULL ON NULL INPUT,  we build up
            // CASE WHEN arg1 IS NULL THEN NULL
            // WHEN arg2 IS NULL THEN NULL
            // ...
            // ELSE invokeUDF(arg1, arg2, ...) END
            List caseOperandList = new ArrayList();
            for (int i = 0; i < paramTypes.length; ++i) {
                // REVIEW jvs 17-Jan-2005: This assumes that CAST will never
                // convert a non-NULL value into a NULL.  If that's not true,
                // we should be referencing the arg CAST result instead.
                caseOperandList.add(
                    makeCall(
                        opTab.isNullOperator,
                        exprs[i]));
                caseOperandList.add(
                    makeLiteral(
                        null,
                        routine.getReturnType(),
                        SqlTypeName.Null));
            }
            caseOperandList.add(returnNode);
            RexNode [] caseOperands = (RexNode [])
                caseOperandList.toArray(new RexNode[0]);
            RexNode nullCase = makeCall(
                opTab.caseOperator,
                caseOperands);
            returnNode = nullCase;
        }

        RexNode returnCast = makeCast(routine.getReturnType(), returnNode);
        return returnCast;
    }
}

// End FarragoRexBuilder.java
