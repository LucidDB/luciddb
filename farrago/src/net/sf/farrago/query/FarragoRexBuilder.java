/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.query;

import java.math.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.*;


/**
 * FarragoRexBuilder refines JavaRexBuilder with Farrago-specific details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRexBuilder
    extends JavaRexBuilder
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoPreparingStmt preparingStmt;

    //~ Constructors -----------------------------------------------------------

    public FarragoRexBuilder(FarragoPreparingStmt preparingStmt)
    {
        super(preparingStmt.getFarragoTypeFactory());

        this.preparingStmt = preparingStmt;
    }

    //~ Methods ----------------------------------------------------------------

    // override JavaRexBuilder
    public RexLiteral makeLiteral(String s)
    {
        return makePreciseStringLiteral(s);
    }

    public FarragoPreparingStmt getPreparingStmt()
    {
        return preparingStmt;
    }

    // override RexBuilder
    public RexNode makeCall(
        SqlOperator op,
        RexNode ... exprs)
    {
        if (op.getKind().belongsTo(SqlKind.COMPARISON)) {
            return makeComparison(op, exprs);
        } else if (op instanceof FarragoUserDefinedRoutine) {
            return makeUdfInvocation(op, exprs);
        } else {
            return super.makeCall(op, exprs);
        }
    }

    private RexNode makeUdfInvocation(
        SqlOperator op,
        RexNode [] exprs)
    {
        FarragoUserDefinedRoutine routine = (FarragoUserDefinedRoutine) op;
        FemRoutine femRoutine = routine.getFemRoutine();
        FarragoRoutineInvocation invocation =
            new FarragoRoutineInvocation(
                routine,
                exprs);

        RexNode returnNode;
        if (femRoutine.getBody().getLanguage().equals("SQL")) {
            // replace calls to SQL-defined routines by
            // inline expansion of body
            String bodyString = femRoutine.getBody().getBody();
            bodyString =
                FarragoUserDefinedRoutine.removeReturnPrefix(
                    bodyString);
            SqlParser parser = new SqlParser(bodyString);
            SqlNode sqlExpr;
            try {
                sqlExpr = parser.parseExpression();
            } catch (SqlParseException e) {
                throw Util.newInternal(
                    e,
                    "Error while parsing routine definition:  " + bodyString);
            }
            returnNode =
                preparingStmt.expandInvocationExpression(
                    sqlExpr,
                    invocation);
        } else {
            // leave calls to external functions alone
            returnNode =
                super.makeCall(
                    routine.getReturnType(),
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
            List<RexNode> caseOperandList = new ArrayList<RexNode>();
            for (int i = 0; i < paramTypes.length; ++i) {
                // REVIEW jvs 17-Jan-2005: This assumes that CAST will never
                // convert a non-NULL value into a NULL.  If that's not true,
                // we should be referencing the arg CAST result instead.
                caseOperandList.add(
                    makeCall(
                        SqlStdOperatorTable.isNullOperator,
                        exprs[i]));
                caseOperandList.add(
                    makeCast(
                        routine.getReturnType(),
                        constantNull()));
            }
            caseOperandList.add(returnNode);
            RexNode nullCase =
                makeCall(
                    SqlStdOperatorTable.caseOperator,
                    caseOperandList);
            returnNode = nullCase;
        }

        RexNode returnCast =
            makeCast(
                routine.getReturnType(),
                returnNode);
        return returnCast;
    }

    private RexNode makeComparison(
        SqlOperator op,
        RexNode [] exprs)
    {
        RelDataType type = exprs[0].getType();
        if (!type.isStruct()) {
            return super.makeCall(op, exprs);
        }
        SqlIdentifier typeName = type.getSqlIdentifier();
        CwmSqldataType cwmType =
            preparingStmt.getStmtValidator().findSqldataType(typeName);
        if (cwmType instanceof FemUserDefinedType) {
            FemUserDefinedType udt = (FemUserDefinedType) cwmType;
            assert (udt.getOrdering().size() == 1);
            FemUserDefinedOrdering udo = udt.getOrdering().iterator().next();
            preparingStmt.addDependency(udo, null);
            UserDefinedOrderingCategory udoc = udo.getCategory();
            if (udoc == UserDefinedOrderingCategoryEnum.UDOC_RELATIVE) {
                return makeRelativeComparison(udt, udo, op, exprs);
            } else if (udoc == UserDefinedOrderingCategoryEnum.UDOC_MAP) {
                return makeMapComparison(udt, udo, op, exprs);
            } else {
                assert (udoc == UserDefinedOrderingCategoryEnum.UDOC_STATE);
            }
        }

        // leave this for RelStructuredTypeFlattener to handle
        // like a ROW
        return super.makeCall(op, exprs);
    }

    private RexNode makeRelativeComparison(
        FemUserDefinedType udt,
        FemUserDefinedOrdering udo,
        SqlOperator op,
        RexNode [] exprs)
    {
        FarragoUserDefinedRoutine routine = getRoutine(udo);
        RexNode routineInvocation = makeUdfInvocation(routine, exprs);
        return super.makeCall(
            op,
            routineInvocation,
            makeExactLiteral(new BigDecimal(BigInteger.ZERO)));
    }

    private RexNode makeMapComparison(
        FemUserDefinedType udt,
        FemUserDefinedOrdering udo,
        SqlOperator op,
        RexNode [] exprs)
    {
        FarragoUserDefinedRoutine routine = getRoutine(udo);
        RexNode [] mappedExprs = new RexNode[exprs.length];
        for (int i = 0; i < exprs.length; ++i) {
            mappedExprs[i] =
                makeUdfInvocation(
                    routine,
                    new RexNode[] { exprs[i] });
        }
        return super.makeCall(op, mappedExprs);
    }

    private FarragoUserDefinedRoutine getRoutine(FemUserDefinedOrdering udo)
    {
        FemRoutine routine = FarragoCatalogUtil.getRoutineForOrdering(udo);
        assert (routine != null);
        return preparingStmt.getRoutineLookup().convertRoutine(routine);
    }
}

// End FarragoRexBuilder.java
