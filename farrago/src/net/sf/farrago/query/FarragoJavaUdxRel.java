/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.runtime.*;

import net.sf.farrago.runtime.*;

/**
 * FarragoJavaUdxRel is the implementation for a {@link
 * TableFunctionRel} which invokes a Java UDX (user-defined transformation).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJavaUdxRel extends TableFunctionRelBase
    implements JavaRel
{
    private final String serverMofId;
    
    /**
     * Creates a <code>FarragoJavaUdxRel</code>.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     *
     * @param rexCall function invocation expression
     *
     * @param rowType row type produced by function
     *
     * @param serverMofId MOFID of data server to associate with this UDX
     * invocation, or null for none
     *
     * @param inputs 0 or more relational inputs
     */
    public FarragoJavaUdxRel(
        RelOptCluster cluster, RexNode rexCall, RelDataType rowType,
        String serverMofId, RelNode [] inputs)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.ITERATOR),
            rexCall,
            rowType,
            inputs);
        this.serverMofId = serverMofId;
    }

    /**
     * Creates a <code>FarragoJavaUdxRel</code> with no relational
     * inputs.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     *
     * @param rexCall function invocation expression
     *
     * @param rowType row type produced by function
     *
     * @param serverMofId MOFID of data server to associate with this UDX
     * invocation, or null for none
     */
    public FarragoJavaUdxRel(
        RelOptCluster cluster, RexNode rexCall, RelDataType rowType,
        String serverMofId)
    {
        this(cluster, rexCall, rowType, serverMofId, RelNode.emptyArray);
    }

    // implement RelNode
    public Object clone()
    {
        FarragoJavaUdxRel clone = new FarragoJavaUdxRel(
            getCluster(),
            getCall(),
            getRowType(),
            serverMofId,
            RelOptUtil.clone(inputs));
        clone.inheritTraitsFrom(this);
        return clone;
    }
    
    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO jvs 8-Jan-2006:  get estimate from user or history
        return planner.makeTinyCost();
    }

    // override TableFunctionRelBase
    public void explain(RelOptPlanWriter pw)
    {
        if (serverMofId == null) {
            super.explain(pw);
            return;
        }

        // NOTE jvs 7-Mar-2006:  including the serverMofId means
        // we can't use EXPLAIN PLAN in diff-based testing because
        // the MOFID isn't deterministic.
        
        pw.explain(
            this,
            new String [] { "invocation", "serverMofId" },
            new Object [] { serverMofId } );
    }
    
    // implement JavaRel
    public ParseTree implement(JavaRelImplementor implementor)
    {
        final RelDataType outputRowType = getRowType();
        OJClass outputRowClass = OJUtil.typeToOJClass(
            outputRowType,
            implementor.getTypeFactory());

        // Translate relational inputs to ResultSet expressions.
        final Expression [] childExprs = new Expression[inputs.length];
        for (int i = 0; i < inputs.length; ++i) {
            childExprs[i] =
                implementor.visitJavaChild(this, i, (JavaRel) inputs[i]);
            OJClass rowClass = OJUtil.typeToOJClass(
                inputs[i].getRowType(), getCluster().getTypeFactory());

            Expression typeLookupCall = generateTypeLookupCall(
                implementor,
                inputs[i]);
        
            ExpressionList resultSetParams = new ExpressionList();
            resultSetParams.add(childExprs[i]);
            resultSetParams.add(new ClassLiteral(rowClass));
            resultSetParams.add(typeLookupCall);
            resultSetParams.add(Literal.constantNull());

            childExprs[i] = new AllocationExpression(
                OJUtil.typeNameForClass(FarragoTupleIterResultSet.class),
                resultSetParams);
        }

        // Rebind RexInputRefs accordingly.
        final JavaRexBuilder rexBuilder =
            (JavaRexBuilder) implementor.getRexBuilder();
        RexShuttle shuttle = new RexShuttle()
            {
                public RexNode visitInputRef(RexInputRef inputRef)
                {
                    return rexBuilder.makeJava(
                        getCluster().getEnv(),
                        childExprs[inputRef.getIndex()]);
                }
            };
        RexNode rewrittenCall = getCall().accept(shuttle);
        
        MemberDeclarationList memberList = new MemberDeclarationList();
        
        StatementList executeMethodBody = new StatementList();

        // Set up server MOFID context while generating method call
        // so that it will be available to the UDX at runtime in case
        // it needs to call back to the foreign data server.
        FarragoRelImplementor farragoImplementor =
            (FarragoRelImplementor) implementor;
        farragoImplementor.setServerMofId(serverMofId);
        implementor.translateViaStatements(
            this,
            rewrittenCall,
            executeMethodBody,
            memberList);
        farragoImplementor.setServerMofId(null);

        MemberDeclaration executeMethodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                TypeName.forOJClass(OJSystem.VOID), "executeUdx",
                new ParameterList(), null, executeMethodBody);
        memberList.add(executeMethodDecl);

        Expression typeLookupCall = generateTypeLookupCall(
            implementor,
            this);
        
        Expression iteratorExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(FarragoJavaUdxIterator.class),
                new ExpressionList(
                    implementor.getConnectionVariable(),
                    new ClassLiteral(TypeName.forOJClass(outputRowClass)),
                    typeLookupCall),
                memberList);

        // TODO jvs 23-Feb-2006:  get rid of adapter and write
        // a new TupleIter implementation so that we can take
        // advantage of the closeAllocation call.
        Expression tupleIterExp = new AllocationExpression(
            OJUtil.typeNameForClass(RestartableIteratorTupleIter.class),
            new ExpressionList(
                iteratorExp));
        return tupleIterExp;
    }

    /**
     * Stores the row type for a relational expression in the PreparingStmt,
     * and generates a call which will retrieve it from the executable context
     * at runtime.  The literal string key used is based on the relational
     * expression id.
     */
    private Expression generateTypeLookupCall(
        JavaRelImplementor implementor,
        RelNode relNode)
    {
        String resultSetName = "ResultSet:" + relNode.getId();
        FarragoPreparingStmt preparingStmt =
            ((FarragoRelImplementor) implementor).getPreparingStmt();
        preparingStmt.mapResultSetType(
            resultSetName,
            relNode.getRowType());

        MethodCall typeLookupCall =
            new MethodCall(
                implementor.getConnectionVariable(),
                "getRowTypeForResultSet",
                new ExpressionList(
                    Literal.makeLiteral(resultSetName)));

        return typeLookupCall;
    }
}

// End FarragoJavaUdxRel.java
