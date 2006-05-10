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
            getCall(),
            executeMethodBody,
            memberList);
        farragoImplementor.setServerMofId(null);

        MemberDeclaration executeMethodDecl =
            new MethodDeclaration(new ModifierList(ModifierList.PROTECTED),
                TypeName.forOJClass(OJSystem.VOID), "executeUdx",
                new ParameterList(), null, executeMethodBody);
        memberList.add(executeMethodDecl);

        Expression iteratorExp =
            new AllocationExpression(
                OJUtil.typeNameForClass(FarragoJavaUdxIterator.class),
                new ExpressionList(
                    implementor.getConnectionVariable(),
                    new ClassLiteral(TypeName.forOJClass(outputRowClass))),
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
}

// End FarragoJavaUdxRel.java
