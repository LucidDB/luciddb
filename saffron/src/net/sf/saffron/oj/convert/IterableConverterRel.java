/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.oj.convert;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.runtime.*;
import net.sf.saffron.util.*;

import openjava.mop.*;

import openjava.ptree.*;


/**
 * Converts a relational expression to {@link CallingConvention#ITERABLE}
 * calling convention.
 */
public class IterableConverterRel extends ConverterRel
{
    //~ Constructors ----------------------------------------------------------

    public IterableConverterRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.ITERABLE;
    }

    public Object clone()
    {
        return new IterableConverterRel(this.cluster,child);
    }

    public static void init(SaffronPlanner planner)
    {
        final ConverterFactory factory =
            new ConverterFactory() {
                public CallingConvention getConvention()
                {
                    return CallingConvention.ITERABLE;
                }

                public ConverterRel convert(SaffronRel rel)
                {
                    return new IterableConverterRel(rel.getCluster(),rel);
                }
            };
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.JAVA));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ITERATOR));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.COLLECTION));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.VECTOR));
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (inConvention.ordinal_) {
        case CallingConvention.JAVA_ORDINAL:
            return implementJava(implementor,ordinal);
        case CallingConvention.ITERATOR_ORDINAL:
            return implementIterator(implementor,ordinal);
        case CallingConvention.COLLECTION_ORDINAL:
        case CallingConvention.VECTOR_ORDINAL:
            return implementCollection(implementor,ordinal);
        default:
            return super.implement(implementor,ordinal);
        }
    }

    private Object implementCollection(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   <<exp>>
        //
        // (which is not of the same type, but it should work, because
        // they're just going to call 'iterator' on it)
        return implementor.implementChild(this,0,child);
    }

    private Object implementIterator(RelImplementor implementor,int ordinal)
    {
        assert (ordinal == -1) : "Cannot implement callback from child";

        // Generate
        //   new saffron.runtime.BufferedIterator(<<child>>)
        Expression exp = (Expression) implementor.implementChild(this,0,child);
        return new AllocationExpression(
            OJClass.forClass(BufferedIterator.class),
            new ExpressionList(exp));
    }

    private Object implementJava(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            UnboundVariableCollector unboundVars =
                UnboundVariableCollector.collectFromRel(this);
            StatementList body = new StatementList();
            implementor.pushStatementList(body);
            implementor.implementChild(this,0,child);
            implementor.popStatementList(body);

            // private C0 v0;
            // ...
            MemberDeclarationList memberDeclarationList =
                unboundVars.getMemberDeclarationList();

            // this.v0 = v0;
            // ...
            // return this;
            StatementList initBody = unboundVars.getAssignmentList();
            initBody.add(new ReturnStatement(SelfAccess.constantThis()));

            memberDeclarationList.add(
                new MethodDeclaration(
                    new ModifierList(ModifierList.PUBLIC),
                    TypeName.forOJClass(
                        OJClass.forClass(ThreadIterator.class)),
                    "init",
                    unboundVars.getParameterList(),
                    null,
                    initBody));
            memberDeclarationList.add(
                new MethodDeclaration(
                    new ModifierList(ModifierList.PROTECTED),
                    TypeName.forOJClass(OJSystem.VOID),
                    "doWork",
                    new ParameterList(),
                    null,
                    body));
            return new MethodCall(
                new AllocationExpression(
                    TypeName.forOJClass(
                        OJClass.forClass(ThreadIterator.class)),
                    new ExpressionList(),
                    memberDeclarationList),
                "init",
                unboundVars.getArgumentList());
        case 0:
            StatementList stmtList = implementor.getStatementList();
            stmtList.add(
                new ExpressionStatement(
                    new MethodCall(
                        (Expression) null,
                        "put",
                        new ExpressionList(
                            implementor.translate(
                                this,
                                cluster.rexBuilder.makeRangeReference(child.getRowType()))))));
            return null;
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }
}


// End IterableConverterRel.java
