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

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.oj.util.UnboundVariableCollector;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.RelImplementor;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.convert.ConverterFactory;
import net.sf.saffron.rel.convert.ConverterRel;
import net.sf.saffron.rel.convert.FactoryConverterRule;
import net.sf.saffron.util.Util;

import openjava.ptree.*;


/**
 * An <code>ExistsConverterRel</code> converts a relational expression node
 * ({@link SaffronRel}) to {@link
 * net.sf.saffron.opt.CallingConvention#EXISTS_ORDINAL} calling convention: an
 * expression which yields <code>true</code> if the relation returns at least
 * one row.
 */
public class ExistsConverterRel extends ConverterRel
{
    //~ Constructors ----------------------------------------------------------

    public ExistsConverterRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.EXISTS;
    }

    public Object clone()
    {
        return new ExistsConverterRel(cluster,OptUtil.clone(child));
    }

    public static void init(SaffronPlanner planner)
    {
        final ConverterFactory factory =
            new ConverterFactory() {
                public ConverterRel convert(SaffronRel rel)
                {
                    return new ExistsConverterRel(rel.getCluster(),rel);
                }

                public CallingConvention getConvention()
                {
                    return CallingConvention.EXISTS;
                }
            };
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.JAVA));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ARRAY));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ITERATOR));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.MAP));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.HASHTABLE));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.ENUMERATION));
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.COLLECTION));
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (inConvention.ordinal_) {
        case CallingConvention.ITERATOR_ORDINAL:
            return implementIterator(implementor,ordinal);
        case CallingConvention.JAVA_ORDINAL:
            return implementJava(implementor,ordinal);
        case CallingConvention.COLLECTION_ORDINAL:
            return implementCollection(implementor,ordinal);
        case CallingConvention.ARRAY_ORDINAL:
            return implementArray(implementor,ordinal);
        case CallingConvention.ENUMERATION_ORDINAL:
            return implementEnumeration(implementor,ordinal);
        case CallingConvention.MAP_ORDINAL:
            return implementMap(implementor,ordinal);
        case CallingConvention.HASHTABLE_ORDINAL:
            return implementHashtable(implementor,ordinal);
        default:
            return super.implement(implementor,ordinal);
        }
    }

    private Object implementArray(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new BinaryExpression(
                exp,
                BinaryExpression.GREATER,
                Literal.constantZero());
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementCollection(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new UnaryExpression(
                UnaryExpression.NOT,
                new MethodCall(exp,"isEmpty",new ExpressionList()));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementEnumeration(
        RelImplementor implementor,
        int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new MethodCall(exp,"hasNext",new ExpressionList());
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementHashtable(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new UnaryExpression(
                UnaryExpression.NOT,
                new MethodCall(exp,"isEmpty",new ExpressionList()));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementIterator(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new MethodCall(exp,"hasNext",new ExpressionList());
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementJava(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
         {
            // Generate
            //   new Object() {
            //     boolean anyRows(C0 v0, ...) {
            //       <<start loop>>
            //         return true;
            //       <<end loop>>
            //       return false;
            //     }
            //   }.anyRows(v0, ...)
            // Find all unbound variables in expressions in this tree
            UnboundVariableCollector unboundVars =
                UnboundVariableCollector.collectFromRel(this);

            StatementList stmtList = new StatementList();
            implementor.pushStatementList(stmtList);
            Object o = implementor.implementChild(this,0,child);
            assert(o == null);
            implementor.popStatementList(stmtList);

            stmtList.add(new ReturnStatement(Literal.constantFalse()));
            return new MethodCall(
                new AllocationExpression(
                    TypeName.forClass(Object.class), // "Object"
                    null, // "()"
                    
            // "public boolean anyRows(C0 v0, ...) { ... }"
            new MemberDeclarationList(
                        new MethodDeclaration(
                            new ModifierList(ModifierList.PUBLIC),
                            TypeName.forClass(boolean.class), // "boolean"
                            "anyRows",
                            unboundVars.getParameterList(), // "(C0 v0, ...)"
                            null, // throws nothing
                            stmtList))),
                "anyRows",
                unboundVars.getArgumentList());
        }
        case 0: // called from child
         {
            // Generate
            //   return true;
            StatementList stmtList = implementor.getStatementList();
            stmtList.add(new ReturnStatement(Literal.constantTrue()));
            return null;
        }
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }

    private Object implementMap(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
            Expression exp =
                (Expression) implementor.implementChild(this,0,child);
            return new UnaryExpression(
                UnaryExpression.NOT,
                new MethodCall(exp,"isEmpty",new ExpressionList()));
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }
}


// End ExistsConverterRel.java
