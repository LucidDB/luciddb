/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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
import net.sf.saffron.oj.*;
import net.sf.saffron.oj.util.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.util.*;

import openjava.ptree.*;

import java.util.*;


/**
 * A <code>VectorConverterRel</code> converts a plan from
 * <code>inConvention</code> to {@link
 * net.sf.saffron.opt.CallingConvention#VECTOR_ORDINAL}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 16 December, 2001
 */
public class VectorConverterRel extends ConverterRel
{
    //~ Instance fields -------------------------------------------------------

    Variable var_v;

    //~ Constructors ----------------------------------------------------------

    public VectorConverterRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster,child);
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return CallingConvention.VECTOR;
    }

    public Object clone()
    {
        return new VectorConverterRel(cluster,child);
    }

    public static void init(SaffronPlanner planner)
    {
        final ConverterFactory factory =
            new ConverterFactory() {
                public CallingConvention getConvention()
                {
                    return CallingConvention.VECTOR;
                }

                public ConverterRel convert(SaffronRel rel)
                {
                    return new VectorConverterRel(rel.getCluster(),rel);
                }
            };
        planner.addRule(
            new FactoryConverterRule(factory,CallingConvention.JAVA));
    }

    public Object implement(RelImplementor implementor,int ordinal)
    {
        switch (inConvention.ordinal_) {
        case CallingConvention.JAVA_ORDINAL:
            return implementJava(implementor,ordinal);
        default:
            return super.implement(implementor,ordinal);
        }
    }

    private Object implementJava(RelImplementor implementor,int ordinal)
    {
        switch (ordinal) {
        case -1: // called from parent
         {
            // Find all unbound variables in expressions in this tree
            UnboundVariableCollector unboundVars =
                UnboundVariableCollector.collectFromRel(this);

            // Generate
            //   new Object() {
            //       /** Executes <code> ... </code>. **/
            //       Vector asVector(C0 v0, ...) {
            //         Vector v = new Vector();
            //         <<child loop
            //           v.addElement(i);
            //         >>
            //         return v;
            //       }
            //     }.asVector(v0, ...)
            this.var_v = implementor.newVariable();
            implementor.setExitStatement(new ReturnStatement(var_v));
            StatementList stmtList =
                new StatementList(
                    
                // "Vector v = new Vector();"
                new VariableDeclaration(
                        null, // no modifiers
                        TypeName.forClass(Vector.class),
                        new VariableDeclarator(
                            var_v.toString(),
                            new AllocationExpression(
                                TypeName.forClass(Vector.class),
                                null))));

            // Give child chance to write its code into "stmtList" (and to
            // call us back so we can write "v.addElement(i);".
            implementor.pushStatementList(stmtList);
            Object o = implementor.implementChild(this,0,child);
            assert(o == null);
            implementor.popStatementList(stmtList);

            // "return v;"
            stmtList.add(new ReturnStatement(var_v));

            // "public void asVector(C0 v0, ...) { ... }"
            MethodDeclaration asVector =
                new MethodDeclaration(
                    new ModifierList(ModifierList.PUBLIC),
                    TypeName.forClass(Vector.class),
                    "asVector",
                    unboundVars.getParameterList(), // "(C0 v0, ...)"
                    null, // throws nothing
                    stmtList);
            asVector.setComment(
                "/** Evaluates <code>"
                + cluster.getOriginalExpression().toString()
                + "</code> and returns the results as a vector. **/");

            return new MethodCall(
                new AllocationExpression(
                    TypeName.forClass(Object.class), // "Object"
                    null, // "()"
                    new MemberDeclarationList(asVector)),
                "asVector",
                unboundVars.getArgumentList());
        }
        case 0: // called from child
         {
            // Generate
            //   v.addElement(i)
            //   Rowtype[] variable = <<child variable>>;
            //   <<parent body (references variable)>>
            StatementList stmtList = implementor.getStatementList();
            stmtList.add(
                new ExpressionStatement(
                    new MethodCall(
                        var_v,
                        "addElement",
                        new ExpressionList(
                            Util.box(
                                OJUtil.typeToOJClass(child.getRowType()),
                                implementor.translateInput(this,0))))));
            return null;
        }
        default:
            throw Util.newInternal("implement: ordinal=" + ordinal);
        }
    }
}


// End VectorConverterRel.java
