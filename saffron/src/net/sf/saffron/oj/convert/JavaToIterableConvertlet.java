/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Tech
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

import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.oj.rel.JavaRelImplementor;
import net.sf.saffron.oj.rel.JavaRel;
import net.sf.saffron.oj.util.UnboundVariableCollector;
import net.sf.saffron.runtime.ThreadIterator;
import net.sf.saffron.rel.convert.ConverterRel;
import openjava.ptree.*;
import openjava.mop.OJClass;
import openjava.mop.OJSystem;

/**
 * Thunk to convert between {@link CallingConvention#JAVA java}
 * and {@link CallingConvention#ITERABLE iterable} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class JavaToIterableConvertlet extends JavaConvertlet {
    public JavaToIterableConvertlet() {
        super(CallingConvention.JAVA, CallingConvention.ITERABLE);
    }

    public void implementJavaParent(JavaRelImplementor implementor,
            ConverterRel converter) {
        StatementList stmtList = implementor.getStatementList();
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                    (Expression) null,
                    "put",
                    new ExpressionList(
                        implementor.translate(
                                (JavaRel) converter,
                                converter.getCluster().rexBuilder.makeRangeReference(converter.child.getRowType()))))));
    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        UnboundVariableCollector unboundVars =
            UnboundVariableCollector.collectFromRel(converter);
        StatementList body = new StatementList();
        implementor.pushStatementList(body);
        implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
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
    }
}

// End JavaToIterableConvertlet.java
