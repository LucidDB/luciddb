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
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.rel.convert.ConverterRel;
import net.sf.saffron.util.Util;
import openjava.ptree.*;

import java.util.ArrayList;

/**
 * Thunk to convert between {@link CallingConvention#JAVA java}
 * and {@link CallingConvention#COLLECTION collection} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class JavaToCollectionConvertlet extends JavaConvertlet {
    public JavaToCollectionConvertlet() {
        super(CallingConvention.JAVA,CallingConvention.COLLECTION);
    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        // Find all unbound variables in expressions in this tree
        UnboundVariableCollector unboundVars =
                UnboundVariableCollector.collectFromRel(converter);

        // Generate
        //   new Object() {
        //       /** Executes <code> ... </code>. **/
        //       ArrayList asArrayList(C0 v0, ...) {
        //         ArrayList v = new ArrayList();
        //         <<child loop
        //           v.add(i);
        //         >>
        //         return v;
        //       }
        //     }.asArrayList(v0, ...)
        Variable var_v = ((JavaConverterRel) converter).var_v = implementor.newVariable();
        implementor.setExitStatement(new ReturnStatement(var_v));
        StatementList stmtList =
                new StatementList(

                        // "ArrayList v = new ArrayList();"
                        new VariableDeclaration(
                                null, // no modifiers
                                TypeName.forClass(ArrayList.class),
                                new VariableDeclarator(
                                        var_v.toString(),
                                        new AllocationExpression(
                                                TypeName.forClass(ArrayList.class),
                                                null))));

        // Give child chance to write its code into "stmtList" (and to
        // call us back so we can write "v.add(i);".
        implementor.pushStatementList(stmtList);
        Expression o = implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        assert(o == null);
        implementor.popStatementList(stmtList);

        // "return v;"
        stmtList.add(new ReturnStatement(var_v));

        // "public void asArrayList(C0 v0, ...) { ... }"
        MethodDeclaration asArrayList =
                new MethodDeclaration(
                        new ModifierList(ModifierList.PUBLIC),
                        TypeName.forClass(ArrayList.class),
                        "asArrayList",
                        unboundVars.getParameterList(), // "(C0 v0, ...)"
                        new TypeName [] { TypeName.forOJClass(
                                Util.clazzSQLException) },
                        stmtList);
        asArrayList.setComment(
                "/** Evaluates <code>"
                + converter.getCluster().getOriginalExpression().toString()
                + "</code> and returns the results as a vector. **/");

        return new MethodCall(
                new AllocationExpression(
                        TypeName.forClass(Object.class), // "Object"
                        null, // "()"
                        new MemberDeclarationList(asArrayList)),
                "asArrayList",
                unboundVars.getArgumentList());
    }

    public void implementJavaParent(JavaRelImplementor implementor,
            ConverterRel converter) {
        // Generate
        //   v.add(i)
        //   Rowtype[] variable = <<child variable>>;
        //   <<parent body (references variable)>>
        StatementList stmtList = implementor.getStatementList();
        final JavaConverterRel javaConverter = (JavaConverterRel) converter;
        stmtList.add(
            new ExpressionStatement(
                new MethodCall(
                        javaConverter.var_v,
                    "add",
                    new ExpressionList(
                        Util.box(
                            OJUtil.typeToOJClass(converter.child.getRowType()),
                            implementor.translateInput(javaConverter, 0))))));
    }
}

// End JavaToCollectionConvertlet.java
