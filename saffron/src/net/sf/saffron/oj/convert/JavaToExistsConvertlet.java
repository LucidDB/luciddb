/*
// Saffron preprocessor and data engine.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package net.sf.saffron.oj.convert;

import openjava.ptree.*;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.relopt.CallingConvention;


/**
 * Thunk to convert between {@link CallingConvention#JAVA java}
 * and {@link CallingConvention#EXISTS exists} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class JavaToExistsConvertlet extends JavaConvertlet
{
    public JavaToExistsConvertlet()
    {
        super(CallingConvention.JAVA, CallingConvention.EXISTS);
    }

    public void implementJavaParent(
        JavaRelImplementor implementor,
        ConverterRel converter)
    {
        // Generate
        //   return true;
        StatementList stmtList = implementor.getStatementList();
        stmtList.add(new ReturnStatement(Literal.constantTrue()));
    }

    public ParseTree implement(
        JavaRelImplementor implementor,
        ConverterRel converter)
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
            UnboundVariableCollector.collectFromRel(converter);

        StatementList stmtList = new StatementList();
        implementor.pushStatementList(stmtList);
        Object o =
            implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        assert (o == null);
        implementor.popStatementList(stmtList);

        stmtList.add(new ReturnStatement(Literal.constantFalse()));
        return new MethodCall(
            new AllocationExpression(
                OJUtil.typeNameForClass(Object.class), // "Object"
                null, // "()"
                
        // "public boolean anyRows(C0 v0, ...) { ... }"
        new MemberDeclarationList(
                    new MethodDeclaration(
                        new ModifierList(ModifierList.PUBLIC),
                        OJUtil.typeNameForClass(boolean.class), // "boolean"
                        "anyRows",
                        unboundVars.getParameterList(), // "(C0 v0, ...)"
                        null, // throws nothing
                        stmtList))),
            "anyRows",
            unboundVars.getArgumentList());
    }
}


// End JavaToExistsConvertlet.java
