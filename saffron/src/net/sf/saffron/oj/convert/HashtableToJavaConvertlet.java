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
import net.sf.saffron.oj.util.OJUtil;
import net.sf.saffron.util.Util;
import net.sf.saffron.rel.convert.ConverterRel;
import openjava.ptree.*;
import openjava.mop.Toolbox;
import openjava.mop.OJClass;

/**
 * Thunk to convert between {@link CallingConvention#HASHTABLE}
 * and {@link CallingConvention#JAVA java} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class HashtableToJavaConvertlet extends JavaConvertlet {
    public HashtableToJavaConvertlet() {
        super(CallingConvention.HASHTABLE,CallingConvention.JAVA);
    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        // Generate
        //   Hashtable h = <<exp>>;
        //   for (Enumeration keys = h.keys(); keys.hasMoreElements();) {
        //     Object key = keys.nextElement();
        //     Object value = h.get(key);
        //     Row row = new Row(key, value);
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_h = implementor.newVariable();
        Variable variable_keys = implementor.newVariable();
        Variable variable_key = implementor.newVariable();
        Variable variable_value = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(Util.clazzHashtable),
                variable_h.toString(),
                exp));
        stmtList.add(
            new ForStatement(
                TypeName.forOJClass(Util.clazzEnumeration),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_keys.toString(),
                        new MethodCall(variable_h,"keys",null))
                },
                new MethodCall(variable_keys,"hasMoreElements",null),
                new ExpressionList(),
                forBody));
        forBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzObject),
                new VariableDeclarator(
                    variable_key.toString(),
                    new MethodCall(variable_keys,"nextElement",null))));
        forBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzObject),
                new VariableDeclarator(
                    variable_value.toString(),
                    new MethodCall(
                        variable_h,
                        "get",
                        new ExpressionList(variable_key)))));
        OJClass rowType = OJUtil.typeToOJClass(converter.getRowType());
        Variable variable_row =
            implementor.bind(
                    converter,
                forBody,
                new AllocationExpression(
                    TypeName.forOJClass(rowType),
                    new ExpressionList(variable_key,variable_value)));
        Util.discard(variable_row);
        implementor.generateParentBody(converter,forBody);
        return null;
    }
}

// End HashtableToJavaConvertlet.java
