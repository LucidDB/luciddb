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
import openjava.mop.OJClass;
import openjava.mop.Toolbox;

/**
 * Thunk to convert between {@link CallingConvention#VECTOR vector}
 * and {@link CallingConvention#JAVA java} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class VectorToJavaConvertlet extends JavaConvertlet {
    public VectorToJavaConvertlet() {
        super(CallingConvention.VECTOR,CallingConvention.JAVA);
    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        // Generate
        //   for (Enumeration e = <<exp>>.elements(); e.hasMoreElements();) {
        //     Row row = (Row) e.nextElement();
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_enum = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        OJClass rowType = OJUtil.typeToOJClass(converter.child.getRowType());
        stmtList.add(
            new ForStatement(
                TypeName.forOJClass(Util.clazzEnumeration),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_enum.toString(),
                        new MethodCall(exp,"elements",new ExpressionList()))
                },
                new MethodCall(variable_enum,"hasMoreElements",null),
                new ExpressionList(),
                forBody));
        Variable variable_row =
            implementor.bind(
                    converter,
                forBody,
                Util.castObject(
                    new MethodCall(variable_enum,"nextElement",null),
                    Toolbox.clazzObject,
                    rowType));
        Util.discard(variable_row);
        implementor.generateParentBody(converter,forBody);
        return null;
    }
}

// End VectorToJavaConvertlet.java
