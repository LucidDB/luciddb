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

import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.util.Util;
import org.eigenbase.rel.convert.ConverterRel;
import openjava.ptree.*;
import openjava.mop.OJClass;
import openjava.mop.Toolbox;

/**
 * Thunk to convert between {@link CallingConvention#ITERABLE iterable}
 * and {@link CallingConvention#JAVA java} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class IterableToJavaConvertlet extends JavaConvertlet {
    public IterableToJavaConvertlet() {
        super(CallingConvention.ITERABLE,CallingConvention.JAVA);

    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        // Generate
        //   Iterator iter = <<exp>>.iterator();
        //   while (iter.hasNext()) {
        //     V row = (Type) iter.next();
        //     <<body>>
        //   }
        //
        StatementList stmtList = implementor.getStatementList();

        // Generate
        //   Iterator iter = <<exp>>.iterator();
        //   while (iter.hasNext()) {
        //     V row = (Type) iter.next();
        //     <<body>>
        //   }
        //
        StatementList whileBody = new StatementList();
        Variable variable_iter = implementor.newVariable();
        Expression exp = implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        stmtList.add(
            new VariableDeclaration(
                new TypeName("java.util.Iterator"),
                variable_iter.toString(),
                exp));
        stmtList.add(
            new WhileStatement(
                new MethodCall(variable_iter,"hasNext",null),
                whileBody));
        OJClass rowType = OJUtil.typeToOJClass(converter.child.getRowType());
        Variable variable_row =
            implementor.bind(
                    converter,
                whileBody,
                Util.castObject(
                    new MethodCall(variable_iter,"next",null),
                    Toolbox.clazzObject,
                    rowType));
        Util.discard(variable_row);
        implementor.generateParentBody(converter,whileBody);
        return null;
    }
}

// End IterableToJavaConvertlet.java
