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

/**
 * Thunk to convert between {@link CallingConvention#ARRAY array}
 * and {@link CallingConvention#JAVA java} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class ArrayToJavaConvertlet extends JavaConvertlet {
    public ArrayToJavaConvertlet() {
        super(CallingConvention.ARRAY,CallingConvention.JAVA);
    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        // Generate
        //   V[] array = <<exp>>;
        //   for (int i = 0; i < array.length; i++) {
        //     V row = array[i];
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_array = implementor.newVariable();
        Variable variable_i = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(OJUtil.ojClassForExpression(converter,exp)),
                variable_array.toString(),
                exp));
        stmtList.add(
            new ForStatement(
                new TypeName("int"),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_i.toString(),
                        Literal.constantZero())
                },
                new BinaryExpression(
                    variable_i,
                    BinaryExpression.LESS,
                    new FieldAccess(variable_array,"length")),
                new ExpressionList(
                    new UnaryExpression(
                        UnaryExpression.POST_INCREMENT,
                        variable_i)),
                forBody));
        Variable variable_row =
            implementor.bind(
                    converter,
                forBody,
                new ArrayAccess(variable_array,variable_i));
        Util.discard(variable_row);
        implementor.generateParentBody(converter,forBody);
        return null;
    }
}

// End ArrayToJavaConvertlet.java
