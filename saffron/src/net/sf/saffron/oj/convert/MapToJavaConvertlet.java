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
import openjava.mop.Toolbox;
import openjava.mop.OJClass;

/**
 * Thunk to convert between {@link CallingConvention#MAP map}
 * and {@link CallingConvention#JAVA java} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class MapToJavaConvertlet extends JavaConvertlet {
    public MapToJavaConvertlet() {
        super(CallingConvention.MAP,CallingConvention.JAVA);
    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        // Generate
        //   Map m = <<exp>>;
        //   for (Iterator entries = m.entrySet().iterator();
        //        entries.hasNext();) {
        //     Map.Entry entry = (Map.Entry) keys.next();
        //     Row row = new Row(entry.getKey(), entry.getValue());
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_m = implementor.newVariable();
        Variable variable_entries = implementor.newVariable();
        Variable variable_entry = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp = implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        stmtList.add(
            new VariableDeclaration(
                TypeName.forOJClass(Util.clazzMap),
                variable_m.toString(),
                exp));
        stmtList.add(
            new ForStatement(
                TypeName.forOJClass(Util.clazzIterator),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_entries.toString(),
                        new MethodCall(
                            new MethodCall(variable_m,"entrySet",null),
                            "iterator",
                            null))
                },
                new MethodCall(variable_entries,"hasNext",null),
                new ExpressionList(),
                forBody));
        forBody.add(
            new VariableDeclaration(
                TypeName.forOJClass(Toolbox.clazzMapEntry),
                new VariableDeclarator(
                    variable_entry.toString(),
                    new CastExpression(
                        TypeName.forOJClass(Toolbox.clazzMapEntry),
                        new MethodCall(variable_entries,"next",null)))));
        OJClass rowType = OJUtil.typeToOJClass(converter.getRowType());
        Variable variable_row =
            implementor.bind(
                    converter,
                forBody,
                new AllocationExpression(
                    TypeName.forOJClass(rowType),
                    new ExpressionList(
                        new MethodCall(variable_entry,"getKey",null),
                        new MethodCall(variable_entry,"getValue",null))));
        Util.discard(variable_row);
        implementor.generateParentBody(converter,forBody);
        return null;
    }
}

// End MapToJavaConvertlet.java
