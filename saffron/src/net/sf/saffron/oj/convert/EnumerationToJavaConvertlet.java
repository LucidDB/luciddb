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
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.util.Util;


/**
 * Thunk to convert between {@link CallingConvention#ENUMERATION}
 * and {@link CallingConvention#JAVA java} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class EnumerationToJavaConvertlet extends JavaConvertlet
{
    public EnumerationToJavaConvertlet()
    {
        super(CallingConvention.ENUMERATION, CallingConvention.JAVA);
    }

    public ParseTree implement(
        JavaRelImplementor implementor,
        ConverterRel converter)
    {
        // Generate
        //   for (Enumeration enum = <<exp>>; enum.hasMoreElements();)
        //   {
        //     Row row = enum.nextElement();
        //     <<parent>>
        //   }
        StatementList stmtList = implementor.getStatementList();
        Variable variable_enum = implementor.newVariable();
        StatementList forBody = new StatementList();
        Expression exp =
            implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
        stmtList.add(
            new ForStatement(
                TypeName.forOJClass(Util.clazzEnumeration),
                new VariableDeclarator [] {
                    new VariableDeclarator(
                        variable_enum.toString(),
                        exp)
                },
                new MethodCall(variable_enum, "hasMoreElements", null),
                new ExpressionList(),
                forBody));
        Variable variable_row =
            implementor.bind(
                converter,
                forBody,
                new MethodCall(variable_enum, "nextElement", null));
        Util.discard(variable_row);
        implementor.generateParentBody(converter, forBody);
        return null;
    }
}


// End EnumerationToJavaConvertlet.java
