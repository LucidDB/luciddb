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
import net.sf.saffron.rel.convert.ConverterRel;
import openjava.ptree.Expression;
import openjava.ptree.BinaryExpression;
import openjava.ptree.Literal;
import openjava.ptree.ParseTree;

/**
 * Thunk to convert between {@link CallingConvention#ARRAY array}
 * and {@link CallingConvention#EXISTS exists} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class ArrayToExistsConvertlet extends JavaConvertlet {
    public ArrayToExistsConvertlet() {
        super(CallingConvention.ARRAY,CallingConvention.EXISTS);

    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        Expression exp = implementor.visitJavaChild(converter, 0,
                (JavaRel) converter.child);
        return new BinaryExpression(
            exp,
            BinaryExpression.GREATER,
            Literal.constantZero());
    }
}

// End ArrayToExistsConvertlet.java
