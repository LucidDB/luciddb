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

import openjava.ptree.Expression;
import openjava.ptree.MethodCall;
import openjava.ptree.ParseTree;

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.relopt.CallingConvention;


/**
 * Thunk to convert between {@link CallingConvention#COLLECTION collection}
 * and {@link CallingConvention#ITERATOR iterator} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class CollectionToIteratorConvertlet extends JavaConvertlet
{
    public CollectionToIteratorConvertlet()
    {
        super(CallingConvention.COLLECTION, CallingConvention.ITERATOR);
    }

    public ParseTree implement(
        JavaRelImplementor implementor,
        ConverterRel converter)
    {
        // Generate
        //   <<exp>>.iterator()
        Expression exp =
            implementor.visitJavaChild(
                converter, 0, (JavaRel) converter.getChild());
        return new MethodCall(exp, "iterator", null);
    }
}


// End CollectionToIteratorConvertlet.java
