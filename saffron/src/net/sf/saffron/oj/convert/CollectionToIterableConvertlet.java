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

import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.rel.convert.ConverterRel;
import openjava.ptree.ParseTree;

/**
 * Thunk to convert between {@link CallingConvention#COLLECTION collection}
 * and {@link CallingConvention#ITERABLE iterable} calling-conventions.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public class CollectionToIterableConvertlet extends JavaConvertlet {
    public CollectionToIterableConvertlet() {
        super(CallingConvention.COLLECTION, CallingConvention.ITERABLE);
    }

    public ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter) {
        // Generate
        //   <<exp>>
        //
        // (which is not of the same type, but it should work, because
        // they're just going to call 'iterator' on it)
        return implementor.visitJavaChild(converter, 0, (JavaRel) converter.child);
    }
}

// End CollectionToIterableConvertlet.java
