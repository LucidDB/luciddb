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

import net.sf.saffron.rel.convert.ConverterFactory;
import net.sf.saffron.rel.convert.ConverterRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.oj.rel.JavaRelImplementor;
import net.sf.saffron.util.Util;
import openjava.ptree.ParseTree;

/**
 * Abstract class to convert from one {@link CallingConvention} to another. A
 * convertlet can be embedded in a {@link JavaConverterRel} to produce a
 * relational expression which can convert between arbitrary calling
 * conventions; JavaConverterRel provides the {@link SaffronRel} behavior, and
 * the convertlet provides the conversion logic.
 *
 * <p>The concrete derived class ('converlet') must specify the source and
 * target calling-conventions, and implement the {@link #implement} method. If
 * the target calling-convention is Java, it must also override {@link
 * #implementJavaParent}.
 *
 * @author jhyde
 * @since May 27, 2004
 * @version $Id$
 **/
public abstract class JavaConvertlet implements ConverterFactory {
    private final CallingConvention _inConvention;
    private final CallingConvention _convention;

    JavaConvertlet(CallingConvention inConvention, CallingConvention convention) {
        _inConvention = inConvention;
        _convention = convention;
    }

    public CallingConvention getInConvention() {
        return _inConvention;
    }

    public ConverterRel convert(SaffronRel rel)
    {
        return new JavaConverterRel(rel.getCluster(),rel,this);
    }

    public CallingConvention getConvention()
    {
        return _convention;
    }

    public abstract ParseTree implement(JavaRelImplementor implementor,
            ConverterRel converter);

    public void implementJavaParent(JavaRelImplementor implementor,
            ConverterRel converter) {
        throw Util.newInternal(
            getClass() + " cannot convert from " + _inConvention
            + " calling convention");
    }
}

// End JavaConvertlet.java
