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

import openjava.ptree.ParseTree;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterFactory;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.util.Util;


/**
 * Abstract class to convert from one {@link CallingConvention} to another. A
 * convertlet can be embedded in a {@link JavaConverterRel} to produce a
 * relational expression which can convert between arbitrary calling
 * conventions; JavaConverterRel provides the {@link RelNode} behavior, and
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
public abstract class JavaConvertlet implements ConverterFactory
{
    private final CallingConvention inConvention;
    private final CallingConvention convention;

    JavaConvertlet(
        CallingConvention inConvention,
        CallingConvention convention)
    {
        this.inConvention = inConvention;
        this.convention = convention;
    }

    public CallingConvention getInConvention()
    {
        return inConvention;
    }

    public ConverterRel convert(RelNode rel)
    {
        return new JavaConverterRel(
            rel.getCluster(),
            rel,
            this);
    }

    public CallingConvention getConvention()
    {
        return convention;
    }

    public abstract ParseTree implement(
        JavaRelImplementor implementor,
        ConverterRel converter);

    public void implementJavaParent(
        JavaRelImplementor implementor,
        ConverterRel converter)
    {
        throw Util.newInternal(getClass() + " cannot convert from "
            + inConvention + " calling convention");
    }
}


// End JavaConvertlet.java
