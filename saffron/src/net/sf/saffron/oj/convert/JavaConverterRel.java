/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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

import net.sf.saffron.core.SaffronPlanner;
import net.sf.saffron.oj.rel.JavaRel;
import net.sf.saffron.oj.rel.JavaRelImplementor;
import net.sf.saffron.oj.rel.JavaLoopRel;
import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.convert.ConverterRel;
import net.sf.saffron.rel.convert.ConverterRule;
import net.sf.saffron.rel.convert.FactoryConverterRule;
import openjava.ptree.ParseTree;
import openjava.ptree.Variable;


/**
 * An <code>JavaConverterRel</code> converts a plan from
 * <code>inConvention</code> to {@link
 * net.sf.saffron.opt.CallingConvention#ARRAY}.
 */
public class JavaConverterRel extends ConverterRel
        implements JavaRel, JavaLoopRel
{
    //~ Instance fields -------------------------------------------------------

    /**
     * The convertlet actually does the work.
     */
    private final JavaConvertlet _convertlet;
    /** Scratch storage for some of the converlet implementations. Convertlets
     * can be used several times in the same plan, so they have to store temp
     * stuff here.
     */
    Variable var_v;

    //~ Constructors ----------------------------------------------------------

    JavaConverterRel(VolcanoCluster cluster,SaffronRel child,
            JavaConvertlet convertlet)
    {
        super(cluster,child);
        _convertlet = convertlet;
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getConvention()
    {
        return _convertlet.getConvention();
    }

    // implement SaffronRel
    public Object clone()
    {
        return new JavaConverterRel(cluster,child,_convertlet);
    }

    public ParseTree implement(JavaRelImplementor implementor) {
        return _convertlet.implement(implementor, this);
    }

    public void implementJavaParent(JavaRelImplementor implementor,
            int ordinal) {
        assert ordinal == 0; // converters have exactly 1 child
        _convertlet.implementJavaParent(implementor, this);
    }

}


// End JavaConverterRel.java
