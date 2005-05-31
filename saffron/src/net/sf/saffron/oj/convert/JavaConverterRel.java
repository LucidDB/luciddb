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
import openjava.ptree.Variable;

import org.eigenbase.oj.rel.JavaLoopRel;
import org.eigenbase.oj.rel.JavaRel;
import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.convert.ConverterRel;
import org.eigenbase.rel.convert.ConverterRule;
import org.eigenbase.rel.convert.FactoryConverterRule;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanWriter;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.util.Util;


/**
 * An <code>JavaConverterRel</code> converts a plan from
 * <code>inConvention</code> to {@link
 * org.eigenbase.relopt.CallingConvention#ARRAY}.
 */
public class JavaConverterRel extends ConverterRel implements JavaRel,
    JavaLoopRel
{
    /**
     * The convertlet actually does the work.
     */
    private final JavaConvertlet convertlet;

    /** Scratch storage for some of the converlet implementations. Convertlets
     * can be used several times in the same plan, so they have to store temp
     * stuff here.
     */
    Variable var_v;

    JavaConverterRel(
        RelOptCluster cluster,
        RelNode child,
        JavaConvertlet convertlet)
    {
        super(
            cluster, convertlet.getConvention().getTraitDef(),
            new RelTraitSet(convertlet.getConvention()), child);
        this.convertlet = convertlet;
    }

    private static final String [] terms = { "child", "convention" };

    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            terms,
            new Object [] { getConvention().getName() });
    }

    // implement RelNode
    public Object clone()
    {
        JavaConverterRel clone =
            new JavaConverterRel(cluster, child, convertlet);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public ParseTree implement(JavaRelImplementor implementor)
    {
        return convertlet.implement(implementor, this);
    }

    public void implementJavaParent(
        JavaRelImplementor implementor,
        int ordinal)
    {
        assert ordinal == 0; // converters have exactly 1 child
        convertlet.implementJavaParent(implementor, this);
    }
}


// End JavaConverterRel.java
