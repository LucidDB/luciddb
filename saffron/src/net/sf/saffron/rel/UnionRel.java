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

package net.sf.saffron.rel;

import net.sf.saffron.core.PlanWriter;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.OptUtil;
import net.sf.saffron.opt.VolcanoCluster;


// TODO jvs 25-Sept-2003: Factor out base class SetOpRel and make IntersectRel
// and MinusRel extend that rather than UnionRel.

/**
 * <code>UnionRel</code> returns the union of the rows of its inputs,
 * optionally eliminating duplicates.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 23 September, 2001
 */
public class UnionRel extends SaffronBaseRel
{
    //~ Instance fields -------------------------------------------------------

    protected SaffronRel[] inputs;
    protected boolean all;

    //~ Constructors ----------------------------------------------------------

    public UnionRel(VolcanoCluster cluster,SaffronRel[] inputs,boolean all)
    {
        super(cluster);
        this.inputs = inputs;
        this.all = all;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isDistinct()
    {
        return !all;
    }

    public SaffronRel [] getInputs()
    {
        return inputs;
    }

    // implement SaffronRel
    public double getRows()
    {
        double dRows = 0;
        for (int i = 0; i < inputs.length; i++) {
            dRows += inputs[i].getRows();
        }
        if (!all) {
            dRows *= 0.5;
        }
        return dRows;
    }

    public Object clone()
    {
        return new UnionRel(cluster,OptUtil.clone(inputs),all);
    }

    public void explain(PlanWriter pw)
    {
        String [] terms = new String[inputs.length + 1];
        for (int i = 0; i < inputs.length; i++) {
            terms[i] = "input#" + i;
        }
        terms[inputs.length] = "all";
        pw.explain(this,terms,new Object [] { Boolean.valueOf(all) });
    }

    public void replaceInput(int ordinalInParent,SaffronRel p)
    {
        inputs[ordinalInParent] = p;
    }

    protected SaffronType deriveRowType()
    {
        SaffronType [] types = new SaffronType[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            types[i] = inputs[i].getRowType();
        }
        return cluster.typeFactory.leastRestrictive(types);
    }
}


// End UnionRel.java
