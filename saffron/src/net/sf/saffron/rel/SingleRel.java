/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
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

import net.sf.saffron.core.*;
import net.sf.saffron.opt.VolcanoCluster;
import net.sf.saffron.util.Util;


/**
 * A <code>SingleRel</code> is a base class single-input relational
 * expressions.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 23 September, 2001
 */
public abstract class SingleRel extends SaffronRel
{
    //~ Instance fields -------------------------------------------------------

    public SaffronRel child;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster {@link VolcanoCluster} this relational expression
     *        belongs to
     * @param child input relational expression
     */
    protected SingleRel(VolcanoCluster cluster,SaffronRel child)
    {
        super(cluster);
        this.child = child;
    }

    //~ Methods ---------------------------------------------------------------

    // implement SaffronRel
    public SaffronRel [] getInputs()
    {
        return new SaffronRel [] { child };
    }

    public double getRows()
    {
        // Not necessarily correct, but a better default than Rel's 1.0
        return child.getRows();
    }

    public void childrenAccept(RelVisitor visitor)
    {
        visitor.visit(child,0,this);
    }

    public void explain(PlanWriter pw)
    {
        pw.explain(this,new String [] { "child" },Util.emptyObjectArray);
    }

    // override Rel
    public void replaceInput(int ordinalInParent,SaffronRel rel)
    {
        assert(ordinalInParent == 0);
        this.child = rel;
    }

    protected SaffronType deriveRowType()
    {
        return child.getRowType();
    }
}


// End SingleRel.java
