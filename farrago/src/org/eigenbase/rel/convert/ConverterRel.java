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

package org.eigenbase.rel.convert;

import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.util.Util;

// REVIEW jvs 23-April-2004:  This should really be an interface
// (with a companion abstract class), providing more flexibility in
// multiple inheritance situations.

/**
 * Converts a relational expression from one {@link CallingConvention calling
 * convention} to another.
 * 
 * <p>
 * Sometimes this conversion is expensive; for example, to convert a
 * non-distinct to a distinct object stream, we have to clone every object in
 * the input.
 * </p>
 */
public abstract class ConverterRel extends SingleRel
{
    //~ Instance fields -------------------------------------------------------

    protected CallingConvention _inConvention;

    //~ Constructors ----------------------------------------------------------

    protected ConverterRel(RelOptCluster cluster,RelNode child)
    {
        super(cluster,child);
        this._inConvention = child.getConvention();
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = child.getRows();
        double dCpu = child.getRows();
        double dIo = 0;
        return planner.makeCost(dRows,dCpu,dIo);
    }

    protected Error cannotImplement()
    {
        return Util.newInternal(
            getClass() + " cannot convert from " + _inConvention
            + " calling convention");
    }

    protected CallingConvention getInputConvention()
    {
        return _inConvention;
    }
}


// End ConverterRel.java
