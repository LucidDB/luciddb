/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
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

package org.eigenbase.rel.convert;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.util.Util;


/**
 * Abstract base class for a rule which converts from one calling convention
 * to another without changing semantics.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 5, 2003
 */
public abstract class ConverterRule extends RelOptRule
{
    //~ Instance fields -------------------------------------------------------

    public final CallingConvention inConvention;
    public final CallingConvention outConvention;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a <code>ConverterRule</code>
     *
     * @pre in != null
     * @pre out != null
     */
    public ConverterRule(
        Class clazz,
        CallingConvention in,
        CallingConvention out,
        String description)
    {
        super(new RelOptRuleOperand(clazz, in, null) {
                public boolean matches(RelNode rel)
                {
                    // Don't apply converters to converters -- otherwise we get
                    // an n^2 effect.
                    if (rel instanceof ConverterRel) {
                        return false;
                    }
                    return super.matches(rel);
                }
            });
        assert (in != null);
        assert (out != null);
        this.inConvention = in;
        this.outConvention = out;
        if (description == null) {
            description = "ConverterRule<in=" + in + ",out=" + out + ">";
        }
        this.description = description;
    }

    //~ Methods ---------------------------------------------------------------

    public CallingConvention getOutConvention()
    {
        return outConvention;
    }

    public abstract RelNode convert(RelNode rel);

    /**
     * Returns true if this rule can convert <em>any</em> relational
     * expression of the input convention.
     *
     * <p>
     * The union-to-java converter, for example, is not guaranteed, because it
     * only works on unions.
     * </p>
     */
    public boolean isGuaranteed()
    {
        return false;
    }

    public void onMatch(RelOptRuleCall call)
    {
        RelNode rel = call.rels[0];
        if (rel.getConvention() == inConvention) {
            final RelNode converted = convert(rel);
            if (converted != null) {
                call.transformTo(converted);
            }
        }
    }
}


// End ConverterRule.java
