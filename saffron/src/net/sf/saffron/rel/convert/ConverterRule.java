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

package net.sf.saffron.rel.convert;

import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;


/**
 * Abstract base class for a rule which converts from one calling convention
 * to another without changing semantics.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 5, 2003
 */
public abstract class ConverterRule extends VolcanoRule
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
        super(
            new RuleOperand(clazz,in,null) {
                public boolean matches(SaffronRel rel)
                {
                    // Don't apply converters to converters -- otherwise we get
                    // an n^2 effect.
                    if (rel instanceof ConverterRel) {
                        return false;
                    }
                    return super.matches(rel);
                }
            });
        assert(in != null);
        assert(out != null);
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

    public abstract SaffronRel convert(SaffronRel rel);

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

    public void onMatch(VolcanoRuleCall call)
    {
        SaffronRel rel = call.rels[0];
        if (rel.getConvention() == inConvention) {
            final SaffronRel converted = convert(rel);
            if (converted != null) {
                call.transformTo(converted);
            }
        }
    }
}


// End ConverterRule.java
