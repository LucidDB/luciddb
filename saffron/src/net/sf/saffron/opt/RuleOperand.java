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

package net.sf.saffron.opt;

import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.util.Util;
import net.sf.saffron.util.Walkable;


/**
 * A <code>RuleOperand</code> determines whether a {@link
 * net.sf.saffron.opt.VolcanoRule} can be applied to a particular expression.
 * 
 * <p>
 * For example, the rule to pull a filter up from the left side of a join
 * takes operands: <code>(Join (Filter) (Any))</code>.
 * </p>
 * 
 * <p>
 * Note that <code>children</code> means different things if it is empty or it
 * is <code>null</code>: <code>(Join (Filter <b>()</b>) (Any))</code> means
 * that, to match the rule, <code>Filter</code> must have no operands.
 * </p>
 */
public class RuleOperand implements Walkable
{
    //~ Static fields/initializers --------------------------------------------

    static final RuleOperand [] noOperands = new RuleOperand[0];

    //~ Instance fields -------------------------------------------------------

    RuleOperand parent;
    VolcanoRule rule;
    int [] solveOrder;
    int ordinalInParent;
    int ordinalInRule;
    private final CallingConvention convention;
    private final Class clazz;
    private final RuleOperand [] children;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates an operand which matches any {@link CallingConvention}.
     *
     * @pre clazz != null
     */
    public RuleOperand(Class clazz,RuleOperand [] children)
    {
        this(clazz,null,children);
    }

    /**
     * Creates an operand which matches any {@link CallingConvention}.
     *
     * @pre clazz != null
     */
    public RuleOperand(
        Class clazz,
        CallingConvention convention,
        RuleOperand [] children)
    {
        assert(clazz != null);
        this.clazz = clazz;
        this.convention = convention;
        this.children = children;
        if (children != null) {
            for (int i = 0; i < this.children.length; i++) {
                this.children[i].parent = this;
            }
        }
    }

    //~ Methods ---------------------------------------------------------------

    // implement Walkable
    public Object [] getChildren()
    {
        return children;
    }

    /**
     * Returns whether a relational expression matches this operand. It must
     * be of the right class and calling convention.
     */
    public boolean matches(SaffronRel rel)
    {
        if (!clazz.isInstance(rel)) {
            return false;
        }
        if ((convention != null) && (rel.getConvention() != convention)) {
            return false;
        }
        return true;
    }
}


// End RuleOperand.java
