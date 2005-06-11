/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

package org.eigenbase.relopt;

import java.util.HashSet;

import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Util;


/**
 * Callback for an expression to dump itself to.
 */
public class RelOptPlanWriter extends java.io.PrintWriter
{
    //~ Instance fields -------------------------------------------------------

    private boolean withIdPrefix = true;

    /** Recursion detection. */
    HashSet active = new HashSet();
    boolean brief;
    int level;

    //~ Constructors ----------------------------------------------------------

    public RelOptPlanWriter(java.io.PrintWriter pw)
    {
        this(pw, false);
    }

    public RelOptPlanWriter(
        java.io.PrintWriter pw,
        boolean brief)
    {
        super(pw);
        this.level = 0;
        this.brief = brief;
    }

    //~ Methods ---------------------------------------------------------------

    public void setIdPrefix(boolean b)
    {
        withIdPrefix = b;
    }
    
    /**
     * @pre rel != null
     * @pre terms.length == rel.getChildExps().length + values.length
     * @pre values != null
     */
    public void explain(
        RelNode rel,
        String [] terms,
        Object [] values)
    {
        RelNode [] inputs = rel.getInputs();
        RexNode [] children = rel.getChildExps();
        assert terms.length == (inputs.length + children.length
            + values.length) : "terms.length=" + terms.length
        + " inputs.length=" + inputs.length + " children.length="
        + children.length + " values.length=" + values.length;
        String s;
        if (withIdPrefix) {
            s = rel.getId() + ":";
        } else {
            s = "";
        }
        s = s + rel.getRelTypeName() + ((AbstractRelNode) rel).getQualifier();
        if (brief) {
            explainBrief(s, rel, terms);
            return;
        }

        for (int i = 0; i < level; i++) {
            print("  ");
        }
        print(s);
        int j = 0;
        for (int i = 0; i < children.length; i++) {
            RexNode child = children[i];
            print(((j == 0) ? "(" : ", ") + terms[inputs.length + j++] + "=["
                + child.toString() + "]");
        }
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            print(((j == 0) ? "(" : ", ") + terms[inputs.length + j++] + "=["
                + value.toString() + "]");
        }
        println((j > 0) ? ")" : "");
        level++;
        for (int i = 0; i < inputs.length; i++) {
            RelNode child = inputs[i];
            child.explain(this);
        }
        level--;
    }

    public void explain(
        RelNode rel,
        String [] terms)
    {
        explain(rel, terms, Util.emptyStringArray);
    }

    /**
     * Special form used by {@link com.disruptivetech.farrago.volcano.RelSubset}.
     */
    public void explainSubset(
        String s,
        RelNode child)
    {
        print(s);
        level++;
        child.explain(this);
        level--;
    }

    public void explainTree(RelNode exp)
    {
        this.level = 0;
        exp.explain(this);
    }

    private void explainBrief(
        String s,
        RelNode rel,
        String [] terms)
    {
        print(s);
        if (active.add(rel)) {
            RelNode [] inputs = rel.getInputs();
            RexNode [] children = rel.getChildExps();
            if ((inputs.length > 0) || (children.length > 0)) {
                level++;
                print("(");
                int j = 0;
                for (int i = 0; i < inputs.length; i++) {
                    if (j++ > 0) {
                        print(", ");
                    }
                    RelNode input = inputs[i];
                    if (active.add(input)) {
                        input.explain(this);
                        active.remove(input);
                    } else {
                        print(input.getId());
                    }
                }
                for (int i = 0; i < children.length; i++) {
                    if (j++ > 0) {
                        print(", ");
                    }
                    RexNode child = children[i];
                    print(child.toString());
                }
                print(")");
                level--;
                active.remove(rel);
            }
        } else {
            print("Rel#" + rel.toString());
        }
    }
}


// End RelOptPlanWriter.java
