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

package net.sf.saffron.core;

import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.SaffronBaseRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.util.Util;

import java.util.HashSet;


/**
 * Callback for an expression to dump itself to.
 */
public class PlanWriter extends java.io.PrintWriter
{
    //~ Instance fields -------------------------------------------------------

    public boolean withIdPrefix = true;

    /** Recursion detection. */
    HashSet active = new HashSet();
    boolean brief;
    int level;

    //~ Constructors ----------------------------------------------------------

    public PlanWriter(java.io.PrintWriter pw)
    {
        this(pw,false);
    }

    public PlanWriter(java.io.PrintWriter pw,boolean brief)
    {
        super(pw);
        this.level = 0;
        this.brief = brief;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @pre rel != null
     * @pre terms.length == rel.getChildExps().length + values.length
     * @pre values != null
     */
    public void explain(SaffronRel rel,String [] terms,Object [] values)
    {
        SaffronRel [] inputs = rel.getInputs();
        RexNode [] children = rel.getChildExps();
        assert terms.length == inputs.length + children.length + values.length :
                "terms.length=" + terms.length +
                " inputs.length=" + inputs.length +
                " children.length=" + children.length +
                " values.length=" + values.length;
        String s;
        if (withIdPrefix) {
            s = rel.getId() + ":";
        } else {
            s = "";
        }
        s = s + rel.getRelTypeName() +
                ((SaffronBaseRel) rel).getQualifier();
        if (brief) {
            explainBrief(s,rel,terms);
            return;
        }

        for (int i = 0; i < level; i++) {
            print("  ");
        }
        print(s);
        int j = 0;
        for (int i = 0; i < children.length; i++) {
            RexNode child = children[i];
            print(
                ((j == 0) ? "(" : ", ") + terms[inputs.length + j++] + "=["
                + child.toString() + "]");
        }
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            print(
                ((j == 0) ? "(" : ", ") + terms[inputs.length + j++] + "=["
                + value.toString() + "]");
        }
        println((j > 0) ? ")" : "");
        level++;
        for (int i = 0; i < inputs.length; i++) {
            SaffronRel child = inputs[i];
            child.explain(this);
        }
        level--;
    }

    public void explain(SaffronRel rel,String [] terms)
    {
        explain(rel,terms,Util.emptyStringArray);
    }

    /**
     * Special form used by {@link net.sf.saffron.opt.RelSubset}.
     */
    public void explainSubset(String s,SaffronRel child)
    {
        print(s);
        level++;
        child.explain(this);
        level--;
    }

    //  	public void explainTree(Plan plan)
    //  	{
    //  		this.level = 0;
    //  		plan.explain(this);
    //  	}
    public void explainTree(SaffronRel exp)
    {
        this.level = 0;
        exp.explain(this);
    }

    private void explainBrief(String s,SaffronRel rel,String [] terms)
    {
        print(s);
        if (active.add(rel)) {
            SaffronRel [] inputs = rel.getInputs();
            RexNode [] children = rel.getChildExps();
            if ((inputs.length > 0) || (children.length > 0)) {
                level++;
                print("(");
                int j = 0;
                for (int i = 0; i < inputs.length; i++) {
                    if (j++ > 0) {
                        print(", ");
                    }
                    SaffronRel input = inputs[i];
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


// End PlanWriter.java
