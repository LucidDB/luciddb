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

import java.io.*;

import org.eigenbase.rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.xom.*;


/**
 * Callback for a relational expression to dump in XML format.
 *
 * @testcase
 */
public class RelOptXmlPlanWriter
    extends RelOptPlanWriter
{
    //~ Instance fields --------------------------------------------------------

    private final XMLOutput xmlOutput;
    boolean generic = true;

    //~ Constructors -----------------------------------------------------------

    // TODO jvs 23-Dec-2005:  honor detail level.  The current inheritance
    // structure makes this difficult without duplication; need to factor
    // out the filtering of attributes before rendering.

    public RelOptXmlPlanWriter(PrintWriter pw, SqlExplainLevel detailLevel)
    {
        super(pw, detailLevel);
        xmlOutput = new XMLOutput(this);
        xmlOutput.setGlob(true);
        xmlOutput.setCompact(false);
    }

    //~ Methods ----------------------------------------------------------------

    public void explain(RelNode rel, String [] terms, Object [] values)
    {
        if (generic) {
            explainGeneric(rel, terms, values);
        } else {
            explainSpecific(rel, terms, values);
        }
    }

    /**
     * Generates generic XML (sometimes called 'element-oriented XML'). Like
     * this:
     *
     * <pre>
     * &lt;RelNode id="1" type="Join"&gt;
     *   &lt;Property name="condition"&gt;EMP.DEPTNO = DEPT.DEPTNO&lt;/Property&gt;
     *   &lt;Inputs&gt;
     *     &lt;RelNode id="2" type="Project"&gt;
     *       &lt;Property name="expr1"&gt;x + y&lt;/Property&gt;
     *       &lt;Property name="expr2"&gt;45&lt;/Property&gt;
     *     &lt;/RelNode&gt;
     *     &lt;RelNode id="3" type="TableAccess"&gt;
     *       &lt;Property name="table"&gt;SALES.EMP&lt;/Property&gt;
     *     &lt;/RelNode&gt;
     *   &lt;/Inputs&gt;
     * &lt;/RelNode&gt;
     * </pre>
     *
     * @param rel
     * @param terms
     * @param values
     */
    private void explainGeneric(RelNode rel,
        String [] terms,
        Object [] values)
    {
        RelNode [] inputs = rel.getInputs();
        RexNode [] children = rel.getChildExps();
        assert terms.length
            == (inputs.length + children.length
                + values.length) : "terms.length=" + terms.length
            + " inputs.length=" + inputs.length + " children.length="
            + children.length + " values.length=" + values.length;
        String relType = rel.getRelTypeName();
        xmlOutput.beginBeginTag("RelNode");
        xmlOutput.attribute("type", relType);

        //xmlOutput.attribute("id", rel.getId() + "");
        xmlOutput.endBeginTag("RelNode");

        int j = 0;
        for (int i = 0; i < children.length; i++) {
            RexNode child = children[i];
            xmlOutput.beginBeginTag("Property");
            xmlOutput.attribute("name", terms[inputs.length + j++]);
            xmlOutput.endBeginTag("Property");
            xmlOutput.cdata(child.toString());
            xmlOutput.endTag("Property");
        }
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            xmlOutput.beginBeginTag("Property");
            xmlOutput.attribute("name", terms[inputs.length + j++]);
            xmlOutput.endBeginTag("Property");
            xmlOutput.cdata(value.toString());
            xmlOutput.endTag("Property");
        }
        xmlOutput.beginTag("Inputs", null);
        level++;
        for (int i = 0; i < inputs.length; i++) {
            RelNode child = inputs[i];
            child.explain(this);
        }
        level--;
        xmlOutput.endTag("Inputs");
        xmlOutput.endTag("RelNode");
    }

    /**
     * Generates specific XML (sometimes called 'attribute-oriented XML'). Like
     * this:
     *
     * <pre>
     * &lt;Join condition="EMP.DEPTNO = DEPT.DEPTNO"&gt;
     *   &lt;Project expr1="x + y" expr2="42"&gt;
     *   &lt;TableAccess table="SALES.EMPS"&gt;
     * &lt;/Join&gt;
     * </pre>
     *
     * @param rel
     * @param terms
     * @param values
     */
    private void explainSpecific(RelNode rel,
        String [] terms,
        Object [] values)
    {
        RelNode [] inputs = rel.getInputs();
        RexNode [] children = rel.getChildExps();
        assert terms.length
            == (inputs.length + children.length
                + values.length) : "terms.length=" + terms.length
            + " inputs.length=" + inputs.length + " children.length="
            + children.length + " values.length=" + values.length;
        String tagName = rel.getRelTypeName();
        xmlOutput.beginBeginTag(tagName);
        xmlOutput.attribute("id", rel.getId() + "");

        int j = 0;
        for (int i = 0; i < children.length; i++) {
            RexNode child = children[i];
            xmlOutput.attribute(
                terms[inputs.length + j++],
                child.toString());
        }
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            xmlOutput.attribute(
                terms[inputs.length + j++],
                value.toString());
        }
        xmlOutput.endBeginTag(tagName);
        level++;
        for (int i = 0; i < inputs.length; i++) {
            RelNode child = inputs[i];
            child.explain(this);
        }
        level--;
    }
}

// End RelOptXmlPlanWriter.java
