/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package org.eigenbase.rex;

import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;

/**
 * Visitor which checks the validity of a {@link RexNode} expression.
 *
 * <p>There are two modes of operation:<ul>
 *
 * <li>Use<code>fail=true</code> to throw an {@link AssertionError}
 *     as soon as an invalid node is detected:
 *     <blockquote><code>
 *      RexNode node;<br/>
 *      RelDataType rowType;<br/>
 *      assert new RexChecker(rowType, true).isValid(node);
 *     </code></blockquote>
 *
 *     This mode requires that assertions are enabled.</li>
 *
 * <li>Use <code>fail=false</code> to test for validity without throwing an
 *     error.
 *     <blockquote><code>
 *      RexNode node;<br/>
 *      RelDataType rowType;<br/>
 *      RexChecker checker = new RexChecker(rowType, false);<br/>
 *      node.accept(checker);<br/>
 *      if (!checker.valid) {<br/>
 *      &nbsp;&nbsp;&nbsp;...<br/>
 *      }</br>
 *     </code></blockquote></li>
 *
 * @see RexNode
 * @author jhyde
 * @since May 21, 2006
 * @version $Id$
 */
public class RexChecker extends RexVisitorImpl
{
    private final boolean fail;
    private final RelDataType inputRowType;
    private boolean valid;

    /**
     * Creates a RexChecker.
     *
     * <p>If <code>fail</code> is true, the checker will throw an
     * {@link AssertionError} if an invalid node is found and assertions are
     * enabled.
     *
     * @param inputRowType Input row type
     * @param fail Whether to throw an {@link AssertionError} if an invalid
     *    node is detected
     */
    public RexChecker(RelDataType inputRowType, boolean fail)
    {
        super(true);
        this.fail = fail;
        this.inputRowType = inputRowType;
    }

    public void visitInputRef(RexInputRef ref)
    {
        final RelDataTypeField[] fields = inputRowType.getFields();
        final int index = ref.getIndex();
        if (index < 0 || index >= fields.length) {
            assert !fail : "RexInputRef index " + index +
                " out of range 0.." + (fields.length - 1);
            valid = false;
            return;
        }
        if (!ref.getType().isStruct() &&
            !RelOptUtil.eq(
            "ref", ref.getType(),
            "input", fields[index].getType(), fail)) {
            valid = false;
            return;
        }
    }

    public void visitInputRef(RexLocalRef ref)
    {
        assert !fail : "RexLocalRef illegal outside program";
        valid = false;
    }

    public boolean isValid(RexNode expr)
    {
        valid = true;
        expr.accept(this);
        return valid;
    }
}

// End RexChecker.java
