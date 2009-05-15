/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * Visitor which checks the validity of a {@link RexNode} expression.
 *
 * <p>There are two modes of operation:
 *
 * <ul>
 * <li>Use<code>fail=true</code> to throw an {@link AssertionError} as soon as
 * an invalid node is detected:
 *
 * <blockquote><code>RexNode node;<br/>
 * RelDataType rowType;<br/>
 * assert new RexChecker(rowType, true).isValid(node);</code></blockquote>
 *
 * This mode requires that assertions are enabled.</li>
 * <li>Use <code>fail=false</code> to test for validity without throwing an
 * error.
 *
 * <blockquote><code>RexNode node;<br/>
 * RelDataType rowType;<br/>
 * RexChecker checker = new RexChecker(rowType, false);<br/>
 * node.accept(checker);<br/>
 * if (!checker.valid) {<br/>
 * &nbsp;&nbsp;&nbsp;...<br/>
 * }</br></code></blockquote>
 * </li>
 *
 * @author jhyde
 * @version $Id$
 * @see RexNode
 * @since May 21, 2006
 */
public class RexChecker
    extends RexVisitorImpl<Boolean>
{
    //~ Instance fields --------------------------------------------------------

    protected final boolean fail;
    protected final List<RelDataType> inputTypeList;
    protected int failCount;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RexChecker with a given input row type.
     *
     * <p>If <code>fail</code> is true, the checker will throw an {@link
     * AssertionError} if an invalid node is found and assertions are enabled.
     *
     * <p>Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputRowType Input row type
     * @param fail Whether to throw an {@link AssertionError} if an invalid node
     * is detected
     */
    public RexChecker(final RelDataType inputRowType, boolean fail)
    {
        this(
            new AbstractList<RelDataType>() {
                public RelDataType get(int index)
                {
                    return inputRowType.getFieldList().get(index).getType();
                }

                public int size()
                {
                    return inputRowType.getFieldCount();
                }
            },
            fail);
    }

    /**
     * Creates a RexChecker with a given set of input fields.
     *
     * <p>If <code>fail</code> is true, the checker will throw an {@link
     * AssertionError} if an invalid node is found and assertions are enabled.
     *
     * <p>Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputTypeList Input row type
     * @param fail Whether to throw an {@link AssertionError} if an invalid node
     * is detected
     */
    public RexChecker(List<RelDataType> inputTypeList, boolean fail)
    {
        super(true);
        this.inputTypeList = inputTypeList;
        this.fail = fail;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the number of failures encountered.
     *
     * @return Number of failures
     */
    public int getFailureCount()
    {
        return failCount;
    }

    public Boolean visitInputRef(RexInputRef ref)
    {
        final int index = ref.getIndex();
        if ((index < 0) || (index >= inputTypeList.size())) {
            assert !fail
                : "RexInputRef index " + index
                + " out of range 0.." + (inputTypeList.size() - 1);
            ++failCount;
            return false;
        }
        if (!ref.getType().isStruct()
            && !RelOptUtil.eq(
                "ref",
                ref.getType(),
                "input",
                inputTypeList.get(index),
                fail))
        {
            assert !fail;
            ++failCount;
            return false;
        }
        return true;
    }

    public Boolean visitLocalRef(RexLocalRef ref)
    {
        assert !fail : "RexLocalRef illegal outside program";
        ++failCount;
        return false;
    }

    public Boolean visitCall(RexCall call)
    {
        for (RexNode operand : call.getOperands()) {
            Boolean valid = operand.accept(this);
            if (valid != null && !valid) {
                return false;
            }
        }
        return true;
    }

    public Boolean visitFieldAccess(RexFieldAccess fieldAccess)
    {
        super.visitFieldAccess(fieldAccess);
        final RelDataType refType = fieldAccess.getReferenceExpr().getType();
        assert refType.isStruct();
        final RelDataTypeField field = fieldAccess.getField();
        final int index = field.getIndex();
        if ((index < 0) || (index > refType.getFields().length)) {
            assert !fail;
            ++failCount;
            return false;
        }
        final RelDataTypeField typeField = refType.getFields()[index];
        if (!RelOptUtil.eq(
                "type1",
                typeField.getType(),
                "type2",
                fieldAccess.getType(),
                fail))
        {
            assert !fail;
            ++failCount;
            return false;
        }
        return true;
    }

    /**
     * Returns whether an expression is valid.
     */
    public final boolean isValid(RexNode expr)
    {
        return expr.accept(this);
    }
}

// End RexChecker.java
