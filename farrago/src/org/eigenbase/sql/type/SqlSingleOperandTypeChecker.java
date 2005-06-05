/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql.type;

import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;

/**
 * SqlSingleOperandTypeChecker is an extension of
 * {@link SqlOperandTypeChecker} for implementations which are cabable
 * of checking the type of a single operand in isolation.  This isn't
 * meaningful for all type-checking rules (e.g. SameOperandTypeChecker
 * requires two operands to have matching types, so checking one in
 * isolation is meaningless).
 *
 * @author Wael Chatila
 * @version $Id$
 */
public interface SqlSingleOperandTypeChecker extends SqlOperandTypeChecker
{
    /**
     * Checks the type of a single operand against a particular ordinal
     * position within a formal operator signature.  Note that the actual
     * ordinal position of the operand being checked may be
     * <em>different</em> from the position of the formal operand.
     * For example, when validating the actual call C(X, Y, Z), the strategy
     * for validating the operand Z might involve checking its type
     * against the formal signature OP(W).  In this case, iFormalOperand
     * would be zero, even though the position of Z within call C is two.
     *
     * @param call description of the call being checked; this is only provided
     * for context when throwing an exception; the implementation should
     * <em>NOT</em> examine the operands of the call as part of the check
     *
     * @param operand the actual operand to be checked
     *
     * @param iFormalOperand the 0-based formal operand ordinal
     *
     * @param throwOnFailure whether to throw an exception if check
     * fails (otherwise returns false in that case)
     *
     * @return whether check succeeded
     */
    public boolean checkSingleOperandType(
        SqlCallBinding callBinding,
        SqlNode operand,
        int iFormalOperand,
        boolean throwOnFailure);
}

// End SqlSingleOperandTypeChecker.java
