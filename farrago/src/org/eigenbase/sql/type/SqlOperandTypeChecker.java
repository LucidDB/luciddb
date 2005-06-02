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
 * Strategy interface to check for allowed operand types of an operator call.
 *
 * <p>This interface is an example of the
 * {@link org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.</p>
 *
 * @author Wael Chatila
 * @version $Id$
 */
public interface SqlOperandTypeChecker
{
    /**
     * Checks if a node is of correct type.
     *
     * Note that <code>ruleOrdinal</code> is <emp>not</emp> an index in any
     * call.operands[] array. It's used to specify which
     * signature the node should correspond too.
     * <p>For example, if we have typeStringInt, a check can be made to see
     * if a <code>node</code> is of type int by calling.
     *
     * @param call
     * @param validator
     * @param scope
     * @param node
     * @param ruleOrdinal
     */
    public boolean check(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlNode node,
        int ruleOrdinal,
        boolean throwOnFailure);

    public boolean check(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call,
        boolean throwOnFailure);

    /**
     * @return the argument count.
     */
    public int getArgCount();

    /**
     * @return a string describing the expected argument types of a call, e.g.
     * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     */
    public String getAllowedSignatures(SqlOperator op);
}

// End SqlOperandTypeChecker.java
