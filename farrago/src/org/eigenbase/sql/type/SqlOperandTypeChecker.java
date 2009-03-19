/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
 * <p>This interface is an example of the {@link
 * org.eigenbase.util.Glossary#StrategyPattern strategy pattern}.</p>
 *
 * @author Wael Chatila
 * @version $Id$
 */
public interface SqlOperandTypeChecker
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Checks the types of all operands to an operator call.
     *
     * @param callBinding description of the call to be checked
     * @param throwOnFailure whether to throw an exception if check fails
     * (otherwise returns false in that case)
     *
     * @return whether check succeeded
     */
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure);

    /**
     * @return range of operand counts allowed in a call
     */
    public SqlOperandCountRange getOperandCountRange();

    /**
     * Returns a string describing the allowed formal signatures of a call, e.g.
     * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     *
     * @param op the operator being checked
     * @param opName name to use for the operator in case of aliasing
     *
     * @return generated string
     */
    public String getAllowedSignatures(SqlOperator op, String opName);
}

// End SqlOperandTypeChecker.java
