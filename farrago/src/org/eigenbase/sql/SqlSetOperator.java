/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

package org.eigenbase.sql;

import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInferenceImpl;

/**
 * SqlSetOperator represents a relational set theory operator
 * (UNION, INTERSECT, MINUS).  These are binary operators, but with
 * an extra boolean attribute tacked on for whether to remove duplicates
 * (e.g. UNION ALL does not remove duplicates).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlSetOperator extends SqlBinaryOperator
{
    //~ Instance fields -------------------------------------------------------

    public final boolean all;

    //~ Constructors ----------------------------------------------------------

    public SqlSetOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean all)
    {
        super(name, kind, prec, true,
            ReturnTypeInferenceImpl.useLeastRestrictive,
            null,
            OperandsTypeChecking.typeSetop);
        this.all = all;
    }

    public SqlSetOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean all,
        ReturnTypeInference typeInference,
        UnknownParamInference paramTypeInference,
        OperandsTypeChecking argTypes)
    {
        super(name, kind, prec, true, typeInference, paramTypeInference, argTypes);
        this.all = all;
    }

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlValidator.Scope operandScope)
    {
        validator.validateQuery(call);
    }
}


// End SqlSetOperator.java
