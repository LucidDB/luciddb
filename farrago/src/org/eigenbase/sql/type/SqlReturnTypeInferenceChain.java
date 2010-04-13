/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Strategy to infer the type of an operator call from the type of the operands
 * by using a series of {@link SqlReturnTypeInference} rules in a given order.
 * If a rule fails to find a return type (by returning NULL), next rule is tried
 * until there are no more rules in which case NULL will be returned.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlReturnTypeInferenceChain
    implements SqlReturnTypeInference
{
    //~ Instance fields --------------------------------------------------------

    private final SqlReturnTypeInference [] rules;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FallbackCascade from an array of rules
     *
     * @pre null!=rules
     * @pre null!=rules[i]
     * @pre rules.length > 0
     */
    public SqlReturnTypeInferenceChain(
        SqlReturnTypeInference [] rules)
    {
        Util.pre(null != rules, "null!=rules");
        Util.pre(rules.length > 0, "rules.length>0");
        for (int i = 0; i < rules.length; i++) {
            Util.pre(rules[i] != null, "transforms[i] != null");
        }
        this.rules = rules;
    }

    /**
     * Creates a FallbackCascade from two rules
     */
    public SqlReturnTypeInferenceChain(
        SqlReturnTypeInference rule1,
        SqlReturnTypeInference rule2)
    {
        this(new SqlReturnTypeInference[] { rule1, rule2 });
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        RelDataType ret = null;
        for (int i = 0; i < rules.length; i++) {
            SqlReturnTypeInference rule = rules[i];
            ret = rule.inferReturnType(opBinding);
            if (null != ret) {
                break;
            }
        }
        return ret;
    }
}

// End SqlReturnTypeInferenceChain.java
