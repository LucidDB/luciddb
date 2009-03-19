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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Strategy to infer the type of an operator call from the type of the operands
 * by using one {@link SqlReturnTypeInference} rule and a combination of {@link
 * SqlTypeTransform}s
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlTypeTransformCascade
    implements SqlReturnTypeInference
{
    //~ Instance fields --------------------------------------------------------

    private final SqlReturnTypeInference rule;
    private final SqlTypeTransform [] transforms;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a SqlTypeTransformCascade from a rule and an array of one or more
     * transforms.
     *
     * @pre null!=rule
     * @pre null!=transforms
     * @pre transforms.length > 0
     * @pre transforms[i] != null
     */
    public SqlTypeTransformCascade(
        SqlReturnTypeInference rule,
        SqlTypeTransform [] transforms)
    {
        Util.pre(null != rule, "null!=rule");
        Util.pre(null != transforms, "null!=transforms");
        Util.pre(transforms.length > 0, "transforms.length>0");
        for (int i = 0; i < transforms.length; i++) {
            Util.pre(transforms[i] != null, "transforms[i] != null");
        }
        this.rule = rule;
        this.transforms = transforms;
    }

    /**
     * Creates a SqlTypeTransformCascade from a rule and a single transform.
     *
     * @pre null!=rule
     * @pre null!=transform
     */
    public SqlTypeTransformCascade(
        SqlReturnTypeInference rule,
        SqlTypeTransform transform)
    {
        this(
            rule,
            new SqlTypeTransform[] { transform });
    }

    /**
     * Creates a SqlTypeTransformCascade from a rule and two transforms.
     *
     * @pre null!=rule
     * @pre null!=transform0
     * @pre null!=transform1
     */
    public SqlTypeTransformCascade(
        SqlReturnTypeInference rule,
        SqlTypeTransform transform0,
        SqlTypeTransform transform1)
    {
        this(
            rule,
            new SqlTypeTransform[] { transform0, transform1 });
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        RelDataType ret = rule.inferReturnType(opBinding);
        for (int i = 0; i < transforms.length; i++) {
            SqlTypeTransform transform = transforms[i];
            ret = transform.transformType(opBinding, ret);
        }
        return ret;
    }
}

// End SqlTypeTransformCascade.java
