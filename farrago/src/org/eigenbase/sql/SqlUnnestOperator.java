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
package org.eigenbase.sql;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.resource.*;

import java.util.*;

/**
 * The <code>UNNEST<code> operator.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlUnnestOperator extends SqlFunctionalOperator
{
    public SqlUnnestOperator()
    {
        super(
            "UNNEST", SqlKind.Unnest,
            100, true, null, null,
            SqlTypeStrategies.otcMultisetOrRecordTypeMultiset);
    }

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        RelDataType type = opBinding.getOperandType(0);
        if (type.isStruct()) {
            type = type.getFields()[0].getType();
        }
        MultisetSqlType t = (MultisetSqlType) type;
        return t.getComponentType();
    }
}

// End SqlUnnestOperator.java
