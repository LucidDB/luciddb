/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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
package org.eigenbase.sql.validate;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;


/**
 * Namespace offered by a subquery.
 *
 * @author jhyde
 * @version $Id$
 * @see SelectScope
 * @see SetopNamespace
 * @since Mar 25, 2003
 */
public class SelectNamespace
    extends AbstractNamespace
{

    //~ Instance fields --------------------------------------------------------

    private final SqlSelect select;

    //~ Constructors -----------------------------------------------------------

    public SelectNamespace(SqlValidatorImpl validator, SqlSelect select)
    {
        super(validator);
        this.select = select;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getNode()
    {
        return select;
    }

    public RelDataType validateImpl()
    {
        validator.validateSelect(select, validator.unknownType);
        return rowType;
    }

    public void lookupHints(SqlParserPos pos, List<SqlMoniker> hintList)
    {
        validator.lookupSelectHints(select, pos, hintList);
    }
}

// End SelectNamespace.java
