/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
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
package org.eigenbase.sql.validate;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * An implementation of {@link SqlMoniker} that encapsulates the normalized name
 * information of a {@link SqlIdentifier}.
 *
 * @author tleung
 * @version $Id$
 * @since May 24, 2005
 */
public class SqlIdentifierMoniker
    implements SqlMoniker
{
    //~ Instance fields --------------------------------------------------------

    private final SqlIdentifier id;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an SqlIdentifierMoniker.
     */
    public SqlIdentifierMoniker(SqlIdentifier id)
    {
        Util.pre(id != null, "id != null");
        this.id = id;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlMonikerType getType()
    {
        return SqlMonikerType.Column;
    }

    public String [] getFullyQualifiedNames()
    {
        return id.names;
    }

    public SqlIdentifier toIdentifier()
    {
        return id;
    }

    public String toString()
    {
        return id.toString();
    }

    public String id()
    {
        return id.toString();
    }
}

// End SqlIdentifierMoniker.java
