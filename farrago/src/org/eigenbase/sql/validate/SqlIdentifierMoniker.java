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

import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.util.Util;

/**
 * An implementation of {@link SqlMoniker} that encapsulates the normalized name
 * information of a {@link SqlIdentifier}.
 *
 * @author tleung
 * @since May 24, 2005
 * @version $Id$
 **/
public class SqlIdentifierMoniker implements SqlMoniker
{   
    private final SqlIdentifier id;

    /**
     * Creates an SqlIdentifierMoniker.
     */
    public SqlIdentifierMoniker(SqlIdentifier id)
    {
        Util.pre(id != null, "id != null");
        this.id = id;
    }

    public SqlMonikerType getType()
    {
        return SqlMonikerType.Column;
    }

    public String[] getFullyQualifiedNames()
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
}

// End SqlIdentifierMoniker.java
