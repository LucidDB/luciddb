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
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.Util;

/**
 * A generic implementation of {@link Moniker}.
 *
 * @author tleung
 * @since May 31, 2005
 * @version $Id$
 **/
public class MonikerImpl implements Moniker
{   
    private final String[] names;
    private final MonikerType type;

    /**
     * Creates a moniker with an array of names.
     */
    public MonikerImpl(String[] names, MonikerType type)
    {
        Util.pre(names != null, "names != null");
        Util.pre(type != null, "type != null");
        for (int i = 0; i < names.length; i++) {
            Util.pre(names[i] != null, "names[i] != null");
        }
        this.names = names;
        this.type = type;
    }

    /**
     * Creates a moniker with a single name.
     */
    public MonikerImpl(String name, MonikerType type)
    {
        this(new String[] {name}, type);
    }

    public MonikerType getType()
    {
        return type;
    }

    public String[] getFullyQualifiedNames()
    {
        return names;
    }

    public SqlIdentifier toIdentifier()
    {
        return new SqlIdentifier(names, SqlParserPos.ZERO);
    }

    public String toString() {
        StringBuffer result = new StringBuffer();
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                result.append('.');
            }
            result.append(names[i]);
        }
        return result.toString();
    }
}

// End MonikerImpl.java
