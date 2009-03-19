/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2008-2009 The Eigenbase Project
// Copyright (C) 2008-2009 SQLstream, Inc.
// Copyright (C) 2008-2009 LucidEra, Inc.
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
package org.eigenbase.jdbc4;

import org.objectweb.rmijdbc.*;

import java.sql.*;
import java.util.*;

// NOTE jvs 8-Oct 2008:  If you are looking at the copy of this file under
// farrago/src/org/eigenbase/jdbc4, do not try to edit it or check it out.
// The original is checked into Perforce under farrago/jdbc4, and
// gets copied to farrago/src/org/eigenbase/jdbc4 by ant createCatalog.

/**
 * Gunk for JDBC 4 source compatibility.
 * See <a href="http://pub.eigenbase.org/wiki/Jdbc4Transition">Eigenpedia</a>.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class UnwrappableRJConnection
    extends RJConnection
    implements Wrapper
{
    static final long serialVersionUID = 8754470401040578510L;

    protected UnwrappableRJConnection(RJConnectionInterface rmiconn)
    {
        super(rmiconn);
    }

    protected UnwrappableRJConnection(
        RJDriverInterface drv,
        String url,
        Properties info)
        throws Exception
    {
        super(drv, url, info);
    }

    // implement Wrapper
    public boolean isWrapperFor(Class iface)
    {
        return false;
    }

    // implement Wrapper
    public <T> T unwrap(Class<T> iface)
    {
        throw new UnsupportedOperationException("unwrap");
    }
}

// End UnwrappableRJConnection.java
