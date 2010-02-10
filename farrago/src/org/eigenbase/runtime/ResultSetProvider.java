/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2010-2010 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2010-2010 LucidEra, Inc.
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
package org.eigenbase.runtime;

import java.sql.*;

/**
 * ResultSetProvider is an interface for supplying a result set, typically
 * for use where deferred ResultSet creation is required.
 *
 * @author John Sichi
 * @version $Id$
 */
public interface ResultSetProvider
{
    /**
     * @return result set to be used
     */
    public ResultSet getResultSet() throws SQLException;
}

// End ResultSetProvider.java
