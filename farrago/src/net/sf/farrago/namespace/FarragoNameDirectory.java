/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.namespace;

import net.sf.farrago.FarragoMetadataFactory;
import net.sf.farrago.type.*;

import java.sql.*;
import java.util.*;

/**
 * FarragoNameDirectory defines a virtual hierarchical namespace interface in
 * which to look up tables, routines, other namespaces, etc.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoNameDirectory
{
    /**
     * Look up a FarragoNamedColumnSet by name.  This method supports Farrago's
     * capability to reference a foreign table directly without having to
     * create local metadata about it.
     *
     * @param typeFactory FarragoTypeFactory to use
     * for defining types
     *
     * @param foreignName foreign compound identifier to lookup 
     *
     * @param localName compound identifier by which
     * FarragoNamedColumnSet will be referenced locally
     *
     * @return FarragoNamedColumnSet, or null if none found
     *
     * @exception SQLException if metadata access is unsuccessful
     */
    public FarragoNamedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String [] foreignName,
        String [] localName)
        throws SQLException;

    /**
     * Look up a subdirectory by name.
     *
     * @param foreignName compound identifier for subdirectory
     *
     * @return subdirectory, or null if none found
     *
     * @exception SQLException if metadata access is unsuccessful
     */
    public FarragoNameDirectory lookupSubdirectory(String [] foreignName)
        throws SQLException;

    /**
     * Retrieve the contents of this directory as CWM elements.  The
     * implementation should construct new instances of CWM elements and return
     * them.  There is no need to start a transaction; the caller will take
     * care of that as well as commit/rollback.  This method supports the
     * SQL/MED IMPORT FOREIGN SCHEMA statement, and general metadata browsing.
     *
     *<p>
     *
     * NOTE jvs 23-Feb-2003:  This will probably get more complicated;
     * right now it's just a placeholder.
     *
     * @param factory factory for creating CWM elements
     *
     * @return element iterator, or null if enumeration is unsupported
     *
     * @exception SQLException if metadata access is unsuccessful
     * (but not if enumeration is unsupported)
     */
    public Iterator getContentsAsCwm(FarragoMetadataFactory factory)
        throws SQLException;
}

// End FarragoNameDirectory.java
