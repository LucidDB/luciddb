/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.namespace;

import java.sql.*;
import java.util.*;

import net.sf.farrago.FarragoMetadataFactory;
import net.sf.farrago.type.*;


/**
 * FarragoMedNameDirectory defines a virtual hierarchical namespace interface in
 * which to look up tables, routines, other namespaces, etc.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoMedNameDirectory
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Looks up a FarragoMedColumnSet by name.  This method supports Farrago's
     * capability to reference a foreign table directly without having to
     * create local metadata about it.
     *
     * @param typeFactory FarragoTypeFactory to use
     * for defining types
     *
     * @param foreignName foreign compound identifier to lookup
     *
     * @param localName compound identifier by which
     * FarragoMedColumnSet will be referenced locally
     *
     * @return FarragoMedColumnSet, or null if none found
     *
     * @exception SQLException if metadata access is unsuccessful
     */
    public FarragoMedColumnSet lookupColumnSet(
        FarragoTypeFactory typeFactory,
        String [] foreignName,
        String [] localName)
        throws SQLException;

    /**
     * Looks up a subdirectory by name.
     *
     * @param foreignName compound identifier for subdirectory
     *
     * @return subdirectory, or null if none found
     *
     * @exception SQLException if metadata access is unsuccessful
     */
    public FarragoMedNameDirectory lookupSubdirectory(String [] foreignName)
        throws SQLException;

    /**
     * Retrieves the contents of this directory as CWM elements.  The
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


// End FarragoMedNameDirectory.java
