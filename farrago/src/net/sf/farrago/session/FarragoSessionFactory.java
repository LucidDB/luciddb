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

package net.sf.farrago.session;

import net.sf.farrago.parser.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.util.*;

import java.util.*;

/**
 * FarragoSessionFactory defines an interface with factory methods used
 * to create new sessions and related objects.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionFactory
{
    /**
     * Creates a new session.
     *
     * @param url (same as for JDBC connect)
     *
     * @param info (same as for JDBC connect)
     *
     * @return new session
     */
    public FarragoSession newSession(
        String url,
        Properties info);

    /**
     * Creates a new intrepreter for Fennel commands.
     *
     * @return new interpeter
     */
    public FennelCmdExecutor newFennelCmdExecutor();

    /**
     * Opens a new catalog instance.
     *
     * @param owner the object which should own the new catalog
     *
     * @param userCatalog true for user catalog; false for system catalog
     *
     * @return new catalog instance
     */
    public FarragoCatalog newCatalog(
        FarragoAllocationOwner owner,
        boolean userCatalog);

    /**
     * Creates a new Fennel transaction context.
     *
     * @param catalog catalog for transaction metadata access
     *
     * @param fennelDbHandle handle for database to access
     *
     * @return new context
     */
    public FennelTxnContext newFennelTxnContext(
        FarragoCatalog catalog,
        FennelDbHandle fennelDbHandle);

    /**
     * Gives this factory a chance to clean up after a session has been closed.
     */
    public void cleanupSessions();
}

// End FarragoSessionFactory.java
