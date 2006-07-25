/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package net.sf.farrago.session;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.parser.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.util.*;


/**
 * FarragoSessionFactory defines an interface with factory methods used to
 * create new sessions and related objects.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionFactory
    extends FarragoSessionPersonalityFactory
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a new session.
     *
     * @param url (same as for JDBC connect)
     * @param info (same as for JDBC connect)
     *
     * @return new session
     */
    public FarragoSession newSession(
        String url,
        Properties info);

    /**
     * Creates a new reentrant session. The base session is cloned with a cloned
     * copy of the base session's session variables.
     *
     * @param session the existing session on which to base the re-entrant
     * session
     */
    public FarragoSession newReentrantSession(FarragoSession session);

    /**
     * Release a reentrant session.
     */
    public void releaseReentrantSession(FarragoSession session);

    /**
     * Creates a new intrepreter for Fennel commands.
     *
     * @return new interpeter
     */
    public FennelCmdExecutor newFennelCmdExecutor();

    /**
     * Opens a new repositor instance.
     *
     * @param owner the object which should own the new repos
     * @param userRepos true for user repos; false for system repos
     *
     * @return new repos instance
     */
    public FarragoRepos newRepos(
        FarragoAllocationOwner owner,
        boolean userRepos);

    /**
     * Creates a new Fennel transaction context.
     *
     * @param repos repos for transaction metadata access
     * @param fennelDbHandle handle for database to access
     *
     * @return new context
     */
    public FennelTxnContext newFennelTxnContext(
        FarragoRepos repos,
        FennelDbHandle fennelDbHandle);

    /**
     * @return a new transaction manager
     */
    public FarragoSessionTxnMgr newTxnMgr();

    /**
     * Loads a model extension plugin instance.
     *
     * @param pluginClassLoader FarragoPluginClassLoader to use to load the
     * plugin
     * @param femJar catalog descriptor for jar from which plugin is loaded
     */
    public FarragoSessionModelExtension newModelExtension(
        FarragoPluginClassLoader pluginClassLoader,
        FemJar femJar);

    /**
     * Allows extensions of Farrago to add their own parameters to the Fennel
     * configuration parameters.
     *
     * @param map a non-null, modifiable Map where configuration parameters can
     * be added. All map keys and values are Strings.
     */
    public void applyFennelExtensionParameters(Map map);

    /**
     * Allows extensions of Farrago to perform their own initialization tasks.
     *
     * @param owner the object that should own any allocated objects
     */
    public void specializedInitialization(FarragoAllocationOwner owner);

    /**
     * Allows extensions of Farrago to perform their own shutdown tasks.
     */
    public void specializedShutdown();

    /**
     * Gives this factory a chance to clean up after a session has been closed.
     */
    public void cleanupSessions();

    /**
     * Defines resource bundles to be used for localizing model object names.
     * Any resource having a name of the form "UmlXXX" will be interpreted as
     * the localized name for UML class XXX.
     *
     * @param bundleList receives instances of ResourceBundle
     */
    public void defineResourceBundles(
        List bundleList);
}

// End FarragoSessionFactory.java
