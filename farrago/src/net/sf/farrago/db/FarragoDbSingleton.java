/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
package net.sf.farrago.db;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;


/**
 * FarragoDbSingleton manages a singleton instance of FarragoDatabase. It is
 * reference-counted to allow it to be shared in a library environment such as
 * the directly embedded JDBC driver. Note that all synchronization is done at
 * the class level, not the object level.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoDbSingleton
    extends FarragoCompoundAllocation
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger tracer = FarragoTrace.getDatabaseTracer();

    /**
     * Reference count.
     */
    private static int nReferences;

    // TODO jvs 14-Dec-2005:  make instance private instead of protected
    // once FarragoDatabase no longer extends FarragoDbSingleton

    /**
     * Singleton instance, or null when nReferences == 0.
     */
    protected static FarragoDatabase instance;

    /**
     * Flag indicating whether FarragoDbSingleton is already in {@link
     * #shutdown()}, to help prevent recursive shutdown.
     */
    private static boolean inShutdown;

    /**
     * Flag indicating whether a database backup is already in progress
     */
    private static boolean backupInProgress;

    /**
     * Flag indicating whether FarragoDbSingleton is already in {@link
     * #shutdownConditional(int)}, to help prevent recursive shutdown.
     */
    private static boolean inShutdownConditional;

    //~ Methods ----------------------------------------------------------------

    /**
     * Establishes a database reference. If this is the first reference, the
     * database will be loaded first; otherwise, the existing database is reused
     * with an increased reference count.
     *
     * @param sessionFactory factory for various database-level objects
     *
     * @return loaded database
     */
    public static synchronized FarragoDatabase pinReference(
        FarragoSessionFactory sessionFactory)
    {
        tracer.info("connect");

        ++nReferences;
        if (nReferences == 1) {
            assert (instance == null);
            boolean success = false;
            try {
                FarragoDatabase newDb =
                    new FarragoDatabase(sessionFactory, false);
                assert (newDb == instance);
                success = true;
            } finally {
                if (!success) {
                    nReferences = 0;
                    instance = null;
                }
            }
        }
        return instance;
    }

    /**
     * Establishes a database reference. If requireExistingEngine, a new
     * database will not be loaded, even if this is the first reference.
     *
     * @param sessionFactory factory for various database-level objects
     * @param requireExistingEngine true if require an already created reference
     *
     * @return loaded database
     */
    public static synchronized FarragoDatabase pinReference(
        FarragoSessionFactory sessionFactory,
        boolean requireExistingEngine)
    {
        if (requireExistingEngine) {
            tracer.info("connect");
            if (instance == null) {
                throw FarragoResource.instance().NoDatabaseLoaded.ex();
            }
            ++nReferences;
            return instance;
        }
        return pinReference(sessionFactory);
    }

    static synchronized void addSession(
        FarragoDatabase db,
        FarragoDbSession session)
    {
        assert (db == instance);
        db.addAllocation(session);
    }

    static synchronized void disconnectSession(FarragoDbSession session)
    {
        tracer.info("disconnect");

        FarragoDatabase db = session.getDatabase();

        assert (nReferences > 0);
        assert (db == instance);

        db.forgetAllocation(session);

        nReferences--;
    }

    /**
     * Retrieve a list of connected FarragoSession objects. Each invocation
     * produces a new List. Altering the list has no effect on the given
     * FarragoDatabase.
     *
     * <p>The returned FarragoSession objects may be disconnected at any time.
     * See {@link FarragoSession#isClosed()}.
     *
     * @param db sessions are retrieved from this FarragoDatabase instance
     *
     * @return non-null List of FarragoSession objects
     */
    public static synchronized List<FarragoSession> getSessions(
        FarragoDatabase db)
    {
        List<FarragoSession> sessions = new ArrayList<FarragoSession>();

        for (Object allocation : db.allocations) {
            if (allocation instanceof FarragoSession) {
                sessions.add((FarragoSession) allocation);
            }
        }

        return sessions;
    }

    public static synchronized List<FarragoSession> getSessions()
    {
        assert (instance != null);
        return getSessions(instance);
    }

    /**
     * Conditionally shuts down the database depending on the number of
     * references.
     *
     * @param groundReferences threshold for shutdown; if actual number of
     * sessions is greater than this, no shutdown takes place
     *
     * @return whether shutdown took place
     */
    public static boolean shutdownConditional(int groundReferences)
    {
        boolean flushCodeCache = false;
        boolean success = false;
        synchronized (FarragoDbSingleton.class) {
            if (instance == null) {
                assert (nReferences == 0);
                return true;
            }
            if (inShutdownConditional || inShutdown) {
                return false;
            }
            inShutdownConditional = true;

            try {
                tracer.fine("ground reference count = " + groundReferences);
                tracer.fine("actual reference count = " + nReferences);

                int numLoopbackSessions = countLoopbackSessions();

                if ((nReferences - numLoopbackSessions) <= groundReferences) {
                    flushCodeCache = true;
                }

                success = true;
            } finally {
                // Clean up if an exception is being thrown
                if (!success) {
                    inShutdownConditional = false;
                }
            }
        }

        // Release the monitor on FarragoDbSingleton: Allows loopback sessions
        // to be closed from other threads.  It is possible that a new
        // connection (or even loopback session) will created while the
        // monitor is free.  In that case, we will not shut down.  It is also
        // possible some loopback sessions will not actually be closed by
        // the flush (connection resource leak, timing, etc.), in which case
        // we will not shut down.
        if (flushCodeCache) {
            instance.flushCodeCache();
        }

        synchronized (FarragoDbSingleton.class) {
            try {
                if (nReferences <= groundReferences) {
                    shutdown();
                    return true;
                } else {
                    return false;
                }
            } finally {
                inShutdownConditional = false;
            }
        }
    }

    /**
     * Shuts down the database, killing any running sessions.
     */
    public static void shutdown()
    {
        // REVIEW: SWZ 12/31/2004: If an extension project adds "specialized
        // initialization" that ends up pinning a reference to FarragoDatabase
        // (e.g. it opens a Connection so that it can execute SQL statements),
        // then the extension's specialized shutdown should close the
        // connection. When it does, FarragoSessionFactory.cleanupSessions()
        // will be invoked and calls shutdownConditional() -- resulting in a
        // recursive call to shutdown. The inShutdown field blocks this, but
        // there's probably a better way -- maybe a new implementation of
        // Connection that represents an internal connection and avoids the
        // extra reference count and cleanupSessions() call.

        // SWZ: 09/12/2008: Note that loopback sessions don't really solve
        // the above problem.  Loopback sessions are sessions (connections)
        // owned by data wrappers/servers which can be closed by flushing
        // the code cache.

        boolean flushCodeCache = false;
        synchronized (FarragoDbSingleton.class) {
            if (instance == null) {
                assert (nReferences == 0);
                return;
            }
            if (inShutdown) {
                return;
            }
            inShutdown = true;

            tracer.info("shutdown");

            if (countLoopbackSessions() > 0) {
                flushCodeCache = true;
            }
        }

        // Attempt to close loopback sessions.  We will shut down even if
        // none close.
        if (flushCodeCache) {
            instance.flushCodeCache();
        }

        synchronized (FarragoDbSingleton.class) {
            assert (instance != null);
            try {
                instance.sessionFactory.specializedShutdown();
                instance.close(false);
            } finally {
                instance = null;
                nReferences = 0;
                inShutdown = false;
            }
        }
    }

    private static int countLoopbackSessions()
    {
        int numLoopbackSessions = 0;
        for (FarragoSession session : getSessions()) {
            if (session.isLoopback()) {
                numLoopbackSessions++;
            }
        }
        return numLoopbackSessions;
    }

    /**
     * @return true if the single currently exists
     */
    public static boolean isReferenced()
    {
        if (nReferences > 0) {
            assert (instance != null);
            return true;
        } else {
            assert (instance == null);
            return false;
        }
    }

    /**
     * Sets a flag indicating whether a backup is in progress. If the flag is
     * true and a backup is already in progress, then false is returned.
     * Otherwise, true is returned.
     *
     * @param inProgress true if the flag is to be set, indicating that a backup
     * is in progress
     *
     * @return true if setting of the flag was successful
     */
    public static synchronized boolean setBackupFlag(boolean inProgress)
    {
        if (inProgress && backupInProgress) {
            return false;
        }
        backupInProgress = inProgress;
        return true;
    }

    public static boolean isInShutdown()
    {
        return inShutdown;
    }
}

// End FarragoDbSingleton.java
