/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
package org.eigenbase.test.concurrent;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Used to extend functionality of mtsql.
 *
 * @author jhahn
 * @version $Id$
 */
public interface ConcurrentTestPluginCommand {

    public static interface TestContext {
        /**
         * Store a message as output for mtsql script.
         *
         * @param message Message to be output
         */
        public void storeMessage(String message);

        /**
         * Get connection for thread.
         *
         * @return connection for thread
         */
        public Connection getConnection();

        /**
         * Get current statement for thread, or null if none.
         *
         * @return current statement for thread
         */
        public Statement getCurrentStatement();
    }

    /**
     * Implement this method to extend functionality of mtsql.
     *
     * @param testContext Exposed context for plugin to run in.
     * @throws IOException
     */
    void execute(TestContext testContext)
        throws IOException;
}
// End ConcurrentTestPluginCommand.java