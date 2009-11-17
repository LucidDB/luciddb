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

import java.util.ArrayList;

/**
 * Used to extend functionality of mtsql.
 *
 * @author jhahn
 * @version $Id$
 */
public abstract class ConcurrentTestPlugin
{

    /**
     * Should containing test be disabled?
     *
     * @return true if containing test should be disabled
     */
    public boolean isTestDisabled() {
        return false;
    }

    /**
     * What commands are supported by this plugin within
     * a thread or repeat section. Commands should start with '@'.
     *
     * @return List of supported commands
     */
    public Iterable<String> getSupportedThreadCommands() {
        return new ArrayList<String>();
    }

    /**
     * What commands are supported by this plugin before
     * the setup section. Commands should start with '@'.
     *
     * @return List of supported commands
     */
    public Iterable<String> getSupportedPreSetupCommands() {
        return new ArrayList<String>();
    }

    /**
     *  Create and return plugin command for given name.
     * @param name Name of command plugin
     * @param params parameters for command.
     * @return Initialized plugin command.
     */
    public abstract ConcurrentTestPluginCommand getCommandFor(
        String name, String params);

    /**
     *  Do pre-setup action for given command and parameters.
     * @param name Name of command plugin
     * @param params parameters for command.
     */
    public void preSetupFor(String name, String params) {
    }
}
// End ConcurrentTestPlugin.java