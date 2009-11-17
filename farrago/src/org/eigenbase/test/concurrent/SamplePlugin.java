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
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * Sample mtsql plugin. To use add at start of script
 * "@plugin org.eigenbase.test.concurrent.SamplePlugin".  After doing a prepare
 * you can then do "@describeResultSet" to show columns returned by query.
 *
 * @author jhahn
  * @version $Id$
 */
public class SamplePlugin extends ConcurrentTestPlugin
{

    private final static String DESCRIBE_RESULT_SET_CMD = "@describeResultSet";
    public ConcurrentTestPluginCommand getCommandFor(String name, String params)
    {
        if (name.equals(DESCRIBE_RESULT_SET_CMD)) {
            return new DescribeResultSet();
        }
        assert (false);
        return null;
    }

    public Iterable<String> getSupportedThreadCommands()
    {
        return Arrays.asList(new String[] { DESCRIBE_RESULT_SET_CMD });
    }

    static class DescribeResultSet implements ConcurrentTestPluginCommand {

        public void execute(TestContext testContext) throws IOException
        {
            Statement stmt =
                (PreparedStatement) testContext.getCurrentStatement();
            if (stmt == null) {
                testContext.storeMessage("No current statement");
            } else if (!(stmt instanceof PreparedStatement)) {
            } else {
                try {
                    ResultSetMetaData metadata =
                        ((PreparedStatement) stmt).getMetaData();
                    for (int i = 1; i <= metadata.getColumnCount(); i++) {
                        testContext.storeMessage(
                            metadata.getColumnName(i) + ": "
                            + metadata.getColumnTypeName(i));
                    }
                } catch (SQLException e) {
                    throw new IllegalStateException(e.toString());
                }
            }
        }
    }
}
// End SamplePlugin.java