/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.test.concurrent;

import java.io.*;

import java.util.*;


/**
 * FarragoTestConcurrentScriptedTestCase is a base class for multi-threaded,
 * scripted tests. Subclasses must implement the suite() method in order to get
 * a database connection correctly initialized.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public abstract class FarragoTestConcurrentScriptedTestCase
    extends FarragoTestConcurrentTestCase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoTestConcurrentScriptedTestCase object.
     *
     * @param testName .
     *
     * @throws Exception .
     */
    protected FarragoTestConcurrentScriptedTestCase(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Executes the given multi-threaded test script.
     */
    protected void runScript(String mtsqlFile, String jdbcUrl)
        throws Exception
    {
        File mtsqlFileSansExt =
            new File(mtsqlFile.substring(0, mtsqlFile.length() - 6));

        FarragoTestConcurrentScriptedCommandGenerator cmdGen =
            newScriptedCommandGenerator(mtsqlFile);

        if (cmdGen.isDisabled()) {
            return;
        }

        cmdGen.executeSetup(jdbcUrl);

        executeTest(
            cmdGen,
            cmdGen.useLockstep(),
            jdbcUrl);

        Map<Integer, String[]> results = cmdGen.getResults();
        OutputStream outStream = openTestLogOutputStream(mtsqlFileSansExt);

        BufferedWriter out =
            new BufferedWriter(new OutputStreamWriter(outStream));

        for (Map.Entry<Integer, String[]> entry : results.entrySet()) {
            Integer threadId = entry.getKey();
            String [] threadResult = entry.getValue();

            String threadName = "thread " + threadResult[0];
            if (FarragoTestConcurrentScriptedCommandGenerator.SETUP_THREAD_ID
                .equals(
                    threadId))
            {
                threadName = "setup";
            }

            out.write("-- " + threadName);
            out.newLine();
            out.write(threadResult[1]);
            out.write("-- end of " + threadName);
            out.newLine();
            out.newLine();
        }

        out.flush();

        diffTestLog();
    }
}

// End FarragoTestConcurrentScriptedTestCase.java
