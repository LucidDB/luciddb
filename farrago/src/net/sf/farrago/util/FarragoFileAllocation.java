/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.util;

import java.io.*;

import java.util.logging.*;

import net.sf.farrago.trace.*;


/**
 * FarragoFileAllocation takes care of deleting a File when it is closed. If the
 * File is a directory, the directory and everything under it is deleted.
 * Deletion failure is traced as a warning but does not result in an exception.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoFileAllocation
    implements FarragoAllocation
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getFileAllocationTracer();

    //~ Instance fields --------------------------------------------------------

    private File file;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoFileAllocation.
     *
     * @param file the file to be deleted when this allocation is closed
     */
    public FarragoFileAllocation(File file)
    {
        this.file = file;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (file == null) {
            return;
        }
        if (file.exists()) {
            deleteFileOrDirectory(file);
        }
        file = null;
    }

    private void deleteFileOrDirectory(File file)
    {
        if (file.isDirectory()) {
            deleteDirectory(file);
        } else {
            if (!file.delete()) {
                tracer.warning("Failed to delete file " + file);
            }
        }
    }

    private void deleteDirectory(File dir)
    {
        File [] files = dir.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; ++i) {
                deleteFileOrDirectory(files[i]);
            }
        }
        if (!dir.delete()) {
            tracer.warning("Failed to delete directory " + dir);
        }
    }
}

// End FarragoFileAllocation.java
