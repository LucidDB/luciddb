/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.util;

import java.io.*;
import java.util.logging.*;

import net.sf.farrago.trace.*;


/**
 * FarragoFileAllocation takes care of deleting a File when it is closed.  If
 * the File is a directory, the directory and everything under it is deleted.
 * Deletion failure is traced as a warning but does not result in an exception.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoFileAllocation implements FarragoAllocation
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getFileAllocationTracer();

    //~ Instance fields -------------------------------------------------------

    private File file;

    //~ Constructors ----------------------------------------------------------

    /**
     * Create a new FarragoFileAllocation.
     *
     * @param file the file to be deleted when this allocation is closed
     */
    public FarragoFileAllocation(File file)
    {
        this.file = file;
    }

    //~ Methods ---------------------------------------------------------------

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
