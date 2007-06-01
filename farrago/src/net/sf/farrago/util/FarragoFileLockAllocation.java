/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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
package net.sf.farrago.util;

import java.io.*;

import java.nio.channels.*;

import java.util.logging.*;

import net.sf.farrago.trace.*;


/**
 * FarragoFileLockAllocation takes care of unlocking a file when it is closed.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoFileLockAllocation
    implements FarragoAllocation
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Location to lock. We lock a bogus byte way beyond any real data to make
     * sure the lock doesn't interfere with I/O on operating systems with
     * non-advisory lock semantics such as Windows. Don't use Long.MAX_VALUE
     * because that breaks on any OS without large file support.
     */
    private static final int LOCK_OFFSET = 0x7FFFFFFD;

    private static final Logger tracer =
        FarragoTrace.getFileLockAllocationTracer();

    //~ Instance fields --------------------------------------------------------

    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private FileLock lock;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoFileLockAllocation by locking a file.
     *
     * @param owner the FarragoAllocationOwner which will be made responsible
     * for the lock as a result of this call
     * @param file the file to be locked for the lifetime of this allocation; if
     * it does not exist, it will be created (but not deleted) automatically
     * @param tryLock if true and lock cannot be obtained throw an exception; if
     * false, wait for the lock instead
     *
     * @exception IOException if lock attempt failed
     */
    public FarragoFileLockAllocation(
        FarragoAllocationOwner owner,
        File file,
        boolean tryLock)
        throws IOException
    {
        file = file.getCanonicalFile();
        this.file = file;
        file.createNewFile();
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            channel = randomAccessFile.getChannel();
            if (tryLock) {
                tracer.fine("Trying to lock file " + file);
                lock = channel.tryLock(LOCK_OFFSET, 1, false);
                if (lock == null) {
                    throw new IOException();
                }
            } else {
                tracer.fine("Locking file " + file);
                lock = channel.lock(LOCK_OFFSET, 1, false);
            }
        } catch (IOException ex) {
            tracer.fine("Failed to acquire lock on file " + file);
            closeAllocation();
            throw ex;
        }
        tracer.fine("Successfully acquired lock on file " + file);
        owner.addAllocation(this);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoAllocation
    public void closeAllocation()
    {
        try {
            if (lock != null) {
                tracer.fine("Unlocking file " + file);
                lock.release();
            }
        } catch (IOException ex) {
            // TODO:  trace?
        } finally {
            lock = null;
        }
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException ex) {
            // TODO:  trace?
        } finally {
            channel = null;
        }
        try {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        } catch (IOException ex) {
            // TODO:  trace?
        } finally {
            randomAccessFile = null;
        }
    }

    /**
     * Command-line entry point for bench testing. Attempts to lock the file
     * named by the one and only argument and hold the lock for 20 seconds.
     *
     * <p>TODO jvs 24-Aug-2005: Figure out a way to test this automatically.
     * Requires starting two concurrent processes.
     *
     * @param args args[0] = name of file to lock
     */
    public static void main(String [] args)
        throws Exception
    {
        FarragoCompoundAllocation owner = new FarragoCompoundAllocation();
        try {
            FarragoFileLockAllocation alloc =
                new FarragoFileLockAllocation(
                    owner,
                    new File(args[0]),
                    true);
            Thread.currentThread().sleep(20000);
            System.out.println("done");
        } finally {
            owner.closeAllocation();
        }
    }
}

// End FarragoFileLockAllocation.java
