/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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
package net.sf.farrago.util;

import java.io.*;
import java.nio.channels.*;


/**
 * FarragoFileLockAllocation takes care of unlocking a file when it is closed.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoFileLockAllocation implements FarragoAllocation
{
    //~ Instance fields -------------------------------------------------------

    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private FileLock lock;

    //~ Constructors ----------------------------------------------------------

    /**
     * Create a new FarragoFileLockAllocation by locking a file.
     *
     * @param owner the FarragoAllocationOwner which will be made responsible
     * for the lock as a result of this call
     *
     * @param file the file to be locked for the lifetime of this allocation
     *
     * @param tryLock if true and lock cannot be obtained throw an
     * exception; if false, wait for the lock instead
     *
     * @exception IOException if lock attempt failed
     */
    public FarragoFileLockAllocation(
        FarragoAllocationOwner owner,
        File file,
        boolean tryLock)
        throws IOException
    {
        try {
            if (!file.exists()) {
                // REVIEW:  to avoid i18n in the util package, just throw
                // IOException with no message; caller must wrap with
                // a proper excn.  Is this OK?
                throw new IOException();
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            channel = randomAccessFile.getChannel();
            if (tryLock) {
                // NOTE:  we lock a bogus byte way beyond any real data
                // to make sure the lock doesn't interfere with I/O
                // on operating systems with non-advisory lock semantics
                // such as Windows.  Don't use Long.MAX_VALUE because
                // that breaks on any OS without large file support.
                lock = channel.tryLock(0x7FFFFFFE - 1, 1, false);
                if (lock == null) {
                    throw new IOException();
                }
            } else {
                lock = channel.lock(Integer.MAX_VALUE - 1, 1, false);
            }
        } catch (IOException ex) {
            closeAllocation();
            throw ex;
        }
        owner.addAllocation(this);
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoAllocation
    public void closeAllocation()
    {
        try {
            if (lock != null) {
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
}


// End FarragoFileLockAllocation.java
