/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
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

#ifndef Fennel_FileDevice_Included
#define Fennel_FileDevice_Included

#include "fennel/device/DeviceMode.h"
#include "fennel/synch/SynchObj.h"

FENNEL_BEGIN_NAMESPACE

class DeviceIOListener;
class RandomAccessRequest;

/**
 * FileDevice is a base class for devices built atop the OS file system.
 */
class FileDevice
{
protected:
    // REVIEW:  should this be synchronized here or at a higher level?
    /**
     * current file size in bytes
     */
    FileSize cbFile;

    /**
     * the opened OS file
     */
    int handle;

    /**
     * mode in which file was opened
     */
    DeviceMode mode;

    /**
     * path to file in file system
     */
    std::string filename;

    /**
     * On Cygwin, there's no pread/pwrite, so all I/O has to be
     * synchronized per device.
     */
    StrictMutex mutex;

public:
    /**
     * Opens a file device.
     *
     * @param filename path to file
     * @param mode modifiers for how to open file
     */
    FileDevice(std::string filename,DeviceMode mode);
    virtual ~FileDevice();

    void transfer(RandomAccessRequest const &);
    
    void flush();
    
    void close();
    
    /**
     * @return whether the device file is currently open
     */
    bool isOpen() const
    {
        return handle == -1 ? 0 : 1;
    }

    FileSize getSizeInBytes()
    {
        return cbFile;
    }
    
    void setSizeInBytes(FileSize cbNew);
};

FENNEL_END_NAMESPACE

#endif

// End FileDevice.h
