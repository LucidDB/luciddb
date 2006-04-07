/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

#ifndef Fennel_FlatFileBinding_Included
#define Fennel_FlatFileBinding_Included

#include "fennel/device/RandomAccessRequest.h"

FENNEL_BEGIN_NAMESPACE

/**
 * This class specifies parameters for reading or writing a file.
 * When data transfer errors are detected, an error is thrown.
 *
 * @author John Pham
 * @version $Id$
 */
class FlatFileBinding : public RandomAccessRequestBinding
{
    std::string path;
    char *pBuffer;
    uint bufferSize;
    
public:
    /**
     * Initializes an object with file access parameters
     *
     * @param path path to file that is to be accessed
     * @param buf buffer to be written into or containing data to be read
     * @param size size of the buffer, in bytes
     */
    FlatFileBinding(std::string &path, char *buf, uint size);

    // implement RandomAccessRequestBinding
    PBuffer getBuffer() const;
    uint getBufferSize() const;
    void notifyTransferCompletion(bool bSuccess);
};

FENNEL_END_NAMESPACE

#endif

// End FlatFileBinding.h
