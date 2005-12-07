/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_FlatFileBuffer_Included
#define Fennel_FlatFileBuffer_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/device/RandomAccessDevice.h"

FENNEL_BEGIN_NAMESPACE

class FlatFileBuffer;
typedef boost::shared_ptr<FlatFileBuffer> SharedFlatFileBuffer;

/**
 * FlatFileBuffer provides data for FlatFileExecStream. The data is provided
 * as a stream of characters stored in a buffer.
 *
 * <p>
 *
 * This implementation sequentially reads an ascii file page by page. It may
 * be updated to handle unicode. In addition, other implementations may choose
 * to prefetch pages for better performance.
 *
 * <p>
 *
 * FIXME: this class should manage the "current row" pointer. May want
 * multiple pages to implement read ahead. end() is ambiguous, may want to
 * change it to readCompleted(), then have end() return pointer to end of
 * contents.
 *
 * @author John Pham
 * @version $Id$
 */

class FlatFileBuffer : public ClosableObject
{
    std::string path;
    SharedRandomAccessDevice pRandomAccessDevice;
    FileSize filePosition, fileEnd;
    
protected:
	char *buffer;
	uint bufferSize, contentSize;

    // implement ClosableObject
    void closeImpl();
    
public:
    virtual ~FlatFileBuffer();

    /**
     * construct a buffer
     *
     * @param path location of flat file to be opened
     */
    FlatFileBuffer(const std::string &path);

    /**
     * sets internal buffers
     *
     * @param buffer storage for characters read
     *
     * @param size size of buffer, in characters
     */
    void setStorage(char *buffer, uint size);
    
    /**
     * open file and allocates resources needed for reading the file
     */
	void open();

    /**
     * returns a pointer to the beginning of the buffer
     */
    inline char *buf() { return buffer; }
    
    /**
     * returns the size of the buffer contents, in characters
     */
    inline uint size() { return contentSize; }

    /**
     * returns a pointer to the end of buffer contents
     */
    inline char *contentEnd() { return buffer + contentSize; }
    
    /**
     * whether entire file has been read
     */
    bool readCompleted();
    
    /**
     * reads more data into buffer, preserving unread portion of the buffer;
     * the unread portion is moved to the beginning of the buffer
     *
     * @param unread pointer to unread portion of the buffer
     *
     * @return number of characters read
     */
	uint fill(char *unread = NULL);

    /**
     * whether buffer has been filled to maximum capacity
     */
    bool full();
};

FENNEL_END_NAMESPACE

#endif

// End FlatFileBuffer.h
