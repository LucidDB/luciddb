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
#include "fennel/common/TraceSource.h"
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
 * This implementation sequentially reads an ascii file page by page. It
 * handles the details of file access, and manages a pointer to unread data.
 * The file is opened during a call to <code>open()</code> and is closed
 * either by an explicit call to <code>close()</code> or on deallocation.
 *
 * <p>
 *
 * Storage must be provided with a call to <code>setStorage()</code> before
 * calling <code>read()</code>. Once <code>read()</code> has been called,
 * <code>getReadPtr()</code> will return a pointer to the current data
 * pointer. A call to <code>getEnd()</code> will return a pointer to the
 * end of data. A call to <code>isFull()</code> describes whether more data
 * can be read without consuming some. A call to <code>isComplete()</code>
 * describes whether or not the file has been completely read.
 *
 * <p>
 *
 * The method <code>setReadPtr()</code> may be used to consume input data.
 * Upon reaching the end of the buffer, subsequent calls to
 * <code>read()</code> are used to fetch more data. Unread data is
 * preserved, but the current read pointer may be updated.
 *
 * <p>
 *
 * FIXME: This class should use a special character pointer. It may be
 * updated to handle unicode. It may be refined to prefetch pages for
 * better performance.
 *
 * @author John Pham
 * @version $Id$
 */

class FlatFileBuffer : public ClosableObject, public TraceSource
{
    std::string path;
    SharedRandomAccessDevice pRandomAccessDevice;
    FileSize filePosition, fileEnd;
    
	char *pBuffer;
	uint bufferSize, contentSize;
    char *pCurrent;

    // implement ClosableObject
    void closeImpl();
    
public:
    /**
     * Constructs a buffer
     *
     * @param path location of flat file to be opened
     */
    FlatFileBuffer(const std::string &path);
    virtual ~FlatFileBuffer();

    /**
     * Opens a file and obtains resources needed for reading the file
     */
	void open();

    /**
     * Sets internal buffers
     *
     * @param buffer storage for characters read
     *
     * @param size size of buffer, in characters
     */
    void setStorage(char *pBuffer, uint size);
    
    /**
     * Reads more data into buffer, preserving unread data.
     * Invalidates previous read pointers.
     *
     * @return number of characters read
     */
	uint read();

    /**
     * Returns a pointer to the current row
     */
    char *getReadPtr();

    /**
     * Returns a pointer to the end of buffer contents
     */
    char *getEndPtr();

    /**
     * Returns the difference between getEndPtr() and getReadPtr()
     */
    int getSize();

    /**
     * Returns whether buffer has been filled to maximum capacity
     */
    bool isFull();

    /**
     * Returns whether entire file has been read
     */
    bool isComplete();
    
    /**
     * Consumes buffer contents up to pointer
     */
    void setReadPtr(char *ptr);
};

FENNEL_END_NAMESPACE

#endif

// End FlatFileBuffer.h
