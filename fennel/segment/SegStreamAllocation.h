/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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

#ifndef Fennel_SegStreamAllocation_Included
#define Fennel_SegStreamAllocation_Included

#include "fennel/common/ClosableObject.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegStreamAllocation is a helper for managing the allocation state
 * of storage created by SegOutputStream when it is used as temp
 * workspace.  It supports three states:
 *
 *<ol>
 *
 *<li>UNALLOCATED:  no segment pages are currently allocated
 *
 *<li>WRITING:  a SegOutputStream is being used to allocate segment pages
 *
 *<li>READING: a SegInputStream is being used to read and simultaneously
 * deallocate segment pages
 *
 *</ol>
 *
 * Calling close() at any time safely returns it to the UNALLOCATED state.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class SegStreamAllocation : public ClosableObject
{
    /**
     * In state READING, a record of number of pages allocated
     * while in state WRITING.
     */
    BlockNum nPagesWritten;

    /**
     * Non-singular iff in state WRITING.
     */
    SharedSegOutputStream pSegOutputStream;

    /**
     * Non-singular iff in state READING.
     */
    SharedSegInputStream pSegInputStream;

protected:
    // implement ClosableObject
    virtual void closeImpl();

public:
    /**
     * Creates a shared pointer to a new SegStreamAllocation, initially in the
     * UNALLOCATED state.  Use this if you want to create a shared pointer,
     * since it will correctly set up the allocation to close on reset.
     *
     * @return new allocation
     */
    static SharedSegStreamAllocation newSegStreamAllocation();

    /**
     * Initialize a SegStreamAllocation in the UNALLOCATED state.  Use this
     * only if you want to use the allocation by value (e.g. on the stack or as
     * a member variable); if you want to manipulate via shared pointers, call
     * newSegStreamAllocation() instead.
     */
    explicit SegStreamAllocation();

    /**
     * Changes state from UNALLOCATED to WRITING.
     *
     * @param pSegOutputStreamInit writer
     */
    void beginWrite(SharedSegOutputStream pSegOutputStreamInit);

    /**
     * Changes state from WRITING to READING, or directly to
     * UNALLOCATED if no pages were allocated while in WRITING state.
     * Note that entering the READING state does not actually
     * access any pages yet.
     *
     * @return reader, or a singular pointer if UNALLOCATED
     */
    void endWrite();

    /**
     * @return reader (only available while in state READING)
     */
    SharedSegInputStream const &getInputStream() const;

    /**
     * @return count of pages which were allocated during WRITING phase
     * (a smaller number may remain allocated since pages are deallocated
     * during READING)
     */
    BlockNum getWrittenPageCount() const;
};

FENNEL_END_NAMESPACE

#endif

// End SegStreamAllocation.h
