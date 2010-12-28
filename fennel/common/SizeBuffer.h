/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2003 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifndef Fennel_SharedBuffer_Included
#define Fennel_SharedBuffer_Included

#include <boost/scoped_array.hpp>
#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Contains a buffer, its maximum length and current length.
 *
 * @author John Kalucki
 * @since Aug 01, 2005
 * @version $Id$
 **/
class FENNEL_COMMON_EXPORT SizeBuffer : public boost::noncopyable
{
public:
    explicit SizeBuffer(uint capacity, uint length = 0);
    ~SizeBuffer()
    {
    }
    void length(uint length);
    uint length() const;
    uint capacity() const;

    // Renaming buffer() as get() to be in-line with boost auto pointers
    // is problematic. Too easy for a shared pointer to do shared.get()
    // instead of shared->get() and get a pointer to the wrong object.
    // The use of buffer() forces this mistake to be discovered at
    // compilation time
    PBuffer buffer() const;

protected:
    boost::scoped_array<fennel::FixedBuffer> buf;
    uint cap;
    uint len;
};

FENNEL_END_NAMESPACE

#endif

// End SizeBuffer.h
