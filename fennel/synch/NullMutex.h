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

#ifndef Fennel_NullMutex_Included
#define Fennel_NullMutex_Included

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * A NullMutex is a dummy class for use in cases where the need for
 * synchronization is parameterized.
 */
class NullMutex
{
public:
    NullMutex()
    {
    }
    
    ~NullMutex()
    {
    }
};

/**
 * Guard class for acquisition of an NullMutex.  Models the
 * boost::ScopedLock concept.
 */
class NullMutexGuard : public boost::noncopyable
{
public:
    explicit NullMutexGuard(NullMutex &)
    {
    }
    
    ~NullMutexGuard()
    {
    }

    void lock()
    {
    }
    
    void unlock()
    {
    }

    bool locked() const
    {
        return true;
    }
    
    operator const void*() const
    {
        return this;
    }
};

FENNEL_END_NAMESPACE

#endif

