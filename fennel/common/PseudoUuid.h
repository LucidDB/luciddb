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

#ifndef Fennel_PseudoUuid_Included
#define Fennel_PseudoUuid_Included

#if defined(HAVE_UUID_UUID_H) && defined(HAVE_LIBUUID)
#include <uuid/uuid.h>
#define FENNEL_UUID_REAL
#else
#define FENNEL_UUID_FAKE
#endif

FENNEL_BEGIN_NAMESPACE

/**
 * Wrapper for a UUID.  Since real UUID generation may not be available on all
 * systems, use this instead (may be real, may be fake).  Note that the
 * default constructor leaves the value uninitialized; call generate to get a
 * new UUID.
 */
class PseudoUuid
{
#ifdef FENNEL_UUID_REAL
    uuid_t data;
#else
    unsigned char data[16];
#endif

public:
    /**
     * Generates a new UUID.
     */
    void generate();

    /**
     * Generates a bogus constant UUID.
     */
    void generateInvalid();
    
    bool operator == (PseudoUuid const &) const;
    
    bool operator != (PseudoUuid const &other) const
    {
        return !(*this == other);
    }

    unsigned char getByte(int) const;
};

FENNEL_END_NAMESPACE

#endif

// End PseudoUuid.h
