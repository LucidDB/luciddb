/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_PseudoUuid_Included
#define Fennel_PseudoUuid_Included

#include "fennel/common/FennelExcn.h"

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
protected:
    static const int UUID_LENGTH = 16;

#ifdef FENNEL_UUID_REAL
    uuid_t data;
#else
    unsigned char data[UUID_LENGTH];
#endif

private:
    /**
     * Convert  an  input UUID string of the form 
     * 1b4e28ba-2fa1-11d2-883f-b9a761bde3fb
     * into the internal representation.
     *
     * @throws FennelExcn if the String is not in the correct format.
     */
    void parse(std::string uuid) throw(FennelExcn);
    
public:
    PseudoUuid();
    PseudoUuid(std::string uuid);

    /**
     * Generates a new UUID.
     */
    void generate();

    /**
     * Generates a bogus constant UUID.
     */
    void generateInvalid();

    /**
     * Converts the UUID into a string of the form
     * 1b4e28ba-2fa1-11d2-883f-b9a76
     */
    std::string toString() const;

    /**
     * Returns the hash code for the UUID
     */
    int hashCode() const;

    bool operator == (PseudoUuid const &) const;
    
    bool operator != (PseudoUuid const &other) const
    {
        return !(*this == other);
    }

    unsigned char getByte(int) const;
};

inline std::ostream &operator<<(std::ostream &str, PseudoUuid const &uuid)
{
    str << uuid.toString();
    return str;
}

FENNEL_END_NAMESPACE

#endif

// End PseudoUuid.h
