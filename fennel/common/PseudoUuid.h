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

#ifdef HAVE_LIBUUID

#ifdef HAVE_UUID_UUID_H
#include <uuid/uuid.h>
#define FENNEL_UUID_REAL
#endif

#ifdef HAVE_UUID_H
#include <uuid.h>
#define FENNEL_UUID_REAL_NEW
#endif

#else /* !HAVE_LIBUUID */

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
public:
#ifdef FENNEL_UUID_REAL_NEW
    static const int UUID_LENGTH = UUID_LEN_BIN;
#else /* FENNEL_UUID_REAL || FENNEL_UUID_FAKE */
    static const int UUID_LENGTH = 16;
#endif

protected:
#ifdef FENNEL_UUID_REAL_NEW
    unsigned char data[UUID_LENGTH];
#else /* FENNEL_UUID_REAL || FENNEL_UUID_FAKE */

#ifdef FENNEL_UUID_REAL

    uuid_t data;

#else /* FENNEL_UUID_FAKE */

    unsigned char data[UUID_LENGTH];

#endif
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
