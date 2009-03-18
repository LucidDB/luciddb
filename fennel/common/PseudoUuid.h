/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

// REVIEW: SWZ: 9/23/2006: It's possible to HAVE_LIBUUID but not either version
// of uuid.h.  Should probably detect and use a #error (or something) here.

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
#ifdef FENNEL_UUID_REAL
    uuid_t data;

#else /* FENNEL_UUID_FAKE || FENNEL_UUID_REAL_NEW */
    /*
     * For FENNEL_UUID_REAL_NEW, uuid_t is not longer a concrete type.
     * To keep PseudoUuid simple, we use the new API to copy UUIDs into
     * our own array.
     */
    uint8_t data[UUID_LENGTH];
#endif

public:
    explicit PseudoUuid();
    PseudoUuid(std::string uuid);

    virtual ~PseudoUuid();

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

    uint8_t getByte(int) const;

    const uint8_t *getBytes() const;

    /**
     * Converts  an  input UUID string of the form
     * 1b4e28ba-2fa1-11d2-883f-b9a761bde3fb
     * into the internal representation.
     *
     * @throws FennelExcn if the String is not in the correct format.
     */
    void parse(std::string uuid) throw(FennelExcn);
};

/**
 * Generator for values of PseudoUuid.  Default implementation just
 * calls PseudoUuid.generate() to use whatever OS implementation was
 * supplied by the Fennel build, but subclasses may override
 * (e.g. to call to a Java-based generator).
 */
class PseudoUuidGenerator
{
public:
    virtual ~PseudoUuidGenerator();

    /**
     * Generates a new UUID value.
     *
     * @param pseudoUuid receives the generated value
     */
    virtual void generateUuid(PseudoUuid &pseudoUuid);
};

inline std::ostream &operator<<(std::ostream &str, PseudoUuid const &uuid)
{
    str << uuid.toString();
    return str;
}

FENNEL_END_NAMESPACE

#endif

// End PseudoUuid.h
