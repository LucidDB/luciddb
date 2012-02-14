/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_PseudoUuid_Included
#define Fennel_PseudoUuid_Included

#include "fennel/common/FennelExcn.h"

#if defined(HAVE_LIBUUID) || defined(HAVE_LIBUUID_NEW)

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
class FENNEL_COMMON_EXPORT PseudoUuid
{
public:
#ifdef FENNEL_UUID_REAL_NEW
    static const int UUID_LENGTH = UUID_LEN_BIN;
#else
    static const int UUID_LENGTH = 16;
#endif

protected:
#ifdef FENNEL_UUID_REAL
    uuid_t data;

#else
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
class FENNEL_COMMON_EXPORT PseudoUuidGenerator
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
