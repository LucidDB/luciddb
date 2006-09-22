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

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/PseudoUuid.h"

#ifdef __MINGW32__
#include <windows.h>
#include <rpcdce.h>
#endif

#if !defined(FENNEL_UUID_REAL) && !defined(FENNEL_UUID_REAL_NEW)
#include <iomanip>
#include <sstream>
#endif

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

PseudoUuid::PseudoUuid()
{
    memset(data, 0, sizeof(data));
}

PseudoUuid::PseudoUuid(string uuid)
{
    parse(uuid);
}

void PseudoUuid::generate()
{
#ifdef FENNEL_UUID_REAL_NEW

    // REVIEW: SWZ: 9/22/2006: Consider using a different mode.  V1 seems
    // weak, but v3 and v5 seem reasonable.  They require arguments, however.
    // Also, this method never returns error for V4, but might for others.
    uuid_rc_t result = 
        uuid_make(reinterpret_cast<uuid_t *>(data), UUID_MAKE_V4);
    assert(result == UUID_RC_OK);

#else /* FENNEL_UUID_REAL || FENNEL_UUID_FAKE */

#ifdef FENNEL_UUID_REAL
    
    uuid_generate(data);

#else /* FENNEL_UUID_FAKE */

    memset(&data,0,sizeof(data));
#ifdef __MINGW32__
    assert(sizeof(data) == sizeof(UUID));
    UuidCreate((UUID *) data);
#else
    int x = rand();
    assert(sizeof(x) <= sizeof(data));
    memcpy(&data,&x,sizeof(x));
#endif
    
#endif

#endif
}

void PseudoUuid::generateInvalid()
{
    memset(&data,0xFF,sizeof(data));
}

bool PseudoUuid::operator == (PseudoUuid const &other) const
{
#ifdef FENNEL_UUID_REAL_NEW

    int cmp;
    uuid_rc_t result = 
        uuid_compare(
            reinterpret_cast<const uuid_t *>(data),
            reinterpret_cast<const uuid_t *>(other.data),
            &cmp);
    assert(result == UUID_RC_OK);
    return cmp == 0;

#else /* FENNEL_UUID_REAL || FENNEL_UUID_FAKE */

#ifdef FENNEL_UUID_REAL

    return uuid_compare(data,other.data) == 0;

#else /* FENNEL_UUID_FAKE */

    return (memcmp(data,other.data,sizeof(data)) == 0);

#endif

#endif
}

unsigned char PseudoUuid::getByte(int index) const
{
    assert(index < sizeof(data));

    return data[index];
}

int PseudoUuid::hashCode() const {
    return
        ((int)(data[0] ^ data[4] ^ data[8] ^ data[12]) & 0xFF) << 24 |
        ((int)(data[1] ^ data[5] ^ data[9] ^ data[13]) & 0xFF) << 16 |
        ((int)(data[2] ^ data[6] ^ data[10] ^ data[14]) & 0xFF) << 8 |
        ((int)(data[3] ^ data[7] ^ data[11] ^ data[15]) & 0xFF);
}

string PseudoUuid::toString() const
{
#ifdef FENNEL_UUID_REAL_NEW

    char uuidstr[40];
    char *pUuidStr = uuidstr;
    size_t uuidstrlen = sizeof(uuidstr);
    uuid_rc_t result = 
        uuid_export(
            reinterpret_cast<const uuid_t *>(data),
            UUID_FMT_STR,
            reinterpret_cast<void **>(&pUuidStr),
            &uuidstrlen);
    assert(result == UUID_RC_OK);
    return string(uuidstr);

#else /* FENNEL_UUID_REAL || FENNEL_UUID_FAKE */

#ifdef FENNEL_UUID_REAL
    char uuidstr[40];
    uuid_unparse(data, uuidstr);
    return string(uuidstr);

#else /* FENNEL_UUID_FAKE */

    ostringstream ostr;
    
    for(int i = 0; i < sizeof(data); i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            ostr << "-";
        }
        
        ostr << hex << setw(2) << setfill('0') << (int) data[i];;
    }
    
    return ostr.str();
#endif

#endif
}

void PseudoUuid::parse(string uuid) throw (FennelExcn)
{
#ifdef FENNEL_UUID_REAL_NEW
    uuid_rc_t rv = 
        uuid_import(
            reinterpret_cast<uuid_t *>(data),
            UUID_FMT_STR,
            uuid.c_str(),
            uuid.length());
    if (rv != UUID_RC_OK) {
        throw FennelExcn("Invalid UUID format");
    }
#else /* FENNEL_UUID_REAL || FENNEL_UUID_FAKE */

#ifdef FENNEL_UUID_REAL    
    int rv = uuid_parse(uuid.c_str(), data);
    if (rv == -1) {
       throw FennelExcn("Invalid UUID format");
    }    

#else /* FENNEL_UUID_FAKE */

    unsigned char id[UUID_LENGTH];
    if (uuid.length() != 36) {
        ostringstream errstr;
        errstr << "Invalid UUID format: length " << uuid.length() 
               << ", expected 36";
        throw FennelExcn(errstr.str());
    }
    
    istringstream istr(uuid);
    char hexchars[3];
    char *endptr;
    int value;
    memset(hexchars, 0, sizeof(hexchars));
    istr >> noskipws;
    for(int i = 0; i < sizeof(id); i++) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            char ch;
            istr >> ch;
            if (ch != '-') {
                throw FennelExcn("Invalid UUID format: '-' expected");
            }
        }
        istr >> hexchars[0];
        istr >> hexchars[1];
        value = strtol(hexchars, &endptr, 16);
        // Make sure both characters were correctly converted
        if (endptr != hexchars+2) {
            throw FennelExcn("Invalid UUID format: hex digits expected");
        }
        id[i] = (uint8_t) value;
    }
    memcpy(data, id, sizeof(data));
#endif
#endif
}

FENNEL_END_CPPFILE("$Id$");

// End PseudoUuid.cpp
