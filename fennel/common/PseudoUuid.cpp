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

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/PseudoUuid.h"

#ifdef __MINGW32__
#include <windows.h>
#include <rpcdce.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

void PseudoUuid::generate()
{
#ifdef FENNEL_UUID_REAL
    
    uuid_generate(data);
    
#else

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
}

void PseudoUuid::generateInvalid()
{
    memset(&data,0xFF,sizeof(data));
}

bool PseudoUuid::operator == (PseudoUuid const &other) const
{
#ifdef FENNEL_UUID_REAL
    return uuid_compare(data,other.data) == 0;
#else
    return (memcmp(data,other.data,sizeof(data)) == 0);
#endif
}

FENNEL_END_CPPFILE("$Id$");

// End PseudoUuid.cpp
