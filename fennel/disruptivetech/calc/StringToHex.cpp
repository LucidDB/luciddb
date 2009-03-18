/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 The Eigenbase Project
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
#include "fennel/disruptivetech/calc/StringToHex.h"

#include <string>
#include <sstream>
#include <iomanip>

FENNEL_BEGIN_CPPFILE("$Id$");

using namespace fennel;
using namespace std;

// returns a hex-coded string
string
stringToHex(char const * const buf)
{
    assert(buf != NULL);
    uint buflen = strlen(buf);
    return stringToHex(buf, buflen);
}

string
stringToHex(string const & s)
{
    return stringToHex(s.c_str());
}

string
stringToHex(char const * const buf, uint buflen)
{
    assert(buf != NULL);
    ostringstream ostr;
    for (uint i=0; i<buflen; i++) {
        unsigned char ch = (unsigned char) buf[i];
        ostr << hex << setw(2) << setfill('0') << (uint) ch;
    }
    return ostr.str();
}

FENNEL_END_CPPFILE("$Id$");

// End StringToHex.cpp
