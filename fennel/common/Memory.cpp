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

#include <ctype.h>
#include <algorithm>
#include <boost/io/ios_state.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

void hexDump(std::ostream &o,void const *v,uint cb,uint cbDone)
{
    boost::io::ios_all_saver streamStateSaver(o);
    
    PConstBuffer b = (PConstBuffer) v;
    uint cbLine = 16,cbThis;
    o.fill('0');
    for (; cb; cb -= cbThis, cbDone += cbThis) {
        cbThis = std::min(cbLine,cb);
        o << std::hex;
        o.width(4);
        o << cbDone << ": ";
        uint i;
        for (i = 0; i < cbThis; i++, b++) {
            o.width(2);
            o << (uint) *b << " ";
        }
        for (i = cbThis; i < cbLine; i++) {
            o << "   ";
        }
        o << "| ";
        for (i = 0, b -= cbThis; i < cbThis; i++, b++) {
            if (isprint(*b)) {
                o << *b;
            } else {
                o << " ";
            }
        }
        o << std::endl;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End Memory.cpp
