/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#include <ctype.h>
#include <algorithm>

#include <boost/io/ios_state.hpp>
#include <boost/format.hpp>

#ifdef __MINGW32__
# include <windows.h>
#else
# include <pthread.h>
#endif

// NOTE jvs 26-June-2005:  I added this to squelch link errors with
// the Boost filesystem library.  Yet another case where I have no
// idea what's really going on.
#ifdef __MINGW32__
void *operator new [](unsigned sz) throw (std::bad_alloc)
{
    void *p = malloc(sz ? sz : 1);
    if (!p) {
        throw std::bad_alloc();
    }
    return p;
}

void operator delete [](void *p) throw ()
{
    free(p);
}
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

std::logic_error constructAssertion(
    char const *pFilename,int lineNum,char const *condExpr)
{
    boost::format fmt("Assertion `%1%' failed at line %2% in file %3%");
    return std::logic_error(
        (fmt % condExpr % lineNum % pFilename).str());
}

int getCurrentThreadId()
{
#ifdef __MINGW32__
    return static_cast<int>(GetCurrentThreadId());
#else
    return static_cast<int>(pthread_self());
#endif
}

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
