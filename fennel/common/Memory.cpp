/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifdef __MSVC__
# include <windows.h>
# include "fennel/common/AtomicCounter.h"
# include "fennel/common/IntrusiveDList.h"
# include "fennel/common/CompoundId.h"
# include "fennel/common/AbortExcn.h"
# include "fennel/common/VoidPtrHash.h"
#else
# include <pthread.h>
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
#ifdef __MSVC__
    return static_cast<int>(GetCurrentThreadId());
#elif defined(__APPLE__)
    return reinterpret_cast<int64_t>(pthread_self());
#else
    return static_cast<int>(pthread_self());
#endif
}

void hexDump(std::ostream &o,void const *v,uint cb,uint cbDone)
{
    boost::io::ios_all_saver streamStateSaver(o);

    PConstBuffer b = (PConstBuffer) v;
    uint cbLine = 16, cbThis;
    o.fill('0');
    for (; cb; cb -= cbThis, cbDone += cbThis) {
        cbThis = std::min(cbLine, cb);
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

// TODO jvs 27-Feb-2009:  move this somewhere else

// force references to some classes which aren't referenced elsewhere
#ifdef __MSVC__
class UnreferencedCommonStructs
{
    AtomicCounter atomicCounter;
    IntrusiveDListNode dlistNode;
    CompoundId compoundId;
    AbortExcn abortExcn;
    VoidPtrHash voidPtrHash;
};
#endif

FENNEL_END_CPPFILE("$Id$");

// End Memory.cpp
