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

int64_t getCurrentThreadId()
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
