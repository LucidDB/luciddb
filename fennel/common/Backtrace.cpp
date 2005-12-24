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
#include "fennel/common/Backtrace.h"
#include "fennel/common/TraceTarget.h"

#include <sstream>

using std::endl;
using std::ostream;

FENNEL_BEGIN_CPPFILE("$Id$");

Backtrace::~Backtrace()
{
    if (ownbuf)
        delete[] addrbuf;
}

Backtrace::Backtrace(size_t maxdepth) 
    :ownbuf(true), bufsize(maxdepth + 1)
{
    addrbuf = new (void *)[bufsize];
    depth = backtrace(addrbuf, bufsize);
}

Backtrace::Backtrace(size_t bufsize, void** buffer)
    :ownbuf(false), bufsize(bufsize)
{
    addrbuf = buffer;
    depth = backtrace(addrbuf, bufsize);
}


// NOTE: mberkowitz 22-Dec-2005 Not implemented on mingw:
//  could use signal() in lieu of sigaction(), but is there backtrace()?

void Backtrace::print(int fd) const
{
#ifndef __MINGW32__
    // skip 1st stack frame (the Backtrace constructor)
    if (depth > 1)
        backtrace_symbols_fd(addrbuf+1, depth-1, fd);
#endif
}

// TODO: be more readable. Imitate gdb backtrace: demangle, include source code line
// numbers; omit hex addresses.
ostream& Backtrace::print(ostream& os) const
{
#ifndef __MINGW32__
    char **syms = backtrace_symbols(addrbuf, depth);
    if (syms) {
        // skip 1st stack frame (the Backtrace constructor)
        for (int i = 1; i < depth; i++)
            os << syms[i] << endl;
        free(syms);
    }
#endif
    return os;
}

std::ostream* AutoBacktrace::pstream = &std::cerr;
SharedTraceTarget AutoBacktrace::ptrace;
#ifndef __MINGW32__
struct sigaction AutoBacktrace::nextAction;
#endif

void AutoBacktrace::signal_handler(int signum)
{
#ifndef __MINGW32__
    Backtrace bt;
    if (ptrace) {
        std::ostringstream oss;
        oss << bt;
        std::string msg = oss.str();
        if (pstream)
            *pstream << msg;
        ptrace->notifyTrace("backtrace", TRACE_SEVERE, msg);
    } else if (pstream) {
        *pstream << bt;
    }

    // invoke next handler: never coming back, so reset the signal handler
    sigaction(signum, &nextAction, NULL);
    raise(signum);
#endif
}

void AutoBacktrace::setOutputStream()
{
    pstream = 0;
}

void AutoBacktrace::setOutputStream(ostream& os)
{
    pstream = &os;
}

void AutoBacktrace::setTraceTarget()
{
    ptrace.reset();
}

void AutoBacktrace::setTraceTarget(SharedTraceTarget p)
{
    ptrace = p;
}

void AutoBacktrace::install()
{
    // Traps SIGABRT: this handles assert(); unless NDEBUG, permAssert() => assert(),
    // so that's covered. std::terminate() also => abort().
    // TODO: trap permAssert() directly.
#ifndef __MINGW32__
    struct sigaction act;
    struct sigaction old_act;
    act.sa_handler = signal_handler;
    sigemptyset (&act.sa_mask);
    act.sa_flags = 0;
    int rc = sigaction(SIGABRT, &act, &old_act);
    if (rc) {
        return;                         // failed
    }
    if (old_act.sa_handler != signal_handler) {
        // installed for the first time
        nextAction = old_act;
    }
#endif
}

FENNEL_END_CPPFILE("$Id");
// End Backtrace.cpp

