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

#ifndef Fennel_Backtrace_Included
#define Fennel_Backtrace_Included

#include <ostream>
#include <stdlib.h>

#ifndef __MINGW32__
#include <signal.h>
#include <execinfo.h>
#include <link.h>
#endif


FENNEL_BEGIN_NAMESPACE

/**
 * A Backtrace represents a backtrace of the run-time stack.
 * The constructor wraps up the backtrace of the current thread at the point of construction.
 * A Backtrace object can be printed to an ostream.
 */
class Backtrace
{
    size_t depth;                       // actual depth
    const bool ownbuf;                  // the object allocated the addrbuf
    const size_t bufsize;
    void** addrbuf;                     // [bufsize]

#ifndef __MINGW32__
    struct LibraryInfo 
    {
        ElfW(Addr) baseAddress;
        char const *pImageName;
    };

    static int lookupLibraryBase(struct dl_phdr_info *, size_t, void *);
#endif

public:
    /**
     * Captures the backtrace at the point of construction.
     * The Backtrace object allocates its own address buffer.
     * @param maxdepth maximum depth of backtrace
     */
    Backtrace(size_t maxdepth = 32);

    /**
     * Captures the backtrace at the point of construction. The caller provides an
     * address buffer, probably on the stack.
     * The buffer should contain an extra item to refer to the Backtrace constructor itself.
     * @param bufsize buffer size, in words (sizeof(void*) = 1 word)
     * @param buffer an array of BUFSIZE (void *) entries.
     */
    Backtrace(size_t bufsize, void** buffer);

    ~Backtrace();

    /** prints the backtrace in human readable form. Skips the Backtrace constructor */
    std::ostream& print(std::ostream&) const;

    /** prints the backtrace to a unix file descriptor: for use when out of memory */
    void print(int fd) const;

    /**
     * Attempts to demangle a C++ name and write it to an ostream.
     *
     * @param out receives demangled name
     *
     * @param pMangled name to be demangled
     */
    static void writeDemangled(
        std::ostream &out, char const *pMangled);
};

inline std::ostream& operator << (std::ostream& os, const Backtrace& bt)
{
    return bt.print(os);
}

/**
 * AutoBacktrace provides a handler that intercepts fatal errors, prints a backtrace,
 * and passes on the fatal error to other handlers.
 * The backtrace handler has global scope.
 * Fatal errors include abort(), assert(), fennel permAssert(), and runaway C++ exceptions.
 */
class AutoBacktrace {
    static std::ostream* pstream;
    static SharedTraceTarget ptrace;
    static void signal_handler(int signum);
#ifndef __MINGW32__
#define BACKTRACE_SIG_MAX 32
    static struct sigaction nextAction[BACKTRACE_SIG_MAX];
#endif

    AutoBacktrace() {}                  // hide constructor
    
    static void installSignal(int signum);
public:
    /**
     * Installs backtrace on error; default output is to stderr.
     *
     *<p>
     *
     * NOTE jvs 25-Dec-2005:  for more flexibility, we could
     * allow the caller to specify a sigset_t.
     *
     * @param includeSegFault if true, SIGSEGV is included in the
     * set of signals causing a backtrace; if false, it is omitted
     * (required in environments such as a Java VM where spurious
     * segfaults may be signalled but handled as part of normal
     * operation)
     */
    static void install(bool includeSegFault = true);

    /**
     * Sets an ostream to which the backtrace is written.
     *
     * @param outStream receives backtrace if signal is handled
     */
    static void setOutputStream(std::ostream &outStream);
    
    /**
     * Unsets a target stream for backtrace.
     */
    static void setOutputStream();

    /**
     * Sets a TraceTarget to which the backtrace is written,
     * independent of setOutputStream.
     *
     * @param pTraceTarget receives backtrace if signal is handled;
     * if singular (the default), resets target to nothing
     */
    static void setTraceTarget(
        SharedTraceTarget pTraceTarget = SharedTraceTarget());
};

FENNEL_END_NAMESPACE
    
#endif
    
// End Backtrace.h
