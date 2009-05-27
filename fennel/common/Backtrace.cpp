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
#include "fennel/common/Backtrace.h"
#include "fennel/common/TraceTarget.h"

#include <sstream>

#ifndef __MSVC__
#include <cxxabi.h>
#endif

using std::endl;
using std::ostream;

FENNEL_BEGIN_CPPFILE("$Id$");

Backtrace::~Backtrace()
{
    if (ownbuf) {
        delete[] addrbuf;
    }
}

Backtrace::Backtrace(size_t maxdepth)
    : ownbuf(true), bufsize(maxdepth + 1)
{
#ifndef __MSVC__
    addrbuf = new void * [bufsize];
    depth = backtrace(addrbuf, bufsize);
#endif
}

Backtrace::Backtrace(size_t bufsize, void** buffer)
    : ownbuf(false), bufsize(bufsize)
{
#ifndef __MSVC__
    addrbuf = buffer;
    depth = backtrace(addrbuf, bufsize);
#endif
}

// NOTE: mberkowitz 22-Dec-2005 Not implemented on mingw:
//  could use signal() in lieu of sigaction(), but is there backtrace()?

void Backtrace::print(int fd) const
{
#ifndef __MSVC__
    // skip 1st stack frame (the Backtrace constructor)
    if (depth > 1) {
        backtrace_symbols_fd(addrbuf + 1, depth - 1, fd);
    }
#endif
}

#ifndef __MSVC__
int Backtrace::lookupLibraryBase(
    struct dl_phdr_info *pInfo, size_t size, void *pData)
{
    LibraryInfo *pLibInfo = reinterpret_cast<LibraryInfo *>(pData);
    if (strcmp(pLibInfo->pImageName, pInfo->dlpi_name) == 0) {
        pLibInfo->baseAddress = pInfo->dlpi_addr;
        return 1;
    }
    return 0;
}
#endif

// NOTE jvs 25-Dec-2005:  we could theoretically call the addr2line utility
// from this method to produce source file/line numbers directly, but
// that seems like tempting fate given the handler context in which this
// method may be called.  Instead, we defer that to the
// open/util/bin/analyzeBacktrace utility.
ostream& Backtrace::print(ostream& os) const
{
#ifndef __MSVC__
    char **syms = backtrace_symbols(addrbuf, depth);
    if (syms) {
        // skip 1st stack frame (the Backtrace constructor)
        for (int i = 1; i < depth; i++) {
            // Attempt to demangle C++ function names.
            // Input is of the form "imagename(mangledname+offset) [0xaddr]"

            char *pSymbol = syms[i];
            char *pLeftParen = strchr(pSymbol, '(');
            char *pPlus = strchr(pSymbol, '+');
            char *pLeftBracket = strchr(pSymbol, '[');
            char *pRightBracket = strchr(pSymbol, ']');

            // Special case:  unmangled C names like 'main' can't
            // go through the demangler, so skip anything that
            // doesn't start with an underscore
            if (pLeftParen && (pLeftParen[1] != '_')) {
                pLeftParen = NULL;
            }

            if (!pLeftParen || !pPlus || (pLeftParen > pPlus)
                || !pLeftBracket || !pRightBracket
                || (pLeftBracket > pRightBracket)
                || (pPlus > pLeftBracket))
            {
                // Unrecognized format; dump as is.
                os << pSymbol << endl;
                continue;
            }

            // attempt to determine the library base address if the
            // absolute address is in a region of memory mapped to a .so;
            // lookup "imagename" in list of loaded libraries
            *pLeftParen = 0;
            LibraryInfo libInfo;
            libInfo.baseAddress = 0;
            libInfo.pImageName = pSymbol;
            dl_iterate_phdr(lookupLibraryBase, &libInfo);

            // dump everything up to lparen
            os << pSymbol << '(';

            // restore lparen we zeroed out earlier
            *pLeftParen = '(';

            *pPlus = 0;
            writeDemangled(os, pLeftParen + 1);
            // dump plus and everything up to lbracket
            *pPlus = '+';
            *pLeftBracket = 0;
            os << pPlus;
            *pLeftBracket = '[';
            os << '[';

            // apply .so base address bias if relevant and available
            *pRightBracket = 0;
            unsigned long addr = strtoul(
                pLeftBracket + 1, NULL, 16);
            *pRightBracket = ']';
            if (libInfo.baseAddress) {
                addr -= libInfo.baseAddress;
            }
            os << "0x";
            os << std::hex;
            os << addr;
            os << std::dec;
            os << ']';
            os << endl;
        }
        free(syms);
    }
#endif
    return os;
}

void Backtrace::writeDemangled(std::ostream &out, char const *pMangled)
{
    int status = -3;
    char *pDemangled = NULL;
#ifndef __MSVC__
    pDemangled =
        abi::__cxa_demangle(pMangled, NULL, NULL, &status);
#endif
    if (status || !pDemangled) {
        // non-zero status means demangling failed;
        // use mangled name instead
        out << pMangled;
    } else {
        out << pDemangled;
        free(pDemangled);
    }
}

std::ostream* AutoBacktrace::pstream = &std::cerr;
SharedTraceTarget AutoBacktrace::ptrace;

#ifndef __MSVC__
struct sigaction AutoBacktrace::nextAction[BACKTRACE_SIG_MAX];
#endif

void AutoBacktrace::signal_handler(int signum)
{
#ifndef __MSVC__
    Backtrace bt;
    if (ptrace) {
        std::ostringstream oss;
        oss <<
            "*** CAUGHT SIGNAL " << signum << "; BACKTRACE:" << std::endl;
        oss << bt;
        std::string msg = oss.str();
        if (pstream) {
            *pstream << msg;
        }
        ptrace->notifyTrace("backtrace", TRACE_SEVERE, msg);
    } else if (pstream) {
        *pstream <<
            "*** CAUGHT SIGNAL " << signum << "; BACKTRACE:" << std::endl;
        *pstream << bt;
    }

    // invoke next handler: never coming back, so reset the signal handler
    sigaction(signum, &(nextAction[signum]), NULL);
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

void AutoBacktrace::setTraceTarget(SharedTraceTarget p)
{
    ptrace = p;
}

void AutoBacktrace::install(bool includeSegFault)
{
    // Traps SIGABRT: this handles assert(); unless NDEBUG, permAssert() =>
    // assert(), so that's covered. std::terminate() also => abort().  TODO:
    // trap permAssert() directly.
#ifndef __MSVC__
    installSignal(SIGILL);
    installSignal(SIGABRT);

    if (includeSegFault) {
        installSignal(SIGSEGV);
    }

    installSignal(SIGBUS);
#endif
}

void AutoBacktrace::installSignal(int signum)
{
#ifndef __MSVC__
    permAssert(signum < BACKTRACE_SIG_MAX);
    struct sigaction act;
    struct sigaction old_act;
    act.sa_handler = signal_handler;
    sigemptyset (&act.sa_mask);
    act.sa_flags = 0;
    int rc = sigaction(signum, &act, &old_act);
    if (rc) {
        return;                         // failed
    }
    if (old_act.sa_handler != signal_handler) {
        // installed for the first time
        nextAction[signum] = old_act;
    }
#endif
}

FENNEL_END_CPPFILE("$Id");
// End Backtrace.cpp

