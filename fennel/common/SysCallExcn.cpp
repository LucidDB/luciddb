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
#include "fennel/common/SysCallExcn.h"
#include "fennel/common/FennelResource.h"
#include <errno.h>
#include <sstream>

#ifdef __MINGW32__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

SysCallExcn::SysCallExcn(std::string msgInit)
    : FennelExcn(msgInit)
{
#ifdef __MINGW32__
    DWORD dwErr = GetLastError();
#endif
    std::ostringstream oss;
    oss << msg;
    oss << ": ";
#ifdef __MINGW32__
    oss << "GetLastError() = ";
    oss << dwErr;
#else
    char *pMsg = strerror(errno);
    if (pMsg) {
        oss << pMsg;
    } else {
        oss << "errno = ";
        oss << errno;
    }
#endif
    msg = oss.str();
    msg = FennelResource::instance().sysCallFailed(msg);
}

FENNEL_END_CPPFILE("$Id$");

// End SysCallExcn.cpp
