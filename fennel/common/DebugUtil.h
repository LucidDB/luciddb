/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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

#ifndef Fennel_DebugUtil_Included
#define Fennel_DebugUtil_Included

FENNEL_BEGIN_NAMESPACE

// a class to hold some debugging routines
struct DebugUtil {
    /// Pauses the program so the debugger can attach; flag HUP means resume on
    /// SIGHUP, not SIGCONT.
    static void waitForDebugger(bool hup);
};

FENNEL_END_NAMESPACE
#endif
// End DebugUtil.h
