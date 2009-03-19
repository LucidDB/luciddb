/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 SQLstream, Inc.
// Copyright (C) 2008-2008 LucidEra, Inc.
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
#include "fennel/farrago/JavaThreadTracker.h"
#include "fennel/farrago/JniUtil.h"
#include "fennel/farrago/JavaExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void JavaThreadTracker::onThreadStart()
{
    JniEnvAutoRef pEnv;
    // We want to stay attached for the duration of the timer thread,
    // so suppress detach here and do it explicitly in onThreadEnd
    // instead.  See comments on suppressDetach about the need for a
    // cleaner approach to attaching native-spawned threads.
    pEnv.suppressDetach();
}

void JavaThreadTracker::onThreadEnd()
{
    JniUtil::detachJavaEnv();
}

FennelExcn *JavaThreadTracker::cloneExcn(std::exception &ex)
{
    JavaExcn *pJavaExcn = dynamic_cast<JavaExcn *>(&ex);
    if (!pJavaExcn) {
        return ThreadTracker::cloneExcn(ex);
    }
    return new JavaExcn(pJavaExcn->getJavaException());
}

FENNEL_END_CPPFILE("$Id$");

// End JavaThreadTracker.cpp
