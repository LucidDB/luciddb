/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/JavaExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaExcn::JavaExcn(jthrowable javaExceptionInit)
    : FennelExcn("FennelJavaExcn")
{
    javaException = javaExceptionInit;

    // Initialize the msg field to the stack trace. It is necessary to
    // store the stack trace, so that 'what' can hand out a 'const
    // char *'.
    JniEnvAutoRef pEnv;
    jstring s = reinterpret_cast<jstring>(
        pEnv->CallStaticObjectMethod(
            JniUtil::classUtil,
            JniUtil::methUtilGetStackTrace,
            javaException));
    msg = JniUtil::toStdString(pEnv, s);
}

jthrowable JavaExcn::getJavaException() const
{
    return javaException;
}

const std::string& JavaExcn::getStackTrace() const
{
    return msg;
}

void JavaExcn::throwSelf()
{
    throw *this;
}

FENNEL_END_CPPFILE("$Id$");

// End JavaExcn.cpp
