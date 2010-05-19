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
#include "fennel/farrago/JavaErrorTarget.h"
#include "fennel/farrago/JavaExcn.h"

#include "boost/lexical_cast.hpp"

FENNEL_BEGIN_CPPFILE("$Id$");

#define JAVAERRORTARGET_TYPE_STR ("JavaErrorTarget")

JavaErrorTarget::JavaErrorTarget(jobject javaErrorInit)
{
    JniEnvAutoRef pEnv;

    JniUtil::incrementHandleCount(JAVAERRORTARGET_TYPE_STR, this);
    javaError = pEnv->NewGlobalRef(javaErrorInit);

    jclass classErrorTarget = pEnv->FindClass(
        "net/sf/farrago/fennel/FennelJavaErrorTarget");
    methNotifyError = pEnv->GetMethodID(
        classErrorTarget,
        "handleRowError",
        "(Ljava/lang/String;ZLjava/lang/String;Ljava/nio/ByteBuffer;I)Ljava/lang/Object;");
}

JavaErrorTarget::~JavaErrorTarget()
{
    JniEnvAutoRef pEnv;

    pEnv->DeleteGlobalRef(javaError);
    JniUtil::decrementHandleCount(JAVAERRORTARGET_TYPE_STR, this);
    javaError = NULL;
}

void JavaErrorTarget::notifyError(
    const std::string &source,
    ErrorLevel level,
    const std::string &message,
    void *address,
    long capacity,
    int index)
{
    JniEnvAutoRef pEnv;

    // NOTE jvs 21-Aug-2007:  use ref reapers here since this
    // may be called over and over before control returns to Java

    jstring javaSource = pEnv->NewStringUTF(source.c_str());
    JniLocalRefReaper javaSourceReaper(pEnv, javaSource);
    jboolean javaIsWarning = (level == ROW_WARNING) ? true : false;
    jstring javaMessage = pEnv->NewStringUTF(message.c_str());
    JniLocalRefReaper javaMessageReaper(pEnv, javaMessage);
    jobject javaByteBuffer =
        pEnv->NewDirectByteBuffer(address, capacity);
    JniLocalRefReaper javaByteBufferReaper(pEnv, javaByteBuffer);
    jint javaIndex = index;

    pEnv->CallObjectMethod(
        javaError, methNotifyError,
        javaSource, javaIsWarning, javaMessage,
        javaByteBuffer, javaIndex);
}

FENNEL_END_CPPFILE("$Id$");

// End JavaErrorTarget.cpp
