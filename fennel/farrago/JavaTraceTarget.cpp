/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/farrago/JavaTraceTarget.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaTraceTarget::JavaTraceTarget(jobject javaTraceInit)
{
    JniEnvAutoRef pEnv;
    javaTrace = javaTraceInit;
    jclass classNativeTrace = pEnv->FindClass(
        "net/sf/farrago/util/NativeTrace");

    methTrace = pEnv->GetMethodID(
        classNativeTrace,"trace",
        "(Ljava/lang/String;ILjava/lang/String;)V");
    methGetSourceTraceLevel = pEnv->GetMethodID(
        classNativeTrace,"getSourceTraceLevel",
        "(Ljava/lang/String;)I");
}

JavaTraceTarget::~JavaTraceTarget()
{
    // NOTE:  we don't really own javaTrace, so don't delete it here
    javaTrace = NULL;
}

void JavaTraceTarget::notifyTrace(
    std::string source,TraceLevel level,std::string message)
{
    JniEnvAutoRef pEnv;
    jstring javaSource = pEnv->NewStringUTF(source.c_str());
    jstring javaMessage = pEnv->NewStringUTF(message.c_str());
    pEnv->CallVoidMethod(javaTrace,methTrace,javaSource,level,javaMessage);
}

TraceLevel JavaTraceTarget::getSourceTraceLevel(std::string source)
{
    JniEnvAutoRef pEnv;
    jstring javaSource = pEnv->NewStringUTF(source.c_str());
    int level = pEnv->CallIntMethod(
        javaTrace,methGetSourceTraceLevel,javaSource);
    return static_cast<TraceLevel>(level);
}

FENNEL_END_CPPFILE("$Id$");

// End JavaTraceTarget.cpp
