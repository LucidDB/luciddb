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

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/PseudoUuid.h"
#include "JniPseudoUuid.h"

#include <jni.h>

FENNEL_BEGIN_CPPFILE("$Id$");
#define UUID_LENGTH (net_sf_farrago_fennel_FennelPseudoUuidGenerator_UUID_LENGTH)

static jbyteArray makeJbyteArray(JNIEnv *jEnv, PseudoUuid cppUuid)
{
    jbyteArray uuid = jEnv->NewByteArray(UUID_LENGTH);
    jbyte *uuidBytes = jEnv->GetByteArrayElements(uuid, NULL);

    for(int i = 0; i < UUID_LENGTH; i++) {
        uuidBytes[i] = cppUuid.getByte(i);
    }

    jEnv->ReleaseByteArrayElements(uuid, uuidBytes, 0);

    return uuid;
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_net_sf_farrago_fennel_FennelPseudoUuidGenerator_nativeGenerate(
    JNIEnv *jEnv, jclass)
{
    PseudoUuid cppUuid;
    cppUuid.generate();

    return makeJbyteArray(jEnv, cppUuid);
}

extern "C" JNIEXPORT jbyteArray JNICALL
Java_net_sf_farrago_fennel_FennelPseudoUuidGenerator_nativeGenerateInvalid(
    JNIEnv *jEnv, jclass)
{
    PseudoUuid cppUuid;
    cppUuid.generateInvalid();

    return makeJbyteArray(jEnv, cppUuid);
}

FENNEL_END_CPPFILE("$Id$");

// End JniPseudoUuid.cpp
