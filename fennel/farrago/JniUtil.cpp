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
#include "fennel/farrago/JniUtil.h"
#include "fennel/farrago/JavaExcn.h"
#include "fennel/common/FennelResource.h"

FENNEL_BEGIN_CPPFILE("$Id$");

JavaVM *JniUtil::pVm = NULL;
jmethodID JniUtil::methGetClassName = 0;
jmethodID JniUtil::methHasNext = 0;
jmethodID JniUtil::methNext = 0;
jmethodID JniUtil::methIterator = 0;
jmethodID JniUtil::methFillBuffer = 0;
jmethodID JniUtil::methGetJavaStreamHandle = 0;
jmethodID JniUtil::methGetIndexRoot = 0;
jmethodID JniUtil::methToString = 0;

AtomicCounter JniUtil::handleCount;

jint JniUtil::init(JavaVM *pVmInit)
{
    pVm = pVmInit;
    JniEnvAutoRef pEnv;
    jclass classClass = pEnv->FindClass("java/lang/Class");
    jclass classObject = pEnv->FindClass("java/lang/Object");
    jclass classCollection = pEnv->FindClass("java/util/Collection");
    jclass classIterator = pEnv->FindClass("java/util/Iterator");
    jclass classJavaTupleStream = pEnv->FindClass(
        "net/sf/farrago/runtime/JavaTupleStream");
    jclass classFennelJavaStreamMap = pEnv->FindClass(
        "net/sf/farrago/fennel/FennelJavaStreamMap");
    methGetClassName = pEnv->GetMethodID(
        classClass,"getName","()Ljava/lang/String;");
    methIterator = pEnv->GetMethodID(
        classCollection,"iterator","()Ljava/util/Iterator;");
    methHasNext = pEnv->GetMethodID(
        classIterator,"hasNext","()Z");
    methNext = pEnv->GetMethodID(
        classIterator,"next","()Ljava/lang/Object;");
    methFillBuffer = pEnv->GetMethodID(
        classJavaTupleStream,"fillBuffer","(Ljava/nio/ByteBuffer;)I");
    methGetJavaStreamHandle = pEnv->GetMethodID(
        classFennelJavaStreamMap,"getJavaStreamHandle",
        "(I)J");
    methGetIndexRoot = pEnv->GetMethodID(
        classFennelJavaStreamMap,"getIndexRoot",
        "(J)J");
    methToString = pEnv->GetMethodID(
        classObject,"toString","()Ljava/lang/String;");
    return jniVersion;
}

JNIEnv *JniUtil::getJavaEnv()
{
    void *pEnv;
    // REVIEW:  need to DetachCurrentThread somewhere?
    jint rc = pVm->AttachCurrentThreadAsDaemon(&pEnv,NULL);
    assert(rc == JNI_OK);
    assert(pEnv);
    return static_cast<JNIEnv *>(pEnv);
}

std::string JniUtil::getClassName(jclass jClass)
{
    JniEnvAutoRef pEnv;
    jstring jString = reinterpret_cast<jstring>(
        pEnv->CallObjectMethod(jClass,methGetClassName));
    assert(jString);
    return toStdString(pEnv,jString);
}

std::string JniUtil::toStdString(JniEnvRef pEnv,jstring jString)
{
    const char *pChars = pEnv->GetStringUTFChars(jString,NULL);
    assert(pChars);
    std::string str(pChars,pEnv->GetStringUTFLength(jString));
    pEnv->ReleaseStringUTFChars(jString,pChars);
    return str;
}

jstring JniUtil::toString(JniEnvRef pEnv,jobject jObject)
{
    return reinterpret_cast<jstring>(
        pEnv->CallObjectMethod(jObject,methToString));
}

uint JniUtil::lookUpEnum(std::string *pSymbols,std::string const &symbol)
{
    for (uint i = 0; ; ++i) {
        assert(pSymbols[i].size());
        if (pSymbols[i] == symbol) {
            return i;
        }
    }
}

jobject JniUtil::getIter(JniEnvRef pEnv,jobject jCollection)
{
    return pEnv->CallObjectMethod(jCollection,methIterator);
}

jobject JniUtil::getNextFromIter(JniEnvRef pEnv,jobject jIter)
{
    if (!pEnv->CallBooleanMethod(jIter,methHasNext)) {
        return NULL;
    }
    return pEnv->CallObjectMethod(jIter,methNext);
}

JniExceptionChecker::~JniExceptionChecker()
{
    jthrowable excn = pEnv->ExceptionOccurred();
    if (excn) {
        throw JavaExcn(excn);
    }
}

JniEnvAutoRef::JniEnvAutoRef()
    : JniEnvRef(JniUtil::getJavaEnv())
{
}

void JniEnvRef::handleExcn(std::exception &ex)
{
    std::string what = ex.what();
    if (what == JavaExcn::RTTI_WHAT_JavaExcn) {
        JavaExcn &javaExcn = static_cast<JavaExcn &>(ex);
        pEnv->Throw(javaExcn.getJavaException());
        return;
    }
    if (what == FennelExcn::RTTI_WHAT_FennelExcn) {
        FennelExcn &fennelExcn = static_cast<FennelExcn &>(ex);
        what = fennelExcn.getMessage();
    } else {
        what = FennelResource::instance().internalError(what);
    }
    // TODO:  need special-case handling for out-of-memory here
    jclass classSQLException = pEnv->FindClass("java/sql/SQLException");
    jstring jMessage = pEnv->NewStringUTF(what.c_str());
    jmethodID constructor = pEnv->GetMethodID(
        classSQLException,"<init>","(Ljava/lang/String;)V");
    jthrowable t = (jthrowable)
        pEnv->NewObject(classSQLException,constructor,jMessage);
    pEnv->Throw(t);
}

FENNEL_END_CPPFILE("$Id$");

// End JniUtil.cpp
