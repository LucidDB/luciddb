/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_JniUtil_Included
#define Fennel_JniUtil_Included

#include "fennel/common/AtomicCounter.h"

#include <jni.h>
#include <locale>

FENNEL_BEGIN_NAMESPACE

/**
 * Helper for JniEnvRef.
 */
class JniExceptionChecker
{
    JNIEnv *pEnv;
    
public:
    explicit JniExceptionChecker(JNIEnv *pEnvInit)
    {
        pEnv = pEnvInit;
    }
    
    ~JniExceptionChecker();

    JNIEnv *operator->() const
    {
        return pEnv;
    }
};

/**
 * Wrapper for a JNIEnv pointer.  This allows us to automatically
 * progagate exceptions from all calls to JNIEnv by using
 * the
 * <a href="http://www.boost.org/libs/smart_ptr/sp_techniques.html#wrapper">
 * call wrapper technique</a> (except without the shared_ptrs, because
 * they're too slow!).
 */
class JniEnvRef
{
    JNIEnv *pEnv;
    
public:
    /**
     * Explicit constructor:  use supplied JNIEnv pointer.
     */
    explicit JniEnvRef(JNIEnv *pEnvInit)
    {
        pEnv = pEnvInit;
    }

    void operator = (JniEnvRef const &other)
    {
        pEnv = other.pEnv;
    }

    JniExceptionChecker operator->() const
    {
        return JniExceptionChecker(pEnv);
    }

    JNIEnv *get()
    {
        return pEnv;
    }

    void handleExcn(std::exception &ex);
};

/**
 * An implementation of JniEnvRef which can be used in contexts where
 * no JNIEnv * is available for initialization.
 */
class JniEnvAutoRef : public JniEnvRef
{
public:
    /**
     * Default constructor:  use thread-local JNIEnv pointer.
     */
    explicit JniEnvAutoRef();

};

/**
 * Static utility methods for dealing with JNI.
 */
class JniUtil 
{
    friend class JniEnvAutoRef;

    /**
     * Loaded JavaVM instance.  For now we can only deal with one at a time.
     */
    static JavaVM *pVm;

    /**
     * java.lang.Class.getName()
     */
    static jmethodID methGetClassName;

    /**
     * java.util.Collection.iterator()
     */
    static jmethodID methIterator;
    
    /**
     * java.util.Iterator.hasNext()
     */
    static jmethodID methHasNext;
    
    /**
     * java.util.Iterator.next()
     */
    static jmethodID methNext;

    /**
     * java.lang.Object.toString()
     */
    static jmethodID methToString;

    /**
     * Gets the JNIEnv for the current thread.  Can be used in contexts
     * where the JNIEnv hasn't been passed down from the native entry point.
     *
     * @return current thread's JNIEnv
     */
    static JNIEnv *getJavaEnv();

public:
    /**
     * Required JNI version.
     */
    static const jint jniVersion = JNI_VERSION_1_2;

    /**
     * Java method JavaPullTupleStream.fillBuffer.
     */
    static jmethodID methFillBuffer;

    /**
     * Java method JavaTupleStream.restart.
     */
    static jmethodID methRestart;

    /**
     * Java method FennelJavaStreamMap.getJavaStreamHandle.
     */
    static jmethodID methGetJavaStreamHandle;

    /**
     * Java method FennelJavaStreamMap.getIndexRoot.
     */
    static jmethodID methGetIndexRoot;


    /**
     * Initializes JNI debugging.
     *
     * @param envVarName name of environment variable used to trigger debugging
     */
    static void initDebug(char const *envVarName);
    
    /**
     * Initializes our JNI support.
     *
     * @param pVm the VM in which we're loaded
     */
    static jint init(JavaVM *pVm);

    /**
     * Calls java.lang.Class.getName().
     *
     * @param jClass the Class of interest
     *
     * @return the fully-qualified class name
     */
    static std::string getClassName(jclass jClass);

    /**
     * Converts a Java string to a C++ string.
     *
     * @param pEnv the current thread's JniEnvRef
     *
     * @param jString the Java string
     *
     * @return the converted C++ string
     */
    static std::string toStdString(JniEnvRef pEnv,jstring jString);

    /**
     * Calls toString() on a Java object.
     *
     * @param pEnv the current thread's JniEnvRef
     *
     * @param jObject object on which to call toString()
     *
     * @return result of toString()
     */
    static jstring toString(JniEnvRef pEnv,jobject jObject);

    /**
     * Calls java.util.Collection.iterator().
     *
     * @param pEnv the JniEnvRef for the current thread
     *
     * @param jCollection the Java collection
     *
     * @return the new Java iterator
     */
    static jobject getIter(JniEnvRef pEnv,jobject jCollection);

    /**
     * Calls java.util.Iterator.hasNext/next()
     *
     * @param pEnv the JniEnvRef for the current thread
     *
     * @param jIter the iterator to advance
     *
     * @return next object from iterator, or NULL if !hasNext()
     */
    static jobject getNextFromIter(JniEnvRef pEnv,jobject jIter);

    /**
     * Looks up an enum value.
     *
     * @param pSymbols array of enum symbols, terminated by empty string
     *
     * @param symbol symbol to look up
     *
     * @return position in array (assert if not found)
     */
    static uint lookUpEnum(std::string *pSymbols,std::string const &symbol);

    /**
     * Counter for all handles opened by Farrago.
     */
    static AtomicCounter handleCount;
};

// NOTE jvs 16-Oct-2004:  This crazy kludge is for problems arising on Linux
// when using multiple JNI libs.  This has to be included in each
// JNI_OnLoad as a workaround (and it must be a macro, not a function).
// Code was taken from _Stl_loc_assign_ids() in stlport/src/locale_impl.cpp.
#define FENNEL_JNI_ONLOAD_COMMON() \
{ \
  _STL::num_get<char, _STL::istreambuf_iterator<char, _STL::char_traits<char> > >::id._M_index                       = 12; \
  _STL::num_get<char, const char*>::id._M_index          = 13; \
  _STL::num_put<char, _STL::ostreambuf_iterator<char, _STL::char_traits<char> > >::id._M_index                       = 14; \
  _STL::num_put<char, char*>::id._M_index                = 15; \
  _STL::time_get<char, _STL::istreambuf_iterator<char, _STL::char_traits<char> > >::id._M_index                      = 16; \
  _STL::time_get<char, const char*>::id._M_index         = 17; \
  _STL::time_put<char, _STL::ostreambuf_iterator<char, _STL::char_traits<char> > >::id._M_index                      = 18; \
  _STL::time_put<char, char*>::id._M_index               = 19; \
}

FENNEL_END_NAMESPACE

#endif

// End JniUtil.h
