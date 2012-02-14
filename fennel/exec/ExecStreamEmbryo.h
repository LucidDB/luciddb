/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_ExecStreamEmbryo_Included
#define Fennel_ExecStreamEmbryo_Included

#include <boost/bind.hpp>
#include <boost/function.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamEmbryo encapsulates the "embryonic" state of an ExecStream in
 * which it has been created, and thus has a definite type, and also has all of
 * its parameters defined; but its prepare() method has not yet been called.
 * The embryo can be put in "cold storage" until everything is ready for its
 * quickening.
 *
 *<p>
 *
 * This form is necessary because all of the streams constituting an
 * ExecStreamGraph must be created and added to the graph before any of them
 * can be prepared.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ExecStreamEmbryo
{
    /**
     * Unprepared stream.
     */
    SharedExecStream pStream;

    /**
     * Params to use for preparing stream.
     */
    SharedExecStreamParams pParams;

    /**
     * Bound function for preparing stream.
     */
    boost::function<void ()> prepareFunction;

public:

    /**
     * Initializes reference to stream and parameters for preparing it.  Be
     * sure to invoke this with parameters of most-specific type or the correct
     * prepare() overload will not be called.
     *
     * @param pStreamInit newly allocated ExecStream implementation;
     * ExecStreamEmbryo takes ownership of the supplied pointer,
     * so it must be dynamically allocated
     *
     * @param paramsInit ExecStreamParams implementation; ExecStreamEmbryo
     * does not take ownership of the supplied reference, so normally
     * it should be stack-allocated
     */
    template<class S, class P>
    inline void init(S *pStreamInit, P const &paramsInit)
    {
        pStream.reset(pStreamInit, ClosableObjectDestructor());
        P *pParamCopy = new P(paramsInit);
        pParams.reset(pParamCopy);
        prepareFunction = boost::bind(
            &S::prepare, pStreamInit, boost::ref(*pParamCopy));
    }

    inline SharedExecStream &getStream()
    {
        return pStream;
    }

    inline SharedExecStreamParams &getParams()
    {
        return pParams;
    }

    /**
     * Executes bound prepare method.
     */
    inline void prepareStream()
    {
        prepareFunction();
    }
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamEmbryo.h
