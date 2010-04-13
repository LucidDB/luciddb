/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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
