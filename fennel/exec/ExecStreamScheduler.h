/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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

#ifndef Fennel_ExecStreamScheduler_Included
#define Fennel_ExecStreamScheduler_Included

#include "fennel/exec/ExecStreamDefs.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamScheduler defines an abstract base for controlling the scheduling
 * of execution streams.  A scheduler determines which execution streams to run
 * and in what order.  For more information, see SchedulerDesign.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ExecStreamScheduler : public boost::noncopyable
{
public:
    virtual ~ExecStreamScheduler();

    /**
     * Starts this scheduler, preparing it to execute streams.
     */
    virtual void start() = 0;

    /**
     * Requests that a specific stream be considered for execution.
     *
     * @param stream the stream to make runnable
     */
    virtual void makeRunnable(ExecStream &stream) = 0;

    /**
     * Asynchronously aborts execution of any scheduled streams
     * and prevents further scheduling.  Returns immediately, not waiting
     * for abort request to be fully processed.
     */
    virtual void abort() = 0;

    /**
     * Shuts down this scheduler, preventing any further streams from
     * being scheduled.
     */
    virtual void stop() = 0;

    /**
     * Creates a new ExecStreamBufAccessor suitable for use with
     * this scheduler.
     *
     * @return new buffer accessor
     */
    virtual SharedExecStreamBufAccessor newBufAccessor() = 0;

    /**
     * Reads data from a stream, first performing any scheduling necessary
     * to make output available.
     *
     * @param stream the stream from which to read
     *
     * @return accessor for output data buffer
     */
    virtual ExecStreamBufAccessor &readStream(
        ExecStream &stream) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamScheduler.h
