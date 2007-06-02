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

#ifndef Fennel_TraceSource_Included
#define Fennel_TraceSource_Included

#include <sstream>
#include "fennel/common/TraceTarget.h"

FENNEL_BEGIN_NAMESPACE

/**
 * TraceSource is a common base for all classes which write messages to
 * a TraceTarget.
 */
class TraceSource 
{
    SharedTraceTarget pTraceTarget;
    
    std::string name;

    TraceLevel minimumLevel;

protected:
    /**
     * Constructs a new uninitialized TraceSource.
     */
    explicit TraceSource();

    /**
     * Constructs a new TraceSource.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent,
     * or NULL to disable tracing entirely
     *
     * @param name the name of this source (can be empty string for
     * deferred init)
     */
    explicit TraceSource(
        SharedTraceTarget pTraceTarget,
        std::string name);

public:
    virtual ~TraceSource();
    
    /**
     * For use when initialization has to be deferred until after construction.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent
     *
     * @param name the name of this source
     */
    virtual void initTraceSource(
        SharedTraceTarget pTraceTarget, std::string name);
    
    /**
     * Records a trace message.  Normally only called via FENNEL_TRACE.
     *
     * @param level severity level of event being trace
     *
     * @param message the text of the message
     */
    void trace(TraceLevel level,std::string message) const;

    /**
     * @return true iff tracing is enabled for this source
     */
    bool isTracing() const
    {
        return pTraceTarget.get() ? true : false;
    }
    
    /**
     * Determines whether a particular level is being traced.
     *
     * @param level trace level to test
     * 
     * @return true iff tracing is enabled for the given level
     */
    bool isTracingLevel(TraceLevel level) const
    {
        return level >= minimumLevel;
    }

    /**
     * @return the TraceTarget for this source
     */
    TraceTarget &getTraceTarget() const
    {
        assert(isTracing());
        return *(pTraceTarget.get());
    }

    /**
     * @return the SharedTraceTarget for this source
     */
    SharedTraceTarget getSharedTraceTarget() const
    {
        return pTraceTarget;
    }

    /**
     * Gets the name of this source. Useful to construct nested names for
     * subcomponents that are also TraceSources. 
     * @return the name
     */
    std::string getTraceSourceName() const 
    {
        return name;
    }

    /**
     * Sets the name of this source. Useful to construct dynamic names for
     * fine-grained filtering.
     */
    void setTraceSourceName(std::string const& n) 
    {
        name = n;
    }

    TraceLevel getMinimumTraceLevel() const
    {
        return minimumLevel;
    }

    void disableTracing();
};

/**
 * FENNEL_TRACE can be used from within any class which implements
 * TraceSource. FENNEL_DELEGATE_TRACE is used from a lightweight class that
 * isn't a TraceSource but belongs to and traces as one.
 * msg can be an ostream expression like a << b << c.
 */
#define FENNEL_TRACE(level,msg) FENNEL_DELEGATE_TRACE(level,this,msg)
#define FENNEL_DELEGATE_TRACE(level,tracer,msg) \
do { \
    if (tracer->isTracingLevel(level)) { \
        std::ostringstream oss; \
        oss << msg; \
        tracer->trace(level,oss.str()); \
    } \
} while (false)

// REVIEW jvs 18-Mar-2005:  Why can't we just pass getCurrentThreadId()
// to operator <<?

// return a string id for the current thread
inline char *get_tid(char *tidstr, int cb) 
{
    snprintf(tidstr, cb, "%d", getCurrentThreadId());
    return tidstr;
}

/**
 * FENNEL_TRACE_THREAD can be used from within any class which implements
 * TraceSource.  As FENNEL_TRACE, but also displays the current thread.
 */
#define FENNEL_TRACE_THREAD(level, expr) \
{ \
    char tidstr[32]; \
    FENNEL_TRACE( \
        level, \
        "[thread " << fennel::get_tid(tidstr,sizeof(tidstr)) << "] " << expr); \
}

FENNEL_END_NAMESPACE

#endif

// End TraceSource.h
