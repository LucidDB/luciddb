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
    TraceTarget *pTraceTarget;
    
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
        TraceTarget *pTraceTarget,
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
    void initTraceSource(TraceTarget *pTraceTarget,std::string name);
    
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
        return pTraceTarget ? true : false;
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
        return *pTraceTarget;
    }

    TraceLevel getMinimumTraceLevel() const
    {
        return minimumLevel;
    }

    void disableTracing();
};

/**
 * FENNEL_TRACE can be used from within any class which implements
 * TraceSource.  msg can be an ostream expression like a << b << c.
 */
#define FENNEL_TRACE(level,msg) \
do { \
    if (isTracingLevel(level)) { \
        std::ostringstream oss; \
        oss << msg; \
        trace(level,oss.str()); \
    } \
} while (false)

FENNEL_END_NAMESPACE

#endif

// End TraceSource.h
