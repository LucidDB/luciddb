/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

#ifndef Fennel_TraceTarget_Included
#define Fennel_TraceTarget_Included

FENNEL_BEGIN_NAMESPACE

/**
 * Trace severity levels.  The values correspond to
 * java.util.logging.Level.
 */
enum TraceLevel {
    TRACE_OFF = 10000,
    TRACE_SEVERE = 1000,
    TRACE_WARNING = 900,
    TRACE_INFO = 800,
    TRACE_CONFIG = 700,
    TRACE_FINE = 500,
    TRACE_FINER = 400,
    TRACE_FINEST = 300
};

/**
 * TraceTarget defines a tracing interface to be implemented by callers to
 * Fennel.
 */
class TraceTarget 
{
public:
    virtual ~TraceTarget();

    /**
     * Receives notification when a trace event occurs.
     *
     * @param source the facility from which the message originated
     *
     * @param level the trace event severity level
     *
     * @param message the text of the message
     */
    virtual void notifyTrace(
        std::string source,
        TraceLevel level,
        std::string message) = 0;

    /**
     * Gets the level at which a particular source should be traced.
     *
     * @param source name of source to be traced
     *
     * @return minimum severity level which should be traced
     */
    virtual TraceLevel getSourceTraceLevel(
        std::string source) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End TraceTarget.h
