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

#ifndef Fennel_JavaTraceTarget_Included
#define Fennel_JavaTraceTarget_Included

#include "fennel/common/TraceTarget.h"
#include "fennel/common/StatsTarget.h"
#include "fennel/farrago/JniUtil.h"

FENNEL_BEGIN_NAMESPACE

/**
 * JavaTraceTarget implements TraceTarget by calling back into the 
 * java.util.logging facility.  It also implements StatsTarget by
 * converting performance counter updates into trace events which
 * are published inside of Java.
 */
class JavaTraceTarget : public TraceTarget, public StatsTarget
{
    /**
     * net.sf.farrago.util.NativeTrace object to which trace messages should be
     * written.
     */
    jobject javaTrace;

    /**
     * Method NativeTrace.trace.
     */
    jmethodID methTrace;
    
    /**
     * Method NativeTrace.getSourceTraceLevel.
     */
    jmethodID methGetSourceTraceLevel;
    
public:
    explicit JavaTraceTarget();

    explicit JavaTraceTarget(
        jobject javaTraceInit, jmethodID methTraceInit, 
        jmethodID methGetSourceTraceLevelInit);

    virtual ~JavaTraceTarget();

    // implement TraceTarget
    virtual void notifyTrace(
        std::string source,TraceLevel level,std::string message);
    virtual TraceLevel getSourceTraceLevel(std::string source);
    
    // implement StatsTarget
    virtual void beginSnapshot();
    virtual void endSnapshot();
    virtual void writeCounter(std::string name,uint value);
    virtual void onTimerStart();
    virtual void onTimerStop();
};

FENNEL_END_NAMESPACE

#endif

// End JavaTraceTarget.h
