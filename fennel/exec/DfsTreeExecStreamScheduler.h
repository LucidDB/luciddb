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

#ifndef Fennel_DfsTreeExecStreamScheduler_Included
#define Fennel_DfsTreeExecStreamScheduler_Included

#include "fennel/exec/ExecStreamScheduler.h"

FENNEL_BEGIN_NAMESPACE

class ExecStreamGraphImpl;
    
/**
 * DfsTreeExecStreamScheduler is a reference implementation of
 * the ExecStreamScheduler interface.
 * See SchedulerDesign for more details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class DfsTreeExecStreamScheduler : public ExecStreamScheduler
{
    volatile bool aborted;
    
    SharedExecStreamGraph pGraph;

public:
    /**
     * Constructs a new scheduler.
     *
     * @param pTraceTarget the TraceTarget to which messages will be sent,
     * or NULL to disable tracing entirely
     *
     * @param name the name to use for tracing this scheduler
     */
    explicit DfsTreeExecStreamScheduler(
        TraceTarget *pTraceTarget,
        std::string name);
    
    virtual ~DfsTreeExecStreamScheduler();

    // implement the ExecStreamScheduler interface
    virtual void addGraph(SharedExecStreamGraph pGraph);
    virtual void removeGraph(SharedExecStreamGraph pGraph);
    virtual void start();
    virtual void makeRunnable(ExecStream &stream);
    virtual void abort(ExecStreamGraph &graph);
    virtual void stop();
    virtual ExecStreamBufAccessor &readStream(ExecStream &stream);
};

FENNEL_END_NAMESPACE

#endif

// End DfsTreeExecStreamScheduler.h
