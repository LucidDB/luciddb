/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_DfsTreeExecStreamScheduler_Included
#define Fennel_DfsTreeExecStreamScheduler_Included

#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraphImpl.h"

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

    /**
     * Finds the next consumer to execute for a given producer.
     *
     * @param graphImpl current stream graph
     * @param graphRep graph representation of current stream graph
     * @param stream currrent execution stream
     * @param edge returns edge to consumer to execute next
     * @param current returns id of consumer to execute next
     * @param nonState state to skip when looking for next consumer
     *
     * @return false if reached sink vertex, else true
     */
    bool findNextConsumer(ExecStreamGraphImpl &graphImpl,
                          const ExecStreamGraphImpl::GraphRep &graphRep,
                          const ExecStream &stream,
                          ExecStreamGraphImpl::Edge &edge,
                          ExecStreamId &current,
                          ExecStreamBufState skipState);

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
        SharedTraceTarget pTraceTarget,
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
