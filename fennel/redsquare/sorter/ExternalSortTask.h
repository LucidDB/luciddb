/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004 Red Square
// Copyright (C) 2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

#ifndef Fennel_ExternalSortTask_Included
#define Fennel_ExternalSortTask_Included

FENNEL_BEGIN_NAMESPACE

class ExternalSortStreamImpl;
class ExternalSortRunLoader;

/**
 * ExternalSortTask represents a task entry in the queue serviced by
 * the parallel sorter's thread pool.  Currently, only the run-generation phase
 * is parallelized.  After each run is loaded by the main thread, it is
 * dispatched as a task to be sorted and stored by a thread from the pool.
 */
class ExternalSortTask
{
    /**
     * The stream on behalf of which this task is working.
     */
    ExternalSortStreamImpl &sortStream;

    /**
     * The pre-loaded run to be sorted and stored by this task.
     */
    ExternalSortRunLoader &runLoader;
    
public:
    explicit ExternalSortTask(
        ExternalSortStreamImpl &sortStreamInit,
        ExternalSortRunLoader &runLoaderInit)
        : sortStream(sortStreamInit),
          runLoader(runLoaderInit)
    {
    }
    
    /**
     * Executes this request; this satisfies the ThreadPool Task signature,
     * allowing instances of this class to be submitted as a Task to
     * ThreadPoolScheduler.
     */
    void execute();
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortTask.h
