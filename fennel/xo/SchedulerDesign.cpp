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

FENNEL_BEGIN_CPPFILE("$Id$");

/**
   
<h3>Overview</h3>

The Fennel XO scheduler is responsible for determining which execution streams
to run and in what order (or with what parallelization).  In addition, the
scheduler is responsible for the actual invocation of XO execution methods.  (A
more modular design would separate scheduling from execution, but there is
currently no justification for the extra complexity this separation would
introduce.)

<h3>Interfaces</h3>

The UML static structure diagram below illustrates the relevant relationships:

<hr>
\image html SchedulerInterfaces.gif
<hr>

An ExecutionStreamScheduler may be dedicated per ExecutionStreamGraph, or may
be responsible for multiple graphs, depending on the scheduling granularity
desired.  Each graph consists of multiple vertex instances of ExecutionStream,
with ExecutionStreamBuffer edges representing dataflow.  Each buffer is
assigned a single producer/consumer stream pair.  Hence, the number of buffers
accessible from a stream is equal to its vertex degree in the graph.

<p>

When the scheduler decides that a particular stream should run, it invokes its
ExecutionStream::execute method, passing a reference to an instance of
ExecutionStreamQuantum to limit the amount of data processed.  The exact
interpretation of the quantum is up to the stream implementation.  The stream's
response is dependent on the states of its incident buffers, which typically
change as a side-effect of the execution.  This protocol is specified in more
detail later on.

<p>

It is up to a scheduler implementation to keep track of the <em>runnable</em>
status of each stream.  This information is based on the result of
each call to ExecutionStream::execute (including the return code and incident
buffer state changes), and can be queried via
ExecutionStreamScheduler::isRunnable.  In addition,
ExecutionStreamScheduler::makeRunnable can be invoked explicitly to alert the
scheduler to an externally-driven status change (e.g. arrival of a network
packet or completion of asynchronous disk I/O requested by a stream).  A stream
may call ExecutionStreamScheduler::setTimer to automatically become runnable
periodically.

<p>

Where possible, execution streams should be implemented as non-blocking to
ensure scheduling flexibility.  Streams that may block must return true from
the ExecutionStream::mayBlock method; schedulers may choose to use threading to
prevent such streams from blocking the execution of the scheduler itself.

<h3>Scheduler Extensibility</h3>

Most of the interfaces involved in scheduling are polymorphic:

<ul>

<li>Derived classes of ExecutionStreamScheduler implement different scheduling
policies (e.g. sequential vs. parallel, or full-result vs. top-N).

<li>Derived classes of ExecutionStream implement different kinds of data
processing (e.g. table scan, sorting, join, etc).

<li>Derived classes of ExecutionStreamBuffer may keep track of
scheduler-specific buffer state (e.g. a timestamp for implementing some
fairness policy).  Because ExecutionStreamScheduler acts as a factory for
buffer instances, it can guarantee that all of the buffers it manages are of
the correct type.

<li>Derived classes of ExecutionStreamQuantum may provide scheduler-specific
quantization limits to streams that can understand it.  This requires the code
that constructs an ExecutionStreamGraph to be aware of the kind of scheduler
that will be used to execute it.  Streams which only understand the generic
quantization can be used in any graph.

</ul>

<h3>Top-level Flow</h3>

The flow for instantiation and scheduling of a single stream graph is as
follows:

<ol>

<li>The caller instantiates an ExecutionStreamGraph together with the
ExecutionStream instances it contains.  (See ExecutionStreamBuilder and
ExecutionStreamFactory.)

<li>The caller instantiates an ExecutionStreamScheduler and associates the
graph with it (TODO methods for this).  Internally, the
ExecutionStreamScheduler allocates one instance of ExecutionStreamBuffer for
each dataflow edge in the graph (as well as one for each input or output to the
graph) and notifies the incident streams of its existence.  Once a graph and
its streams have been associated with a scheduler in this way, they may never
be moved to another scheduler.  Buffers start out in state EXECBUF_IDLE, with
pStart, pEnd, and nTuples all zero.

<li>The caller invokes ExecutionStreamGraph::open().

<li>The caller invokes ExecutionStreamScheduler::start().

<li>The caller invokes ExecutionStreamScheduler::readStreamBuffer to read query
output data, and/or ExecutionStreamScheduler::writeStreamBuffer to write query
input data.  This is repeated indefinitely.  Optionally, the caller may invoke
ExecutionStreamScheduler::abort from another thread while a read or write is in
progress; the abort call returns immediately, but the abort request may take
some time to be fulfilled.

<li>The caller invokes ExecutionStreamScheduler::stop().  This is synchronous
and does not return until all execution in other scheduler threads has
completed.

<li>The caller invokes ExecutionStreamGraph::close().

</ol>

<h3>ExecutionStream::execute Protocol</h3>

When the scheduler invokes a stream, the possible responses are enumerated by
ExecutionStreamResult:

<ul>

<li>EXECRC_NEED_INPUT: the invoked stream could not perform any work because it
needed data from at least one of its inputs.  Before return, the invoked stream
is responsible for changing the state of the corresponding input buffers to
EXECBUF_NEED_PRODUCTION.  If the buffer's dataflow mode is CONSUMER_PROVISION,
then the invoked stream is also responsible for setting the buffer pStart and
pEnd pointers to the memory area which the producer should fill with tuples;
otherwise these pointers should be zeroed.

<li>EXECRC_NEED_OUTPUTBUF: the invoked stream could not perform any work
because it needed space in at least one of its output buffers which has not yet
been consumed by the corresponding downstream XO (the output buffer was
still in state EXECBUF_NEED_CONSUMPTION).

<li>EXECRC_EOS: end of stream has been detected; no more data
will ever be produced by the invoked stream.  Before return, the invoked stream
is responsible for setting the state of all of its output buffers to
EXECBUF_EOS, and resetting their pStart/pEnd/nTuples fields to zero.

<li>EXECRC_SUCCESS: the stream produced at least one tuple in one of its
output buffers.  The invoked stream is responsible for a number of state
changes in this case:

    <ul>

    <li>Any partially consumed input buffer should have its nTuples field
    decremented and pStart advanced accordingly.

    <li>Any fully consumed input buffer should have its nTuples field reset to
    0.  The buffer's state field should change to either EXECBUF_IDLE (in which
    case pStart/pEnd should be zeroed) or EXECBUF_NEED_PRODUCTION (with the
    same rules for pStart/pEnd as for EXECRC_NEED_INPUT).

    <li>An output buffer in which tuples were produced should change state to
    EXECBUF_NEED_CONSUMPTION with a nonzero value in its nTuples field.  If the
    buffer's dataflow mode is PRODUCER_PROVISION, then pStart/pEnd should
    reference the area of the producer's memory filled with tuples; otherwise,
    the pEnd pointer set by the consumer should be decremented to the end of
    the last tuple written into the consumer's memory by the invoked stream.

    </ul>

</ul>

From the point of view of a buffer, this means the possible transitions are as
follows:

<hr>
\image html SchedulerBufferStatechart.gif
<hr>

Note that multi-threaded schedulers must guarantee that both streams adjacent
to a buffer are never running simultaneously; this eliminates the need to
synchronize fine-grained access to buffer state.

<h3>Example Graph</h3>

<h3>DfsTreeExecutionStreamScheduler</h3>

A reference implementation of the ExecutionStreamScheduler interface is
provided by the DfsTreeExecutionStreamScheduler class, which is suitable as a
no-frills implementation of a traditional lazy non-parallel query execution.

The algorithm used by this scheduler is as follows:

<ol>

<li>The start method asserts that each of the graphs associated with the
scheduler is a forest of trees (each stream has at most one consumer, and no
cycles exist).

<li>The readStreamBuffer method is the real entry point for synchronous
traversal of the tree.  (writeStreamBuffer is not supported.)

<li>The scheduler asserts that the stream with which readStreamBuffer was
invoked has no consumers (i.e. it is the root of a tree) and has output buffer
provision mode PRODUCER_PROVISION.  The scheduler also asserts that the root
output buffer's mode is not EXECBUF_NEED_CONSUMPTION.  A <em>current
stream</em> pointer is set to reference this stream, and the stream's state is
set to EXECBUF_NEED_PRODUCTION.

<li>VISIT_VERTEX:

<li>If the scheduler's <em>abort</em> flag has been set asynchronously, the
traversal terminates.

<li>The scheduler iterates over each of the input buffers of the current stream
in order.  If it encounters one having state EXECRC_NEED_INPUT, the scheduler
updates its current stream pointer to the corresponding producer stream, and
loops back to label VISIT_VERTEX.

<li>The scheduler invokes the current stream's execute method, and asserts that
the return code was not EXECRC_NEED_OUTPUTBUF (impossible due to
lazy evaluation order).

<li>If the return code was EXECRC_SUCCESS or EXECRC_EOS, then the scheduler
updates the current stream pointer to reference the current stream's parent and
then loops back to label VISIT_VERTEX.  If the current stream has no parent,
the traversal terminates instead.

<li>The scheduler asserts that the return code was EXECRC_NEED_INPUT.  It then
iterates over each of the input buffers of the current stream, asserting that
at least one of them has state EXECBUF_NEED_PRODUCTION (otherwise the scheduler
is stuck in an infinite loop).  The scheduler then loops back to label
VISIT_VERTEX.

</ol>

DfsTreeExecutionStreamScheduler does not support asynchronous runnability
changes (makeRunnable and setTimer).

<p>

TBD:  additional assertions; re-entrancy issues for Java XO's.

<h3>Example Execution</h3>

 */
struct SchedulerDesign 
{
    // NOTE:  dummy class for doxygen
};


FENNEL_END_CPPFILE("$Id$");

// End SchedulerDesign.cpp
