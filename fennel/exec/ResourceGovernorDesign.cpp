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

FENNEL_BEGIN_CPPFILE("$Id$");

/**
   
<h3>Overview</h3>

The Fennel execution stream resource governor is responsible for
globally allocating resources across all execution stream graphs running within
a single Fennel instantiation.
As background, please read the ExecStreamDesign first.

<p>

The resource governor decides how much of each resource to allocate to each
stream graph, as well as how much to allocate to each execution stream within
the graph.  Criteria for deciding how to allocate resources include:

<ul>

<li>Total resource availability across the entire system versus the amount of
resources still available for assignment

<li>System-wide resource knob settings

<li>Minimum versus optimum resource requirements specified by each execution 
stream within the stream graph

<li>The level of accuracy of each stream's specified optimum
resource requirements

</ul>

<h3>Resource Knobs</h3>

Resource knob settings are stored in an ExecStreamResourceKnobs structure.
Currently, two knobs are supported:

<ul>

<li>ExecStreamResourceKnobs::expectedConcurrentStatements - This specifies the
maximum number of
stream graphs (which correspond to SQL statements) that are expected to be
running concurrently at any given time.

<li>ExecStreamResourceKnobs::cacheReservePercentage - This reserves a portion
of the global data cache that the resource governor cannot use when assigning
cache pages to individual stream graphs.

</ul>

When the resource governor is initialized, it is provided with default
settings for these knobs, as well as the current resource availability.
The resource governor will utilize this information in its allocation policy.
The knobs can be dynamically modified at runtime.

<h3>Resource Requirements</h3>

As described in ExecStreamHowTo, if a stream requires heavyweight resources,
it must override the ExecStream::getResourceRequirements method.
There are two versions of this method.
The first specifies two parameters -- minimum and optimum resource requirements.
Both are represented using ExecStreamResourceQuantity structures that the
stream's getResourceRequirements method will set and pass back
to the caller.
The structure allows the number of threads and cache pages to be specified.
The minimum requirement setting indicates the minimum resources the stream
needs to run to completion without errors, whereas the optimum represents the
amount the stream needs to run efficiently.
For example, if a stream implements sorting, an efficient
execution would do a full in-memory sort.
But you do not necessarily need
to do a full in-memory sort for the sort to run to completion if the stream
supports writing temporary results to disk.
Therefore, the minimum setting, in this case, would be some lower value.

<p>

In some cases, it may not be possible for the stream to specify an exact
number for its optimum requirement.
For example, the stream may have access to statistics that will allow it to
estimate a value for its optimum requirement.
However, the estimate may not be completely trustworthy if it is based on
out-dated statistics.
Or even worse, if no statistics are available, the stream may not even be able
to come up with any estimate at all.

<p>

In these cases, the stream should use the second version of
ExecStream::getResourceRequirements, which has a third parameter.
That third parameter receives an ExecStreamResourceSettingType.
An ExecStreamResourceSettingType is an enumerated type that has one of the
following values:

<ul>

<li>EXEC_RESOURCE_ACCURATE - The optimum requirement is accurate. 
The resource governor will not grant any additional resources beyond the
specified optimum.
This is the default that is used when the third parameter is not specified.

<li>EXEC_RESOURCE_ESTIMATE - The optimum requirement is an estimate and
may not be trustworthy.

<li>EXEC_RESOURCE_UNBOUNDED - The optimum requirement is unknown.

</ul>

In the case of an estimate setting, the resource governor
will try to allocate extra resources above the optimum setting.
In the case of an unbounded setting, the resource governor will allocate as
much as possible to the stream.
In both cases, the amount to be allocated is subject to the amount available to
the overall stream graph and the amount required by other streams in the
graph.
Greater weight will be given to the streams with unbounded settings.
Therefore, memory intensive streams that cannot set an optimum requirement
should use the unbounded setting. 
Less memory intensive streams that
cannot set an optimum should use the estimate setting with a reasonable guess
for the optimum setting.

<p>
Using the resource requirement settings provided by each stream, the resource
governor can decide how much to allocate to each stream.
Once it has decided, it calls ExecStream::setResourceAllocation to let each
stream know much it has been allocated.

<h3>Interfaces</h3>

The ExecStreamGovernor class supports the following public methods:

<ul>

<li>ExecStreamGovernor::ExecStreamGovernor - This is the constructor for the
resource governor.
Initial knob settings and resource availability are passed into the constructor.

<li>ExecStreamGovernor::setResourceKnob - If a resource knob is dynamically
changed, this method needs to be called to inform the resource governor of
the new knob setting, so it can recompute the amount of resources it can
allocate to future stream graph requests.
It also ensures that the new setting does not interfere with current resource
assignments.
For example, cacheReservePercentage cannot be set to a value such that the new
number of pages in reserve conflicts with the number of pages already assigned.

<li>ExecStreamGovernor::setResourceAvailability - If resource availability is
dynamically changed, this method needs to be called to inform the resource
governor of the new availability, so it can recompute the amount of resources
it can allocate to future stream graph requests.
For example, the total number of cache pages cannot be set to a value lower
than the number of pages currently assigned.

<li>ExecStreamGovernor::requestResources - The scheduler calls this method to
request resources on behalf of a stream graph.
This is the main component of the resource governor, where resource
allocations are determined.

<li>ExecStreamGovernor::returnResources - When a stream graph is closed, this
method will be called to return each stream's assigned resources so the
resource governor can assign them to new stream graphs.

</ul>

Both ExecStreamGovernor::requestResources and
ExecStreamGovernor::returnResources are polymorphic.
Therefore, different implementations of resource governors can be built,
supporting different allocation policies.

<h3>SimpleExecStreamGovernor</h3>

A reference implementation of the ExecStreamGovernor interface is provided by
the SimpleExecStreamGovernor class.
As its name implies, it is a basic implementation.
It has the following characteristics:

<ul>

<li>It only manages and allocates cache data pages.

<li>Requests fail when insufficient resources are available for even a minimum
allocation.

<li>The allocation policy is fixed and based purely on the resource knob
settings and resource availability.

<li>Once resources are assigned, they are not taken away.

<li>It assumes that all streams are concurrently active and therefore cannot
share resources.
In other words, it has no knowledge of the fact that some portions of the stream
graph may be mutually exclusive.

</ul>

Using the total number of cache pages available, the resource governor will
set aside a reserve based on ExecStreamResourceKnobs::cacheReservePercentage.
The remainder will be divided by
ExecStreamResourceKnobs::expectedConcurrentStatements to yield
ExecStreamGovernor::perGraphAllocation.
This computed value serves as a reference amount in determining how many
cache pages to assign to each graph.
If during the course of execution, fewer pages than
ExecStreamGovernor::perGraphAllocation are available, then the total remaining
number of cache pages becomes the reference amount for allocation.

<p>

When SimpleExecStreamGovernor::requestResources is called, the resource
governor will total both the minimum and optimum requirements of each stream
in the graph.
In the case where the optimum setting is EXEC_RESOURCE_UNBOUNDED,
the resource governor sets the optimum requirement for the stream to
the minimum requirement plus the reference amount.
As you will see further below, this ensures that priority is given to
these streams.
These numbers are used by the SimpleExecStreamGovernor as follows:

<ol>

<li>If the total minimum cache page requirements specified by each execution
stream is greater than the total number of pages currently available, then an
exception is raised.
The memory currently available excludes the cache pages in reserve and any
pages already assigned to other stream graphs.

<li>If the total minimum cache pages is greater than or equal to the reference
amount but less than or equal to the total number of pages currently
available, then the resource governor assigns each stream
its specified minimum.

<li>If the total optimum cache pages is greater than the reference amount,
then the resource governor assigns each stream its minimum and
divides up the difference
between the reference amount and the total minimum using the following formula:

<pre><code>
number of pages to allocate to stream X =
    minimum for X +
    (reference amount - total minimum) *
        (sqrt((optimum for X) - (minimum for X)) /
        sum of the sqrt of the differences between the optimum and minimum
            settings for each stream
</code></pre>

<li>If the total optimum cache pages is less than or equal to the reference
amount and all streams have specified an
EXEC_RESOURCE_ACCURATE optimum setting, then the resource governor assigns
each stream its specified optimum.

<li>If the total optimum cache pages is less than or equal to the reference
amount and one or more streams have specified
an EXEC_RESOURCE_ESTIMATE optimum setting,
then the resource governor assigns the specified optimum to those streams
that specified EXEC_RESOURCE_ACCURATE.
For the streams with EXEC_RESOURCE_ESTIMATE settings, it will assign them
their optimum and then divide
up the excess using a slight variation of the formula shown above.
The variations are that the excess is the difference between the reference
amount and the total optimum, and the computation excludes
those streams with EXEC_RESOURCE_ACCURATE settings.

<p>
Note that it is not possible to encounter a stream with an
EXEC_RESOURCE_UNBOUNDED setting in this case because of how the resource
governor has assigned an optimum requirement to those streams.

</ol>

The following diagram summarizes the policy.
The topmost horizontal line represents the range of possible
reference amounts, relative to the total minimum and total optimum for a
stream graph.
Depending on where the reference amount falls, either one of the five cases
applies, corresponding to the descriptions above.

<hr>
\image html ResourceAllocation.gif
<hr>

<h3>Other Allocation Policies</h3>

More sophisticated resource governors could support one or more of the
following:

<ul>

<li>Queue resource requests when requests cannot be immediately met.

<li>Manage other resources like number of threads and disk and network
bandwidth.

<li>Allocate resources based on current load and and/or usage history.

<li>Assign priorities to requests.

<li>Take into consideration mutual exclusivity within stream graphs.

<li>Allow resources to be assigned incrementally rather than once upfront.

<li>Allow resources to be returned incrementally rather than only at the end
of the entire stream graph's execution.

</ul>
 */
struct ResourceGovernorDesign 
{
    // NOTE:  dummy class for doxygen
};


FENNEL_END_CPPFILE("$Id$");

// End ResourceGovernorDesign.cpp
