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

ExecStreamHowTo is a guide to writing new implementations of the ExecStream
interface and plugging them into Farrago.

<p>

This document is intended for developers who need to extend Fennel query
execution capabilities.  Writing an ExecStream implementation is a moderately
difficult undertaking requiring familiarity with a number of underlying Fennel
libraries.  Before starting, ask yourself whether a new ExecStream is really
the best solution to the problem at hand; it may be easier to extend some other
facility (TODO links):

<ul>

<li>write a user-defined function or transform in Java

<li>write a new calculator instruction

<li>write a new optimizer rule capable of wiring together existing stream
implementations in a novel fashion to achieve the desired effect

</ul>

If you have already ruled out all of those alternatives, then read on.
As background, you may also want to read the SchedulerDesign.

<h3>Choosing a Base Class</h3>

The first step is to decide where your new ExecStream fits into the
class hierarchy.  For traditional query processing, streams are wired together
into tree structures, with each stream producing a single output flow of tuples
and consuming zero or more input streams.  The abstract class
SingleOutputExecStream is a common base for all such stream implementations.
If your stream will be instantiated as an inputless leaf of the tree (e.g. the
implementation for a new kind of table access), then you can derive from
SingleOutputExecStream directly.  Otherwise, choose either ConduitExecStream
(for exactly one input, e.g. a filter) or ConfluenceExecStream (for
any number of inputs, e.g. a join or union) as the base.

<p>

Fennel also supports streams which produce multiple outputs, e.g. for recursive
query processing, but we will ignore that possibility in this document.

<p>

The base classes provided fill in some of the abstract ExecStream
methods, and in many cases those implementations
will suffice.  However, you are free to override any or all of them; usually
the overriding method will need to call the base method.

<p>

As an example, suppose we want to define a new ExecStream which performs the
same job as the <code>uniq</code> command in Unix; that is, it requires a
pre-sorted input stream, and removes duplicates by comparing
successive pairs of tuples.  This is clearly a ConduitExecStream, so
we now know enough to write:

<pre><code>
class UniqExecStream : public ConduitExecStream
{
    // TODO
};
</code></pre>

So, given an input data stream like

<table border="1">
<tr><td>Canada</td><td>Ontario</td></tr>
<tr><td>Canada</td><td>Ontario</td></tr>
<tr><td>EU</td><td>France</td></tr>
<tr><td>EU</td><td>Germany</td></tr>
<tr><td>EU</td><td>Germany</td></tr>
<tr><td>USA</td><td>Georgia</td></tr>
<tr><td>USSR</td><td>Georgia</td></tr>
<tr><td>USSR</td><td>Georgia</td></tr>
</table">

UniqExecStream should produce an output data stream like

<table border="1">
<tr><td>Canada</td><td>Ontario</td></tr>
<tr><td>EU</td><td>France</td></tr>
<tr><td>EU</td><td>Germany</td></tr>
<tr><td>USA</td><td>Georgia</td></tr>
<tr><td>USSR</td><td>Georgia</td></tr>
</table">

<h3>Parameters</h3>

To get maximum mileage out of the effort required to implement an ExecStream,
it is a good idea to parameterize the stream so that it can be used in a variety
of contexts.  In UniqExecStream, we might want to support an option which
causes duplicate detection to result in an error.  With this option disabled,
UniqExecStream can be used to implement SELECT DISTINCT; with this option
enabled, UniqExecStream can be used to prevent duplicate values
when loading a unique index.

<p>

ExecStream parameters are defined via a parallel class hierarchy descending
from ExecStreamParams.  Each class derived from ExecStream should have a
corresponding parameter class, even no new parameters are required; the
rationale for this rule will be explained later when we cover how streams are
instantiated.  For now, here is a parameter class for our running example:

<pre><code>
class UniqExecStreamParams : public ConduitExecStreamParams
{
    bool failOnDuplicate;

    explicit UniqueExecStreamParams()
    {
        failOnDuplicate = false;
    }
};
</code></pre>

It is polite (though not strictly required) to define a default constructor for
your parameters class which assigns sensible values to all parameters.

<h3>Algorithm</h3>

At this point, it is a good idea to sketch out the algorithm which
the stream will use to process its input and produce its output.  This will
necessarily be quite different from the final code, but should be enough to get
an idea of the state variables required.  Here's pseudocode for
UniqExecStream:

<pre><code>
lastTuple = null;
while (!input.EOS()) {
    currentTuple = input.readTuple();
    if (lastTuple != null) {
        if (currentTuple == lastTuple) {
            if (failOnDuplicate) {
                throw DuplicateKeyExcn(lastTuple);
            }
            continue;
        }
    }
    lastTuple = currentTuple;
    output.writeTuple(currentTuple);
}
</code></pre>

One difficult aspect of ExecStream implementation is transforming active
code (as above) into a passive state machine.  In the real implementation,
execution must yield whenever <code>input.readTuple()</code> exhausts an input
buffer or <code>output.writeTuple</code> overflows an output buffer, 
and input and output streams are never invoked directly.

<h3>Class Definition</h3>

In order to fill in our skeletal class, let us map the state from the
pseudocode into actual Fennel data structures:

<ul>

<li>
To perform the desired filtering, we will use TupleDescriptor::compareTuples.
This requires two instances of TupleData.

<li>
Stream execution is buffer-based, and we may need to remember the last tuple
seen across buffer boundaries.  The TupleData class does not provide
storage for the marshalled tuple representation; it only provides references.
So, we need a private buffer capable of holding the marshalled representation
of the largest possible tuple.

<li>
When processing the first tuple, we will need to know that there
was no predecessor.  The TupleData class has no null state as in the
pseudocode, so we'll need an extra boolean for this.

<li>
Finally, we need to remember the value of parameter
<code>failOnDuplicate</code>.

</ul>

So, at a minimum, our class will have the following members:

<pre><code>
class UniqExecStream : public ConduitExecStream
{
    bool failOnDuplicate;
    bool previousTupleValid;
    TupleData previousTuple;
    TupleData currentTuple;
    boost::scoped_array<FixedBuffer> pLastTupleSaved;
    
public:
    // implement ExecStream
    virtual void prepare(UniqExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};
</code></pre>
    
Note that the open and execute methods override base ExecStream methods, while
the prepare method overloads instead since its parameter signature is different.
So even though prepare is declared as virtual, it probably will not be invoked
via virtual dispatch.

<p>

Also note that most ExecStream implementations do not require any constructors
to be defined.  The reason is that the prepare and open method are responsible
for initializing state before execution.  There is one exception to this rule:
if the implementation provides a closeImpl method, it is necessary for the
constructor to initialize any data members which might be accessed by
closeImpl, since closeImpl is called even if the stream is discarded before
prepare/open due to an exception.  Classes which use only proper holder classes
such as boost::scoped_array need not worry about this.

<p>

Likewise, a destructor is unnecessary unless the implementation allocates
resources without proper holder objects and does not release them inside of
closeImpl.

<h3>Preparation</h3>

The prepare method is called only once during the lifetime of a stream, before
it is opened for the first execution.  This is our chance to record
parameter values and precompute any information needed throughout the
lifetime of the stream.  Precomputation is essential for high performance,
since anything which can be allocated or precomputed at preparation time reduces
per-tuple execution cost.

<p>

It's important to understand that the parameters reference passed to the prepare
method is transient and will not be available during execution.  Hence,
it is the responsibility of the prepare method to copy out any information
it needs to preserve.

<p>

Here is the code for UniqExecStream:

<pre><code>
void UniqExecStream::prepare(UniqExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    
    assert(pInAccessor->getTupleDesc() == pOutAccessor->getTupleDesc();
    
    failOnDuplicate = params.failOnDuplicate;
    
    previousTuple.compute(pInAccessor->getTupleDesc());
    currentTuple.compute(pInAccessor->getTupleDesc());
}
</code></pre>

So, where did pInAccessor and pOutAccessor come from?  They were inherited
from ConduitExecStream, which guarantees that these instances of
ExecStreamBufAccessor will be set up correctly by the time prepare is called.
These provide the stream with access to the input and output buffers of
the conduit.  At preparation time, there is of course no data in the buffers
yet, but TupleDescriptor instances have already been set up to inform us
of the shape of the data which we will be expected to process during execution.
As a sanity check, UniqExecStream asserts that its input and output tuple
descriptors are identical, since it only filters the stream without changing
its shape.

<p>

The calls to TupleData::compute are required in order to set up
<code>previousTuple</code> and <code>currentTuple</code> for use during
execution.  Under the covers, these calls allocate and initialize
correctly-sized arrays of TupleDatum objects; this is a good example of the
kind of allocation which should be avoided during execution.

<p>

What about the pLastTupleSaved buffer?  We already have enough information to
allocate it now.  However, it could be very large (depending on the maximum
tuple size) and does not require significant computation to allocate.  So, we
defer its allocation until open is called, thus avoiding memory bloat.

<h3>Opening for Business</h3>

Once prepared, an ExecStream can be used over and over to process multiple
executions of the same prepared query.  Each execution is initiated with
a call to open and terminated with a call to close.  So, it's important
for the open call to reset any state which will affect execution results;
otherwise, leftover state from an earlier execution could pollute a later
execution.  For our example, we need to clear the previousTupleValid flag
and allocate the pLastTupleSaved buffer:

<pre><code>
void UniqExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    previousTupleValid = false;
    if (!restart) {
        uint cbTupleMax =
            pInAccessor->getConsumptionTupleAccessor().getMaxByteCount();
        pLastTupleSaved.reset(new FixedBuffer[cbTupleMax]);
    }
}
</code></pre>

The restart flag is only true when a stream is restarted in the middle of an
execution (e.g. as part of a nested loop join).  In the restart case,
UniqExecStream can skip reallocating its private buffer.

</code></pre>

<h3>Execution</h3>

Finally, everything is set up, and we can get busy with writing the
execute method, which does the real work.  Since each stream implementation is
supposed to do something interesting and different, it's difficult to give
precise instructions for writing this method.  However,
various categories of streams tend to follow well-defined patterns.  Our
UniqExecStream provides a good template for implementing filtering streams, and
from there it is straightforward to generalize to streams with any number of
inputs.  We will start with a complete listing of the method body, and then
walk through line by line.

<pre><code>
ExecStreamResult UniqExecStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    for (uint nTuples = 0; nTuples < quantum.nTuplesMax; ++nTuples) {
        while (!pInAccessor->isTupleConsumptionPending()) {
            if (!pInAccessor->demandData()) {
                return EXECRC_BUF_UNDERFLOW;
            }
            pInAccessor->unmarshalTuple(currentTuple);
            
            if (previousTupleValid) {
                int c = pInAccessor->getTupleDesc().compareTuples(
                    lastTuple, currentTuple);
                assert(c <= 0);
                if (c == 0) {
                    pInAccessor->consumeTuple();
                    if (failOnDuplicate) {
                        throw DuplicateKeyExcn(lastTuple);
                    }
                    continue;
                }
            } else {
                previousTupleValid = true;
            }
            
            TupleAccessor &tupleAccessor = 
                pInAccessor->getScratchTupleAccessor();
            memcpy(
                pLastTupleSaved.get(),
                pInAccessor->getConsumptionStart(),
                tupleAccessor.getCurrentByteCount());
            tupleAccessor.unmarshal(lastTuple);
        }
        
        if (!pOutAccessor->produceTuple(lastTuple)) {
            return EXECRC_BUF_OVERFLOW;
        }
        pInAccessor->consumeTuple();
    }
    return EXECRC_QUANTUM_EXPIRED;
}
</code></pre>

In comparison with the pseudocode presented earlier, this is a bit
more complicated.  Examining the first few lines,

<pre><code>
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }
</code></pre>

we find a call to the base class helper method
ConduitExecStream::precheckConduitBuffers.  This ensures that there is
data ready to be processed in the input buffer and that at least some
space remains available in the output buffer.  If the end of the
input stream has been reached, this call will cause execute to return
EXECRC_EOS.

<p>

Next we come to the beginning of the outer loop:

<pre><code>
    for (uint nTuples = 0; nTuples < quantum.nTuplesMax; ++nTuples) {
</code></pre>

This loop keeps count of the number of tuples produced so far during the
current invocation of execute, yielding once the requested quantum
has expired.  Within this outer loop is nested an inner loop:

<pre><code>
        while (!pInAccessor->isTupleConsumptionPending()) {
</code></pre>

This inner loop is executed once per input tuple consumed (approximately).  The
while test causes the loop body to be skipped altogether if a previous
invocation of execute already accessed an input tuple but had to yield
before completly processing it (due to output buffer overflow).

<pre><code>
            if (!pInAccessor->demandData()) {
                return EXECRC_BUF_UNDERFLOW;
            }
            pInAccessor->unmarshalTuple(currentTuple);
</code></pre>

Now that the stream is ready to process a new input tuple, the first thing to
do is to make sure that one is available.  (precheckConduitBuffers guarantees
this the first time through the loop, but any time after that we can exhaust
the current input buffer.)  The ExecStreamBufAccessor::demandData call takes
care of this.  The ExecStreamBufAccessor::unmarshalTuple call unmarshals a
tuple by reference from the input buffer into currentTuple, putting pInAccessor
into the <em>consumption pending</em> state (the buffer space cannot be
reused until we are done accessing it).

<p>

The next block of code is the heart of the algorithm:

<pre><code>
            if (previousTupleValid) {
                int c = pInAccessor->getTupleDesc().compareTuples(
                    lastTuple, currentTuple);
                assert(c <= 0);
                if (c == 0) {
                    pInAccessor->consumeTuple();
                    if (failOnDuplicate) {
                        throw DuplicateKeyExcn(lastTuple);
                    }
                    continue;
                }
            } else {
                previousTupleValid = true;
            }
</code></pre>

If a previous tuple has been seen, it is compared to the current one.  The
assertion verifies that our input stream was actually sorted in ascending order
as promised.  If the current and previous tuples match, the current one is
consumed (and ignored) and control flows to fetch the next tuple.  (Or an
exception is thrown if the corresponding parameter is in effect.  Note
that in this case the input tuple is consumed so that if re-execution is
requested the stream will skip past the offending tuple.)

<p>

Once the distinctness test has passed, it is necessary to save a copy
of the new values in currentTuple as lastTuple:

<pre><code>
            TupleAccessor &tupleAccessor = 
                pInAccessor->getScratchTupleAccessor();
            memcpy(
                pLastTupleSaved.get(),
                pInAccessor->getConsumptionStart(),
                tupleAccessor.getCurrentByteCount());
            tupleAccessor.unmarshal(lastTuple);
</code></pre>

ExecStreamBufAccessor::getScratchTupleAccessor provides a spare instance of
TupleAccessor for just such purposes.  The code uses a straight memcpy for
speed instead of marshalling individual values.  However, after the memcpy,
it is still necessary to call TupleAccessor::unmarshal so that lastTuple
references the new data just copied.

<p>

At this point, it would be valid to insert a break statement to drop out
of the inner loop, since the while test is guaranteed to be false.  Either
way, it is now time to write out the new tuple:

<pre><code>
        if (!pOutAccessor->produceTuple(lastTuple)) {
            return EXECRC_BUF_OVERFLOW;
        }
        pInAccessor->consumeTuple();
</code></pre>

Unfortunately, there may be insufficient space in the output buffer.  When that
happens, the stream must yield, going into suspended animation until the
contents of the output buffer have been consumed and space is freed up.  This
is the reason that we don't actually consume the input tuple until the output
tuple has been written successfully--the <em>consumption pending</em> state
serves to guide us down the right path on the next execution.  If this
does not make sense to you now, it will after a few hours with a debugger.

<p>

<em>
ASIDE:  In this case, we could have used a separate flag instead of the
consumption state, but in general, the TupleData used for output may be
referencing memory from the input tuple, so the pattern shown here is safest.
</em>

<p>

Finally, execution continues around the outer for loop unless the quantum has
expired:

<pre><code>
    return EXECRC_QUANTUM_EXPIRED;
</code></pre>

<p>

A number of optimizations are possible; here are a few:

<ul>

<li>
Instead of calling ExecStreamBufAccessor::produceTuple, which has to call
TupleAccessor::marshal, we could just <code>memcpy</code> the marshalled tuple
image from the consumption buffer to the production buffer.

<li>
We do not need to copy to the pLastTupleSaved buffer for every new tuple;
this is really only required immediately before returning from execute,
which is when any references to the consumption buffer become invalid.
</ul>

<h3>Closing Up Shop</h3>

A stream can be closed at any time.  There is no guarantee that it has been
prepared or opened; if it has been opened, there is no guarantee that
EOS has been reached, or that execute has been called even once.  In response
to a close request, a stream should release any resources which were not
allocated by prepare.  It does not need to reset other state, since the
class contract guarantees that open will be called again before any
re-execution.

<p>

For UniqExecStream, we should deallocate pLastTupleSaved so that excess
memory is not tied down while the stream is inactive:

<pre><code>
void UniqExecStream::closeImpl()
{
    pLastTupleSaved.reset();
    ConduitExecStream::closeImpl();
}
</code></pre>

<h3>Unit Testing</h3>

TODO

<h3>Models and Factories</h3>

In order for your brand new ExecStream implementation to be usable from
Farrago, there are a few more steps you need to take once it has been
successfully unit tested:

<ol>

<li>
Determine which UML model you should edit to register the stream class
in the catalog.  (TODO:  link to Farrago JNI docs.)  Normally, this
is the file <code>farrago/catalog/xmi/FarragoExtMetamodelUML.zuml</code>,
which can be edited with Poseidon.  If you are working on a
separate project which extends Farrago, it may have its own model.

<li>
Edit the model, defining a UML class which inherits from
ExecutionStreamDef.  The UML class should have attributes corresponding
to the fields of the parameter class you defined in C++
(e.g. UniqExecStreamParams).

<li>
Regenerate the catalog (<code>ant createCatalog</code> for Farrago).

<li>
Regenerate the C++ code which exposes the model to Fennel
(<code>ant generateFemCpp</code> for Farrago).

<li>
Determine the correct C++ factory class which will be responsible for
creating new instances of your stream class.  For Farrago, this
is ExecStreamFactory for some streams, or a class derived from
ExecStreamSubFactory for others.

<li>

Add a visit method to the factory.  For our running example, the
signature would be
<code>virtual void ExecStreamFactory::visit(ProxyUniqStreamDef &)</code>.
Follow the example of other visit methods nearby.

</ol>

<h3>Optimizer Rules</h3>

Of course, just because Farrago now knows how to instantiate your stream, 
you are not done yet, because Farrago does not know anything about
the semantics of your stream.  So the next step is to write an
optimizer rule.  Such a big topic is out of scope for this document
(TODO:  link to a yet-to-be-written HOWTO).

<p>

For our example, the new optimizer rule would be responsible for matching the
pattern wherein a GROUP BY relational operator with no aggregates and all fields
grouped (the translation of DISTINCT) has a child with all fields pre-sorted.
The rule would replace the GROUP BY with a physical relational expression
corresponding to UniqExecStream, say FennelUniqRel (a Java class).  In
response to a call to FennelUniqRel.toStreamDef, FennelUniqRel would create a
new instance of model-generated Java class UniqStreamDef and fill in
its attributes.  Once Fennel received the complete plan, this would be
translated automatically into an instance of C++ class ProxyUniqStreamDef,
which is the input to the factory method discussed in the previous section.
The final step is to get down on your knees and beseech
the optimizer to do what you mean, not what you say.

<h3>Advanced Resource Allocation</h3>

Some ExecStream implementations require resources which are costly enough that
their allocation and deallocation needs to be centrally managed.  The current
heavyweight resources known to Fennel are threads and cache pages (as
defined by ExecStreamResourceQuantity).  Streams which require these
resources (including any stream which performs disk access) must
override method ExecStream::getResourceRequirements
(the default implementation requests zero resources).
In response, the scheduler calls ExecStream::setResourceAllocation.
The default implementation for this method records the granted resource
allocation in member variable ExecStream::resourceAllocation, which
can be examined by the stream to decide how much to allocate.

<p>

For the sake of example, suppose that instead of using boost::scoped_array
to allocate pLastTupleSaved from the heap, we instead wanted to pin a cache
page for this purpose.  In that case, we would change
UniqExecStream as follows:

<pre><code>

class UniqExecStream : public ConduitExecStream
{
    bool failOnDuplicate;
    bool previousTupleValid;
    TupleData previousTuple;
    TupleData currentTuple;
    SegPageLock bufferLock;     // NEW
    SegmentAccessor scratchAccessor; // NEW
    PBuffer pLastTupleSaved;    // CHANGED
    
public:
    // implement ExecStream
    virtual void prepare(UniqExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    
    // NEW
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
};

void UniqExecStream::prepare(UniqExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    
    assert(pInAccessor->getTupleDesc() == pOutAccessor->getTupleDesc();
    
    failOnDuplicate = params.failOnDuplicate;
    
    previousTuple.compute(pInAccessor->getTupleDesc());
    currentTuple.compute(pInAccessor->getTupleDesc());
    
    // NEW
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

// NEW
void UniqExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);
    minQuantity.nCachePages += 1;
    optQuantity = minQuantity;
}

void UniqExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    previousTupleValid = false;
    if (!restart) {
        uint cbTupleMax =
            pInAccessor->getConsumptionTupleAccessor().getMaxByteCount();
            
        // CHANGED
        bufferLock.allocatePage();
        assert(bufferLock.getPage().getCache().getPageSize() >= cbTupleMax);
        pLastTupleSaved = bufferLock.getPage().getWritableData();
    }
}

void UniqExecStream::closeImpl()
{
    // CHANGED
    pLastTupleSaved = NULL;
    bufferLock.unlock();
    
    ConduitExecStream::closeImpl();
}

</code></pre>

TODO:  scheduler interface for centralized thread pooling

<h3>Exception Handling</h3>

ExecStream::execute may throw an exception at any time.  It is up to scheduler
implementations to decide how to handle this.  Some schedulers may terminate the
query; others may enqueue the exception and attempt to continue.  For this
reason, streams which throw exceptions may want to update state before
throwing (as in the UniqExecStream example) to allow for meaningful
resumption of execution (either skipping an offending tuple, or re-attempting
a failed request to an underlying service).

<h3>More To Explore</h3>

The best way to learn more about the techniques involved in
constructing advanced ExecStream implementations is to study the
interfaces involved (particular ExecStreamBufAccessor) and
existing ExecStream implementations.  For buffer-handling tricks,
see SegBufferStream, ScratchBufferStream, and CopyExecStream.
For an example of a stream which consumes two inputs at once,
see CartesianJoinExecStream.  For an example
of a stream with no inputs, see BTreeScanExecStream.

 */
struct ExecStreamHowTo
{
    // NOTE:  dummy class for doxygen
};

FENNEL_END_CPPFILE("$Id$");

// End ExecStreamHowTo.cpp
