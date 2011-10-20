/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

#ifndef Fennel_ExecStreamUnitTestBase_Included
#define Fennel_ExecStreamUnitTestBase_Included

#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/MockConsumerExecStream.h"
#include "fennel/test/ExecStreamGenerator.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamUnitTestBase extends ExecStreamTestBase to be the common base of
 * the unit tests of ExecStreams which assume a single stream graph.  (Other
 * subclasses of ExecStreamTestBase may not rely on this assumption; this is
 * the reason why this class exists separately from ExecStreamTestBase.)
 *
 * @author John V. Sichi, Marc Berkowitz
 * @version $Id$
 */
class FENNEL_TEST_EXPORT ExecStreamUnitTestBase
    : public virtual ExecStreamTestBase
{
public:
;
protected:
    SharedExecStreamGraph pGraph;
    SharedExecStreamGraphEmbryo pGraphEmbryo;

private:
    /**
     * Completes the construction of the stream graph: appends a final sink
     * buffer, and prepares the graph.
     * @param final the last node in the incomplete graph.
     * @return the last node in the completed graph.
     */
    ExecStream& completeGraph(ExecStream& final);

protected:

    /**
     * Defines and constructs a graph consisting of one source stream. Despite
     * the name, does not prepare the graph, so that subclasses can more easily
     * extend the graph.
     *
     * @param sourceStreamEmbryo embryonic source stream which produces tuples
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareSourceGraph(
        ExecStreamEmbryo &sourceStreamEmbryo);

    /**
     * Defines and constructs a graph consisting of one source stream and one
     * transform stream. Does not prepare the graph, so that subclasses can more
     * easily extend the graph.
     *
     * @param sourceStreamEmbryo embryonic source stream which produces tuples
     *
     * @param transformStreamEmbryo embryonic transform stream which processes
     * tuples produced by sourceStreamEmbryo
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareTransformGraph(
        ExecStreamEmbryo &sourceStreamEmbryo,
        ExecStreamEmbryo &transformStreamEmbryo);

    /**
     * Defines and constructs a graph consisting of one source stream and one or
     * multiple transform streams. Does not prepare the graph, so that
     * subclasses can more easily extend the graph.
     *
     * @param sourceStreamEmbryo embryonic source stream which produces tuples
     *
     * @param transforms embryonic transform streams which process
     * tuples produced by sourceStreamEmbryo or a child stream
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareTransformGraph(
        ExecStreamEmbryo &sourceStreamEmbryo,
        std::vector<ExecStreamEmbryo> &transforms);

    /**
     * Defines and constructs a graph consisting of two source streams and one
     * confluence stream. Does not prepare the graph, so that subclasses can
     * more easily extend the graph.
     *
     * @param sourceStreamEmbryo1 embryonic source stream which produces tuples
     *
     * @param sourceStreamEmbryo2 embryonic source stream which produces tuples
     *
     * @param confluenceStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the sourceStreamEmbryos
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareConfluenceGraph(
        ExecStreamEmbryo &sourceStreamEmbryo1,
        ExecStreamEmbryo &sourceStreamEmbryo2,
        ExecStreamEmbryo &confluenceStreamEmbryo);


    /**
     * Defines and constructs a graph consisting of two source streams, one
     * confluence stream, and one transform stream. Does not prepare the graph,
     * so that subclasses can more easily extend the graph.
     *
     * @param sourceStreamEmbryo1 embryonic source stream which produces tuples
     *
     * @param sourceStreamEmbryo2 embryonic source stream which produces tuples
     *
     * @param confluenceStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the sourceStreamEmbryos
     *
     * @param transformStreamEmbryo embryonic transform streams which process
     * tuples produced by a child stream
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareConfluenceTransformGraph(
        ExecStreamEmbryo &sourceStreamEmbryo1,
        ExecStreamEmbryo &sourceStreamEmbryo2,
        ExecStreamEmbryo &confluenceStreamEmbryo,
        ExecStreamEmbryo &transformStreamEmbryo);

    /**
     * Defines and constructs a graph consisting of a list of source streams and
     * one confluence stream. Does not prepare the graph, so that subclasses can
     * more easily extend the graph.
     *
     * @param sourceStreamEmbryos list of embryonic source streams that
     * produce tuples
     *
     * @param confluenceStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the sourceStreamEmbryos
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareConfluenceGraph(
        std::vector<ExecStreamEmbryo> &sourceStreamEmbryos,
        ExecStreamEmbryo &confluenceStreamEmbryo);

    /**
     * Defines and constructs a graph consisting of one or more source streams
     * and one confluence stream. Each source stream can be a list of streams.
     * Does not prepare the graph, so that subclasses can more easily extend the
     * graph.
     *
     * @param sourceStreamEmbryosList list of embryonic source streams which
     * produce tuples
     *
     * @param confluenceStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the source streams
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareConfluenceGraph(
        std::vector<std::vector<ExecStreamEmbryo> > &sourceStreamEmbryosList,
        ExecStreamEmbryo &confluenceStreamEmbryo);

    /**
     * Defines and constructs a graph consisting of one or more sources, a
     * confluence, and a list of transforms. Each source stream can be a list of
     * streams. Does not prepare the graph, so that subclasses can more easily
     * extend the graph.
     *
     * @param sources list of embryonic source streams which  produce tuples
     * @param confluence embryonic confluence stream
     * @param transforms list of embryonic transformer streams
     * @return final node in stream graph
     */
    SharedExecStream prepareConfluenceTransformGraph(
        std::vector<std::vector<ExecStreamEmbryo> > &sources,
        ExecStreamEmbryo &confluence,
        std::vector<ExecStreamEmbryo> &transforms);

    /**
     * Defines and constructs a graph consisting of a source, a splitter, and
     * one or more parallel transform streams which flow together into a
     * confluence stream. Does not prepare the graph, so that subclasses can
     * more easily extend the graph.
     *
     * @param srcStreamEmbryo embryonic source stream which produces tuples
     *
     * @param splitterStreamEmbryo embryonic SplitterExecStream which
     * produces tuples for multiple consumers
     *
     * @param interStreamEmbryos embryonic intermediate streams which
     * transform tuples; each stream consists of a single embryo
     *
     * @param destStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the interStreamEmbryos
     *
     * @param saveSrc if true (the default), save the source in the stream
     * graph; if false, the save has already been done
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareDAG(
        ExecStreamEmbryo &srcStreamEmbryo,
        ExecStreamEmbryo &splitterStreamEmbryo,
        std::vector<ExecStreamEmbryo> &interStreamEmbryos,
        ExecStreamEmbryo &destStreamEmbryo,
        bool saveSrc = true);

    /**
     * Defines and constructs a graph consisting of a source, a splitter, and
     * one or more parallel transform streams which flow together into a
     * confluence stream. Does not prepare the graph, so that subclasses can
     * more easily extend the graph.
     *
     * @param srcStreamEmbryo embryonic source stream which produces tuples
     *
     * @param splitterStreamEmbryo embryonic SplitterExecStream which
     * produces tuples for multiple consumers
     *
     * @param interStreamEmbryosList one or more embryonic intermediate
     * streams which transform tuples; each stream can have one more embryos
     *
     * @param destStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the interStreamEmbryos
     *
     * @param saveSrc if true (the default), save the source in the stream
     * graph; if false, the save has already been done
     *
     * @return final node in stream graph
     */
    SharedExecStream prepareDAG(
        ExecStreamEmbryo &srcStreamEmbryo,
        ExecStreamEmbryo &splitterStreamEmbryo,
        std::vector<std::vector<ExecStreamEmbryo> > &interStreamEmbryosList,
        ExecStreamEmbryo &destStreamEmbryo,
        bool saveSrc = true);


    /**
     * Completes the construction of the stream graph, executes it, and verifies
     * that its output matches that produced by a value generator.
     *
     * @param stream output stream from which to read
     * @param nRowsExpected number of rows expected
     * @param checker functor that checks each output row
     * @param stopEarly if true, stop once nRowsExpected have been
     * fetched, even if more rows are available; this can be used
     * for simulating the cleanup effect of an error in the middle of execution
     */
    virtual void verifyOutput(
        ExecStream &stream,
        uint nRowsExpected,
        MockConsumerExecStreamTupleChecker& checker,
        bool stopEarly = false);


    /**
     * Completes the construction of the stream graph, executes it, and verifies
     * that its output matches that produced by a value generator.
     *
     * @param stream output stream from which to read
     * @param nRowsExpected number of rows expected
     * @param verifier generator for expected values
     * @param stopEarly if true, stop once nRowsExpected have been
     * fetched, even if more rows are available
     */
    void verifyOutput(
        ExecStream &stream,
        uint nRowsExpected,
        MockProducerExecStreamGenerator &verifier,
        bool stopEarly = false);

    /**
     * Completes the construction of the stream graph, executes it, and verifies
     * that its output matches that produced by a string generator.
     *
     * @param stream output stream from which to read
     * @param nRowsExpected number of rows expected
     * @param expected the expected rows (as strings).
     */
    void verifyStringOutput(
        ExecStream &stream,
        uint nRowsExpected,
        const std::vector<std::string>& expected);

    /**
     * Completes the construction of the stream graph, executes it, and verifies
     * that all output tuples matche an expected and given one
     *
     * @param stream output stream from which to read
     *
     * @param expectedTuple
     *
     * @param nRowsExpected
     */
    void verifyConstantOutput(
        ExecStream &stream,
        const TupleData  &expectedTuple,
        uint nRowsExpected);

    /**
     * Completes the construction of the stream graph, executes it, and verifies
     * the resultant tuples against a set of tuples supplied in an input buffer.
     *
     * @param stream output stream from which to read
     *
     * @param outputTupleDesc descriptor of expected output tuple
     *
     * @param nRowsExpected number of rows expected
     *
     * @param expectedBuffer buffer containing expected tuples
     */
    virtual void verifyBufferedOutput(
        ExecStream &stream,
        TupleDescriptor outputTupleDesc,
        uint nRowsExpected,
        PBuffer expectedBuffer);

    /**
     * Reset stream graph so multiple iterations of a method can be called
     * within a single testcase.
     */
    void resetExecStreamTest();

    // refine ExecStreamTestBase
    virtual void tearDownExecStreamTest();

public:
    // refine ExecStreamTestBase
    virtual void testCaseSetUp();
};

FENNEL_END_NAMESPACE
#endif

// End ExecStreamUnitTestBase.h
