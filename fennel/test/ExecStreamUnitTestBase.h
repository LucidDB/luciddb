/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_ExecStreamUnitTestBase_Included
#define Fennel_ExecStreamUnitTestBase_Included

#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/exec/MockProducerExecStream.h"
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
    : public ExecStreamTestBase
{
protected:
    SharedExecStreamGraph pGraph;
    SharedExecStreamGraphEmbryo pGraphEmbryo;


    /**
     * Defines and prepares a graph consisting of one source stream.
     *
     * @param sourceStreamEmbryo embryonic source stream which produces tuples
     *
     * @return output buffer stream
     */
    SharedExecStream prepareSourceGraph(
        ExecStreamEmbryo &sourceStreamEmbryo);

    /**
     * Defines and prepares a graph consisting of one source stream
     * and one transform stream.
     *
     * @param sourceStreamEmbryo embryonic source stream which produces tuples
     *
     * @param transformStreamEmbryo embryonic transform stream which processes
     * tuples produced by sourceStreamEmbryo
     *
     * @return output buffer stream
     */
    SharedExecStream prepareTransformGraph(
        ExecStreamEmbryo &sourceStreamEmbryo,
        ExecStreamEmbryo &transformStreamEmbryo);

    /**
     * Defines and prepares a graph consisting of one source stream
     * and one or multiple transform streams.
     *
     * @param sourceStreamEmbryo embryonic source stream which produces tuples
     *
     * @param transforms embryonic transform streams which process
     * tuples produced by sourceStreamEmbryo or a child stream
     *
     * @return output buffer stream
     */
    SharedExecStream prepareTransformGraph(
        ExecStreamEmbryo &sourceStreamEmbryo,
        std::vector<ExecStreamEmbryo> &transforms);

    /**
     * Defines and prepares a graph consisting of two source streams
     * and one confluence stream.
     *
     * @param sourceStreamEmbryo1 embryonic source stream which produces tuples
     *
     * @param sourceStreamEmbryo2 embryonic source stream which produces tuples
     *
     * @param confluenceStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the sourceStreamEmbryos
     *
     * @return output buffer stream
     */
    SharedExecStream prepareConfluenceGraph(
        ExecStreamEmbryo &sourceStreamEmbryo1,
        ExecStreamEmbryo &sourceStreamEmbryo2,
        ExecStreamEmbryo &confluenceStreamEmbryo);

    /**
     * Defines and prepares a graph consisting of two source streams,
     * one confluence stream, and one transform stream.
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
     * @return output buffer stream
     */
    SharedExecStream prepareConfluenceTransformGraph(
        ExecStreamEmbryo &sourceStreamEmbryo1,
        ExecStreamEmbryo &sourceStreamEmbryo2,
        ExecStreamEmbryo &confluenceStreamEmbryo,
        ExecStreamEmbryo &transformStreamEmbryo);

    /**
     * Defines and prepares a graph consisting of a list of source streams
     * and one confluence stream.
     *
     * @param sourceStreamEmbryos list of embryonic source streams that
     * produce tuples
     *
     * @param confluenceStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the sourceStreamEmbryos
     *
     * @return output buffer stream
     */
    SharedExecStream prepareConfluenceGraph(
        std::vector<ExecStreamEmbryo> &sourceStreamEmbryos,
        ExecStreamEmbryo &confluenceStreamEmbryo);

    /**
     * Defines and prepares a graph consisting of one or more source streams
     * and one confluence stream.  Each source stream can be a list of streams.
     *
     * @param sourceStreamEmbryosList list of embryonic source streams which
     * produce tuples
     *
     * @param confluenceStreamEmbryo embryonic confluence stream which processes
     * tuples produced by the source streams
     *
     * @return output buffer stream
     */
    SharedExecStream prepareConfluenceGraph(
        std::vector<std::vector<ExecStreamEmbryo> > &sourceStreamEmbryosList,
        ExecStreamEmbryo &confluenceStreamEmbryo);

    /**
     * Defines and prepares a graph consisting of a source, a splitter, and one
     * or more parallel transform streams which flow together into a
     * confluence stream.
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
     * @param createSink if true (the default), creates a final output sink
     * in the stream graph
     *
     * @param saveSrc if true (the default), save the source in the stream
     * graph; if false, the save has already been done
     *
     * @return output buffer stream or null stream if createSink is false
     */
    SharedExecStream prepareDAG(
        ExecStreamEmbryo &srcStreamEmbryo,
        ExecStreamEmbryo &splitterStreamEmbryo,
        std::vector<ExecStreamEmbryo> &interStreamEmbryos,
        ExecStreamEmbryo &destStreamEmbryo,
        bool createSink = true,
        bool saveSrc = true);

    /**
     * Defines and prepares a graph consisting of a source, a splitter, and one
     * or more parallel transform streams which flow together into a
     * confluence stream.
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
     * @param createSink if true (the default), creates a final output sink
     * in the stream graph
     *
     * @param saveSrc if true (the default), save the source in the stream
     * graph; if false, the save has already been done
     *
     * @return output buffer stream or null stream if createSink is false
     */
    SharedExecStream prepareDAG(
        ExecStreamEmbryo &srcStreamEmbryo,
        ExecStreamEmbryo &splitterStreamEmbryo,
        std::vector<std::vector<ExecStreamEmbryo> > &interStreamEmbryosList,
        ExecStreamEmbryo &destStreamEmbryo,
        bool createSink = true,
        bool saveSrc = true);

    /**
     * Executes the prepared stream graph and verifies that its output
     * matches that produced by a value generator.
     *
     * @param stream output stream from which to read
     *
     * @param nRowsExpected number of rows expected
     *
     * @param verifier generator for expected values
     *
     * @param stopEarly if true, stop once nRowsExpected have been
     * fetched, even if more rows are available; this can be used
     * for simulating the cleanup effect of an error in the middle of execution
     */
    void verifyOutput(
        ExecStream &stream,
        uint nRowsExpected,
        MockProducerExecStreamGenerator &verifier,
        bool stopEarly = false);

    /**
     * Executes the prepared stream graph and verifies that all output tuples
     * matche an expected and given one
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
     * Executes the prepared stream graph and verifies the resultant tuples
     * against a set of tuples supplied in an input buffer.
     *
     * @param stream output stream from which to read
     *
     * @param outputTupleDesc descriptor of expected output tuple
     *
     * @param nRowsExpected number of rows expected
     *
     * @param expectedBuffer buffer containing expected tuples
     */
    void verifyBufferedOutput(
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
