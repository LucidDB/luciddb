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

#ifndef Fennel_ExecStreamUnitTestBase_Included
#define Fennel_ExecStreamUnitTestBase_Included

#include "fennel/test/ExecStreamTestBase.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/test/ExecStreamGenerator.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamUnitTestBase extends ExecStreamTestBase to be the common base of
 * the unit tests of fennel ExecStreams. It assumes a single stream graph.
 * @author John V. Sichi, Marc Berkowitz
 * @version $Id$
 */
class ExecStreamUnitTestBase : public ExecStreamTestBase
{
protected:
    SharedExecStreamGraph pGraph;
    SharedExecStreamGraphEmbryo pGraphEmbryo;


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
     * @param transformStreamEmbryo embryonic transforms streams which processes
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
     * Executes the prepared stream graph and verifies that its output
     * is an expected-size run of constant bytes.
     *
     * @param stream output stream from which to read
     *
     * @param nBytesExpected number of bytes which stream should produce
     *
     * @param byteExpected constant value expected for each byte
     */
    void verifyConstantOutput(
        ExecStream &stream,
        uint nBytesExpected,
        uint byteExpected);
    
    /**
     * Executes the prepared stream graph and verifies that its output
     * matches that produced by a value generator.
     *
     * @param stream output stream from which to read
     *
     * @param nRowsExpected number of rows expected
     *
     * @param verifier generator for expected values
     */
    void verifyOutput(
        ExecStream &stream,
        uint nRowsExpected,
        MockProducerExecStreamGenerator &verifier);

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

    // refine ExecStreamTestBase
    virtual void tearDown();


public:
    // refine ExecStreamTestBase
    virtual void testCaseSetUp();
};

FENNEL_END_NAMESPACE
#endif
// End ExecStreamTestUnitBase.h
