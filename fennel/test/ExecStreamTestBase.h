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

#ifndef Fennel_ExecStreamTestBase_Included
#define Fennel_ExecStreamTestBase_Included

#include "fennel/test/SegStorageTestBase.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ExecStreamTestBase is a common base for tests of ExecStream
 * implementations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ExecStreamTestBase : virtual public SegStorageTestBase
{
protected:
    SharedExecStreamScheduler pScheduler;
    SharedExecStreamGraph pGraph;

    /**
     * Defines and prepares a graph consisting of two streams without
     * any intermediate buffering.
     *
     * @param pSourceStream source stream which produces tuples
     *
     * @param pOutputStream output stream which processes tuples produced
     * by pSourceStream
     */
    void prepareGraphTwoStreams(
        SharedExecStream pSourceStream,
        SharedExecStream pOutputStream);

    /**
     * Defines and prepares a graph consisting of one source stream
     * and one transform stream, introducing the required buffer streams.
     *
     * @param sourceStreamEmbryo embryonic source stream which produces tuples
     *
     * @param transformStreamEmbryo embryonic transform stream which processes
     * tuples produced by pSourceStream
     *
     * @return output buffer stream
     */
    SharedExecStream prepareGraphTwoBufferedStreams(
        ExecStreamEmbryo &sourceStreamEmbryo,
        ExecStreamEmbryo &transformStreamEmbryo);

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
    
public:
    // override TestBase
    virtual void testCaseSetUp();
    virtual void testCaseTearDown();
};

FENNEL_END_NAMESPACE

#endif

// End ExecStreamTestBase.h
