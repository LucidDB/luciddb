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

#ifndef Fennel_MockProducerExecStream_Included
#define Fennel_MockProducerExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * MockProducerExecStreamGenerator defines an interface for generating
 * a data stream.
 */
class MockProducerExecStreamGenerator
{
public:
    virtual ~MockProducerExecStreamGenerator();
    
    /**
     * Generates one data value.
     *
     * @param iRow 0-based row number to generate
     */
    virtual int64_t generateValue(uint iRow) = 0;
};

typedef boost::shared_ptr<MockProducerExecStreamGenerator>
    SharedMockProducerExecStreamGenerator;
    
/**
 * MockProducerExecStreamParams defines parameters for MockProducerExecStream.
 */
struct MockProducerExecStreamParams : public SingleOutputExecStreamParams
{
    /**
     * Number of rows to generate.
     */
    uint64_t nRows;

    /**
     * Generator for row values.  If non-singular, the tuple descriptor
     * for this stream must be a single int64_t.  If singular, all output
     * is constant 0.
     */
    SharedMockProducerExecStreamGenerator pGenerator;
};

/**
 * MockProducerExecStream generates mock data.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class MockProducerExecStream : public SingleOutputExecStream
{
    uint cbTuple;
    uint64_t nRowsMax;
    uint64_t nRowsProduced;
    TupleData outputData;
    SharedMockProducerExecStreamGenerator pGenerator;
    
public:
    // implement ExecStream
    virtual void prepare(MockProducerExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End MockProducerExecStream.h
