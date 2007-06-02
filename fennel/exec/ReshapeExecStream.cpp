/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ReshapeExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ReshapeExecStream::prepare(ReshapeExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    TupleDescriptor const &inputDesc = pInAccessor->getTupleDesc();
    TupleAccessor &inputAccessor = pInAccessor->getConsumptionTupleAccessor();

    compOp = params.compOp;
    if (compOp != COMP_NOOP) {
        // setup the comparison tuple descriptor
        compTupleDesc.projectFrom(inputDesc, params.inputCompareProj);

        // setup the projection of the columns for comparison
        inputCompareProjAccessor.bind(inputAccessor, params.inputCompareProj);
        inputCompareData.compute(compTupleDesc);

        // unmarshal the comparison tuple passed in as a parameter
        TupleAccessor tupleAccessor;
        tupleAccessor.compute(compTupleDesc);
        tupleAccessor.setCurrentTupleBuf(params.pCompTupleBuffer.get());
        uint nBytes = tupleAccessor.getCurrentByteCount();
        compTupleBuffer.reset(new FixedBuffer[nBytes]);
        memcpy(compTupleBuffer.get(), params.pCompTupleBuffer.get(), nBytes);
        tupleAccessor.setCurrentTupleBuf(compTupleBuffer.get());
        paramCompareData.compute(compTupleDesc);
        tupleAccessor.unmarshal(paramCompareData);

        // setup a tuple projection to project the last key for use in
        // non-equality comparisons
        lastKey.push_back(paramCompareData.size() - 1);
        lastKeyDesc.projectFrom(compTupleDesc, lastKey);
    }

    // setup the output projection
    outputProjAccessor.bind(inputAccessor, params.outputProj);
    inputOutputDesc.projectFrom(inputDesc, params.outputProj);
    outputDesc = pOutAccessor->getTupleDesc();
    assert(inputOutputDesc.size() == outputDesc.size());
    outputData.compute(outputDesc);

    // determine if simple casting is required
    castRequired = (inputOutputDesc != outputDesc);
    if (castRequired) {
        TupleProjection proj;
        if (compOp == COMP_NE) {
            proj = params.inputCompareProj;
        }
        assert(checkCastTypes(
            proj, inputOutputDesc, pOutAccessor->getTupleDesc()));
        inputOutputData.compute(inputOutputDesc);
    }
}

bool ReshapeExecStream::checkCastTypes(
    const TupleProjection &compareProj,
    const TupleDescriptor &inputTupleDesc,
    const TupleDescriptor &outputTupleDesc)
{
    for (uint i = 0; i < inputTupleDesc.size(); i++) {
        if (!(inputTupleDesc[i] == outputTupleDesc[i])) {
            // only allow not nullable -> nullable, unless nulls are being
            // filtered out from that column
            if (inputTupleDesc[i].isNullable &&
                !outputTupleDesc[i].isNullable)
            {
                assert(nullFilter(compareProj, i));
            } else {
                assert(
                    (inputTupleDesc[i].isNullable ==
                        outputTupleDesc[i].isNullable)
                    || (!inputTupleDesc[i].isNullable
                        && outputTupleDesc[i].isNullable));
            }
            if (inputTupleDesc[i].pTypeDescriptor->getOrdinal() !=
                outputTupleDesc[i].pTypeDescriptor->getOrdinal())
            {
                // if types are different, must be casting from char to 
                // varchar
                assert(
                    inputTupleDesc[i].pTypeDescriptor->getOrdinal() ==
                        STANDARD_TYPE_CHAR &&
                    outputTupleDesc[i].pTypeDescriptor->getOrdinal() ==
                        STANDARD_TYPE_VARCHAR);
            }
            if (inputTupleDesc[i].cbStorage != outputTupleDesc[i].cbStorage) {
                // if lengths are different, must be casting from char or
                // varchar to varchar
                assert(
                    (inputTupleDesc[i].pTypeDescriptor->getOrdinal() ==
                        STANDARD_TYPE_VARCHAR ||
                        inputTupleDesc[i].pTypeDescriptor->getOrdinal() ==
                            STANDARD_TYPE_CHAR) &&
                    outputTupleDesc[i].pTypeDescriptor->getOrdinal() ==
                        STANDARD_TYPE_VARCHAR);
            }
        }
    }
    return true;
}

bool ReshapeExecStream::nullFilter(
    const TupleProjection &compareProj, uint colno)
{
    for (uint i = 0; i < compareProj.size(); i++) {
        if (compareProj[i] == colno) {
            if (!paramCompareData[i].pData) {
                return true;
            } else {
                break;
            }
        }
    }
    return false;
}

void ReshapeExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    producePending = false;
}

ExecStreamResult ReshapeExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    if (producePending) {
        if (!pOutAccessor->produceTuple(outputData)) {
            return EXECRC_BUF_OVERFLOW;
        }
        pInAccessor->consumeTuple();
        producePending = false;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        pInAccessor->accessConsumptionTuple();

        // filter the data, if filtering criteria provided
        if (compOp != COMP_NOOP) {
            bool pass = compareInput();
            if (!pass) {
                pInAccessor->consumeTuple();
                continue;
            }
        }

        if (castRequired) {
            castOutput();
        } else {
            outputProjAccessor.unmarshal(outputData);
        }
        producePending = true;
        if (!pOutAccessor->produceTuple(outputData)) {
            return EXECRC_BUF_OVERFLOW;
        }
        producePending = false;
        pInAccessor->consumeTuple();
    }

    return EXECRC_QUANTUM_EXPIRED;
}

bool ReshapeExecStream::compareInput()
{
    inputCompareProjAccessor.unmarshal(inputCompareData);
    int rc;

    // if the comparison is non-equality, first compare the first n-1 keys
    // for equality; if those keys are equal, then do the non-equality
    // comparison on just the last key
    if (compOp == COMP_EQ) {
        rc = compTupleDesc.compareTuples(inputCompareData, paramCompareData);
    } else {
        rc =
            compTupleDesc.compareTuplesKey(
                inputCompareData, paramCompareData,
                paramCompareData.size() - 1);
        if (rc != 0) {
            return false;
        }
        // ignore NULLs if doing a comparison against a non-NULL value
        if (!paramCompareData[paramCompareData.size() - 1].pData) {
            rc =
                lastKeyDesc.compareTuples(
                    inputCompareData, lastKey, paramCompareData, lastKey);
        } else {
            bool containsNullKey;
            rc =
                lastKeyDesc.compareTuples(
                    inputCompareData, lastKey, paramCompareData, lastKey,
                    &containsNullKey);
            if (containsNullKey) {
                return false;
            }
        }
    }

    bool pass;
    switch (compOp) {
    case COMP_EQ:
        pass = (rc == 0);
        break;
    case COMP_NE:
        pass = (rc != 0);
        break;
    case COMP_LT:
        pass = (rc < 0);
        break;
    case COMP_LE:
        pass = (rc <= 0);
        break;
    case COMP_GT:
        pass = (rc > 0);
        break;
    case COMP_GE:
        pass = (rc >= 0);
        break;
    default:
        pass = false;
        permAssert(false);
    }
    return pass;
}

void ReshapeExecStream::castOutput()
{
    outputProjAccessor.unmarshal(inputOutputData);
    for (uint i = 0; i < inputOutputData.size(); i++) {
        // truncate value if it exceeds the destination size
        uint len = std::min(
            inputOutputData[i].cbData, outputDesc[i].cbStorage);
        outputData[i].cbData = len;
        if (inputOutputData[i].pData) {
            outputData[i].pData = inputOutputData[i].pData;
        } else {
            outputData[i].pData = NULL;
        }
    }
}

void ReshapeExecStream::closeImpl()
{
    ConduitExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End ReshapeExecStream.cpp
