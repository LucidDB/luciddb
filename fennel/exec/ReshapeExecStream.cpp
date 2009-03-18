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

#include <hash_set>

FENNEL_BEGIN_CPPFILE("$Id$");

void ReshapeExecStream::prepare(ReshapeExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    TupleDescriptor const &inputDesc = pInAccessor->getTupleDesc();
    TupleAccessor &inputAccessor = pInAccessor->getConsumptionTupleAccessor();
    dynamicParameters.assign(
        params.dynamicParameters.begin(),
        params.dynamicParameters.end());

    compOp = params.compOp;
    if (compOp != COMP_NOOP) {
        initCompareData(params, inputDesc, inputAccessor);
    }

    // Setup the output projection that projects from the input tuple
    outputProjAccessor.bind(inputAccessor, params.outputProj);
    inputOutputDesc.projectFrom(inputDesc, params.outputProj);

    // Setup the output descriptor and data
    outputDesc = pOutAccessor->getTupleDesc();
    outputData.compute(outputDesc);

    // Determine how many of the dynamic parameters need to be written into
    // the output tuple
    uint numOutputDynParams = 0;
    for (uint i = 0; i < dynamicParameters.size(); i++) {
        if (dynamicParameters[i].outputParam) {
            numOutputDynParams++;
        }
    }

    // Setup the output descriptor and data that excludes dynamic parameters
    // in the output tuple
    assert(inputOutputDesc.size() == outputDesc.size() - numOutputDynParams);
    TupleDescriptor partialOutputDesc;
    if (numOutputDynParams == 0) {
        partialOutputDesc = outputDesc;
    } else if (inputOutputDesc.size() > 0) {
        partialOutputDesc.resize(inputOutputDesc.size());
        std::copy(
            outputDesc.begin(),
            outputDesc.end() - numOutputDynParams,
            partialOutputDesc.begin());
    }

    // determine if simple casting is required
    castRequired = (inputOutputDesc != partialOutputDesc);
    if (castRequired) {
        TupleProjection proj;
        if (compOp == COMP_NE) {
            proj = params.inputCompareProj;
        }
        assert(checkCastTypes(proj, inputOutputDesc, partialOutputDesc));
        inputOutputData.compute(inputOutputDesc);
    }
}

void ReshapeExecStream::initCompareData(
    ReshapeExecStreamParams const &params,
    TupleDescriptor const &inputDesc,
    TupleAccessor const &inputAccessor)
{
    // Setup the comparison tuple descriptor
    assert(params.inputCompareProj.size() > 0);
    TupleProjection inputCompareProj = params.inputCompareProj;
    compTupleDesc.projectFrom(inputDesc, inputCompareProj);
    // Adjust the descriptor to allow NULLs in case we're filtering out NULLs
    for (uint i = 0; i < compTupleDesc.size(); i++) {
        compTupleDesc[i].isNullable = true;
    }

    // Setup the projection of the columns for comparison
    inputCompareProjAccessor.bind(inputAccessor, inputCompareProj);
    inputCompareData.compute(compTupleDesc);

    // Setup a descriptor that excludes the dynamic parameters that will
    // be used in comparisons, if there are dynamic parameters.  The dynamic
    // parameters appear at the end of the descriptor.
    TupleDescriptor partialCompTupleDesc;
    numCompDynParams = 0;
    for (uint i = 0; i < dynamicParameters.size(); i++) {
        if (!isMAXU(dynamicParameters[i].compareOffset)) {
            numCompDynParams++;
        }
    }
    if (numCompDynParams > 0) {
        partialCompTupleDesc.resize(compTupleDesc.size() - numCompDynParams);
        std::copy(
            compTupleDesc.begin(),
            compTupleDesc.end() - numCompDynParams,
            partialCompTupleDesc.begin());
    }

    paramCompareData.compute(compTupleDesc);
    if (numCompDynParams == 0) {
        copyCompareTuple(
            compTupleDesc,
            paramCompareData,
            params.pCompTupleBuffer.get());
    } else if (partialCompTupleDesc.size() > 0) {
        TupleData partialCompareData;
        partialCompareData.compute(partialCompTupleDesc);
        copyCompareTuple(
            partialCompTupleDesc,
            partialCompareData,
            params.pCompTupleBuffer.get());

        // Copy the partial tuple data to the tuple data that will
        // be used in the actual comparisons
        std::copy(
            partialCompareData.begin(),
            partialCompareData.end(),
            paramCompareData.begin());
    }

    // Setup a tuple projection to project the last key for use in
    // non-equality comparisons
    lastKey.push_back(paramCompareData.size() - 1);
    lastKeyDesc.projectFrom(compTupleDesc, lastKey);
}

void ReshapeExecStream::copyCompareTuple(
    TupleDescriptor const &tupleDesc,
    TupleData &tupleData,
    PBuffer tupleBuffer)
{
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(tupleDesc);
    tupleAccessor.setCurrentTupleBuf(tupleBuffer);
    uint nBytes = tupleAccessor.getCurrentByteCount();
    compTupleBuffer.reset(new FixedBuffer[nBytes]);
    memcpy(compTupleBuffer.get(), tupleBuffer, nBytes);
    tupleAccessor.setCurrentTupleBuf(compTupleBuffer.get());
    tupleAccessor.unmarshal(tupleData);
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
            StoredTypeDescriptor::Ordinal inputType =
                inputTupleDesc[i].pTypeDescriptor->getOrdinal();
            StoredTypeDescriptor::Ordinal outputType =
                outputTupleDesc[i].pTypeDescriptor->getOrdinal();

            // can't convert between unicode and non-unicode;
            // normalize types to non-unicode to make other checks
            // easier, but verify that either both or neither are unicode
            bool inputUnicode = false;
            if (inputType == STANDARD_TYPE_UNICODE_CHAR) {
                inputType = STANDARD_TYPE_CHAR;
                inputUnicode = true;
            }
            if (inputType == STANDARD_TYPE_UNICODE_VARCHAR) {
                inputType = STANDARD_TYPE_VARCHAR;
                inputUnicode = true;
            }

            bool outputUnicode = false;
            if (outputType == STANDARD_TYPE_UNICODE_CHAR) {
                outputType = STANDARD_TYPE_CHAR;
                outputUnicode = true;
            }
            if (outputType == STANDARD_TYPE_UNICODE_VARCHAR) {
                outputType = STANDARD_TYPE_VARCHAR;
                outputUnicode = true;
            }

            if (inputUnicode || outputUnicode) {
                assert(inputUnicode && outputUnicode);
            }

            if (inputType != outputType) {
                // if types are different, must be casting from char to
                // varchar
                assert(
                    (inputType == STANDARD_TYPE_CHAR)
                    && (outputType == STANDARD_TYPE_VARCHAR));
            }
            if (inputTupleDesc[i].cbStorage != outputTupleDesc[i].cbStorage) {
                // if lengths are different, must be casting from char or
                // varchar to varchar
                assert(
                    ((inputType == STANDARD_TYPE_VARCHAR)
                        || (inputType == STANDARD_TYPE_CHAR))
                    && (outputType == STANDARD_TYPE_VARCHAR));
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
    paramsRead = false;
}

ExecStreamResult ReshapeExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (!paramsRead) {
        readDynamicParams();
        paramsRead = true;
    }

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

void ReshapeExecStream::readDynamicParams()
{
    uint currCompIdx = paramCompareData.size() - numCompDynParams;
    uint currOutputIdx = inputOutputDesc.size();
    for (uint i = 0; i < dynamicParameters.size(); i++) {
        if (!isMAXU(dynamicParameters[i].compareOffset)) {
            TupleDatum const &param =
                pDynamicParamManager->getParam(
                    dynamicParameters[i].dynamicParamId).getDatum();
            paramCompareData[currCompIdx++] = param;
        }
        if (dynamicParameters[i].outputParam) {
            TupleDatum const &param =
                pDynamicParamManager->getParam(
                    dynamicParameters[i].dynamicParamId).getDatum();
            outputData[currOutputIdx++] = param;
        }
    }
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
