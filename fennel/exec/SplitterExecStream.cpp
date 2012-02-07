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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/SplitterExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SplitterExecStream::open(bool restart)
{
    DiffluenceExecStream::open(restart);
    iOutput = 0;
    pLastConsumptionEnd = NULL;
}

ExecStreamResult SplitterExecStream::execute(ExecStreamQuantum const &)
{
    if (pLastConsumptionEnd) {
        while (iOutput < outAccessors.size()) {
            switch (outAccessors[iOutput]->getState()) {
            case EXECBUF_NONEMPTY:
            case EXECBUF_OVERFLOW:
                return EXECRC_BUF_OVERFLOW;
            case EXECBUF_UNDERFLOW:
            case EXECBUF_EMPTY:
                ++iOutput;
                break;
            case EXECBUF_EOS:
                assert(pInAccessor->getState() == EXECBUF_EOS);
                return EXECRC_EOS;
            }
        }

        /*
         * All the output buf accessors have reached EXECBUF_EMPTY. This
         * means the downstream consumers must have consumed everything
         * up to the last byte we told them was available; pass that
         * information on to our upstream producer.
         */
        pInAccessor->consumeData(pLastConsumptionEnd);
        pLastConsumptionEnd = NULL;
        iOutput = 0;
    }

    switch (pInAccessor->getState()) {
    case EXECBUF_OVERFLOW:
    case EXECBUF_NONEMPTY:
        if (!pLastConsumptionEnd) {
            pLastConsumptionEnd = pInAccessor->getConsumptionEnd();

            /*
             * The same buffer is provided for consumption to all the output
             * buffer accessors.
             */
            for (int i = 0; i < outAccessors.size(); i ++) {
                outAccessors[i]->provideBufferForConsumption(
                    pInAccessor->getConsumptionStart(),
                    pLastConsumptionEnd);
            }
        }
        return EXECRC_BUF_OVERFLOW;
    case EXECBUF_UNDERFLOW:
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EMPTY:
        pInAccessor->requestProduction();
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        for (int i = 0; i < outAccessors.size(); i ++) {
            outAccessors[i]->markEOS();
        }
        return EXECRC_EOS;
    default:
        permAssert(false);
    }
}

ExecStreamBufProvision SplitterExecStream::getOutputBufProvision() const
{
    /*
     * Splitter does not own any buffer; however, it provides its producer's
     * buffer directly to its consumer(s).
     */
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End SplitterExecStream.cpp
