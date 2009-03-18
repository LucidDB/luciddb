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
            switch(outAccessors[iOutput]->getState()) {
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

    switch(pInAccessor->getState()) {
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
