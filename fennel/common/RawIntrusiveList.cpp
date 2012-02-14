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
#include "fennel/common/RawIntrusiveList.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void RawIntrusiveList::push_front(IntrusiveListNode &listNode)
{
    assert(!listNode.pNext);
    listNode.pNext = pFront;
    pFront = &listNode;
    if (!pBack) {
        pBack = pFront;
    }
    nNodes++;
}

void RawIntrusiveList::push_back(IntrusiveListNode &listNode)
{
    assert(!listNode.pNext);
    listNode.pNext = NULL;
    if (pBack) {
        pBack = pBack->pNext = &listNode;
    } else {
        pFront = pBack = &listNode;
    }
    nNodes++;
}

void RawIntrusiveList::clear(bool debugClear)
{
    nNodes = 0;
#ifdef DEBUG
    if (debugClear) {
        IntrusiveListNode *p = pFront;
        while (p) {
            IntrusiveListNode *pNext = p->pNext;
            p->pNext = NULL;
            p = pNext;
        }
    }
#endif
    pFront = pBack = NULL;
}

bool RawIntrusiveList::remove(IntrusiveListNode &listNode)
{
    for (RawIntrusiveListMutator iter(*this); iter.getCurrent(); ++iter) {
        if (iter.getCurrent() == &listNode) {
            iter.detach();
            return 1;
        }
    }
    return 0;
}

void RawIntrusiveListMutator::promoteCurrToFront()
{
    assert(pCurr);
    if (pList->pFront == pCurr) {
        return;
    }
    if (pList->pBack == pCurr) {
        pList->pBack = pPrev;
    }
    pPrev->pNext = pCurr->pNext;
    pCurr->pNext = pList->pFront;
    pList->pFront = pCurr;
    pCurr = pPrev->pNext;
    bJustDeleted = 1;
}

void RawIntrusiveListMutator::demoteCurrToBack()
{
    assert(pCurr);
    if (pList->pBack == pCurr) {
        return;
    }
    if (pPrev) {
        pPrev->pNext = pCurr->pNext;
    } else {
        pList->pFront = pCurr->pNext;
    }
    pCurr->pNext = NULL;
    pList->pBack->pNext = pCurr;
    pList->pBack = pCurr;
    if (pPrev) {
        pCurr = pPrev->pNext;
    } else {
        pCurr = pList->pFront;
    }
    bJustDeleted = 1;
}

void RawIntrusiveListMutator::demoteFrontBeforeCurr()
{
    assert(pCurr);
    if (!pPrev) {
        return;
    }
    IntrusiveListNode *pOldFront = pList->pFront;
    if (pOldFront == pPrev) {
        return;
    }
    pList->pFront = pOldFront->pNext;
    pPrev = pPrev->pNext = pOldFront;
    pOldFront->pNext = pCurr;
}

IntrusiveListNode *RawIntrusiveListMutator::detach()
{
    assert(pCurr);
    IntrusiveListNode *pListNode = pCurr;
    if (pCurr == pList->pBack) {
        pList->pBack = pPrev;
    }
    if (pPrev) {
        pPrev->pNext = pCurr->pNext;
        pCurr = pPrev->pNext;
    } else {
        pList->pFront = pCurr->pNext;
        pCurr = pList->pFront;
    }
    pList->nNodes--;
    bJustDeleted = 1;
#ifdef DEBUG
    pListNode->pNext = NULL;
#endif
    return pListNode;
}

FENNEL_END_CPPFILE("$Id$");

// End RawIntrusiveList.cpp
