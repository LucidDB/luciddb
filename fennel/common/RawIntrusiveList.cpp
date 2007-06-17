/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
