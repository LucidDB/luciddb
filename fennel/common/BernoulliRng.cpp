/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2007-2009 The Eigenbase Project
// Copyright (C) 2007-2009 SQLstream, Inc.
// Copyright (C) 2007-2009 LucidEra, Inc.
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
#include "BernoulliRng.h"

FENNEL_BEGIN_CPPFILE("$Id$");

BernoulliRng::BernoulliRng(float successProbability)
    : bernoulliDist(successProbability), rng(uniformRng, bernoulliDist)
{
}


void BernoulliRng::reseed(uint32_t seed)
{
    uniformRng.seed(seed);
}

bool BernoulliRng::nextValue()
{
    return rng();
}

FENNEL_END_CPPFILE("$Id$");

// End BernoulliRng.cpp
