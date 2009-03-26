/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
#ifndef Fennel_InstructionCommon_Included
#define Fennel_InstructionCommon_Included

//! \file InstructionCommon.h
//! External users should include this file if they intend to manipulate
//! instructions directly, otherwise only include CalcCommon.h


#include "fennel/calculator/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include "fennel/calculator/BoolInstruction.h"
#include "fennel/calculator/JumpInstruction.h"
#include "fennel/calculator/NativeInstruction.h"
#include "fennel/calculator/NativeNativeInstruction.h"
#include "fennel/calculator/BoolNativeInstruction.h"
#include "fennel/calculator/IntegralNativeInstruction.h"
#include "fennel/calculator/PointerInstruction.h"
#include "fennel/calculator/PointerPointerInstruction.h"
#include "fennel/calculator/BoolPointerInstruction.h"
#include "fennel/calculator/IntegralPointerInstruction.h"
#include "fennel/calculator/PointerIntegralInstruction.h"
#include "fennel/calculator/ReturnInstruction.h"
#include "fennel/calculator/CastInstruction.h"

#include "fennel/calculator/ExtendedInstruction.h"
#include "fennel/calculator/ExtendedInstructionTable.h"
#include "fennel/calculator/ExtString.h"
#include "fennel/calculator/ExtRegExp.h"
#include "fennel/calculator/ExtMath.h"
#include "fennel/calculator/ExtDateTime.h"
#include "fennel/calculator/ExtCast.h"
#include "fennel/calculator/ExtDynamicVariable.h"
#include "fennel/calculator/ExtWinAggFuncs.h"

#endif

// End InstructionCommon.h

