/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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


#include "fennel/disruptivetech/calc/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include "fennel/disruptivetech/calc/BoolInstruction.h"
#include "fennel/disruptivetech/calc/JumpInstruction.h"
#include "fennel/disruptivetech/calc/NativeInstruction.h"
#include "fennel/disruptivetech/calc/NativeNativeInstruction.h"
#include "fennel/disruptivetech/calc/BoolNativeInstruction.h"
#include "fennel/disruptivetech/calc/IntegralNativeInstruction.h"
#include "fennel/disruptivetech/calc/PointerInstruction.h"
#include "fennel/disruptivetech/calc/PointerPointerInstruction.h"
#include "fennel/disruptivetech/calc/BoolPointerInstruction.h"
#include "fennel/disruptivetech/calc/IntegralPointerInstruction.h"
#include "fennel/disruptivetech/calc/PointerIntegralInstruction.h"
#include "fennel/disruptivetech/calc/ReturnInstruction.h"
#include "fennel/disruptivetech/calc/CastInstruction.h"

#include "fennel/disruptivetech/calc/ExtendedInstruction.h"
#include "fennel/disruptivetech/calc/ExtendedInstructionTable.h"
#include "fennel/disruptivetech/calc/ExtString.h"
#include "fennel/disruptivetech/calc/ExtRegExp.h"
#include "fennel/disruptivetech/calc/ExtMath.h"
#include "fennel/disruptivetech/calc/ExtDateTime.h"
#include "fennel/disruptivetech/calc/ExtCast.h"
#include "fennel/disruptivetech/calc/ExtDynamicVariable.h"
#include "fennel/disruptivetech/calc/ExtWinAggFuncs.h"

#endif

// End InstructionCommon.h

