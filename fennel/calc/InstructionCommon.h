/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
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
//
// InstructionCommon.h
// External users should Include this file if they intend to manipulate
// instructions directly, otherwise only include CalcCommon.h
//
*/
#ifndef Fennel_InstructionCommon_Included
#define Fennel_InstructionCommon_Included

#include "fennel/calc/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include "fennel/calc/BoolInstruction.h"
#include "fennel/calc/JumpInstruction.h"
#include "fennel/calc/NativeInstruction.h"
#include "fennel/calc/NativeNativeInstruction.h"
#include "fennel/calc/BoolNativeInstruction.h"
#include "fennel/calc/IntegralNativeInstruction.h"
#include "fennel/calc/PointerInstruction.h"
#include "fennel/calc/PointerPointerInstruction.h"
#include "fennel/calc/BoolPointerInstruction.h"
#include "fennel/calc/IntegralPointerInstruction.h"
#include "fennel/calc/PointerIntegralInstruction.h"
#include "fennel/calc/ReturnInstruction.h"
#include "fennel/calc/ExtendedInstruction.h"

#endif

// End InstructionCommon.h

