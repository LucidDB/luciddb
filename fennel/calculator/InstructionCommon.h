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

