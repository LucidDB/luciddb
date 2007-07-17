/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 Disruptive Tech
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/disruptivetech/calc/ExtendedInstructionTable.h"
#include "fennel/disruptivetech/calc/WinAggHistogram.h"
#include "fennel/disruptivetech/calc/WinAggHistogramStrA.h"
#include "fennel/disruptivetech/calc/RegisterReference.h"
#include "fennel/tuple/StandardTypeDescriptor.h"


FENNEL_BEGIN_CPPFILE("$Id$");

/**
 * This file contains the C funcstions that act as the interface bewteen the
 * calculator proper and the extended instructions that are the windowed
 * aggregation support.  The functions really are to define the different
 * data types supported.  The real work is done in the WinAggHistogram object.
 *
 * @author JFrost
 * @version $Id$
 */

//! histogramAlloc  - Allocates and initializes the histogram object that is
//! a parameter to all subsequent Windowed Agg operations.
//!
//! INPUT:
//! result - ptr to RegisterRef to be used in later winagg calls
//! targetDataType - RegisterReference who's sole purpose is to provide the base
//!                  data type for histogram allocation
//
void histogramAlloc(RegisterRef<char*>* result, RegisterReference* targetDataType)
{

    StandardTypeDescriptorOrdinal dType = targetDataType->type();

    PBuffer histogramObject = NULL;
    if (StandardTypeDescriptor::isExact(dType)) {
        histogramObject = reinterpret_cast<PBuffer>(new WinAggHistogram<int64_t>);
    } else if (StandardTypeDescriptor::isApprox(dType)) {
        histogramObject = reinterpret_cast<PBuffer>(new WinAggHistogram<double>);
    } else if (StandardTypeDescriptor::isArray(dType)){
        histogramObject = reinterpret_cast<PBuffer>(new WinAggHistogramStrA);
    } else {
        // TODO: find out what exception to throw
        throw 22001;
    }

    TupleDatum *bind = result->getBinding(false);
    *(reinterpret_cast<PBuffer*>(const_cast<PBuffer>(bind->pData))) =
        histogramObject;
}

//! add - Template function that implements the ADD row function for
//! the specified data type.
//!
//! INPUT:
//! node - Register with new data to be added to window
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void add(RegisterRef<STDTYPE>* node, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    

    // Process the new node. 
    pAcc->addRow(node);
}

inline void add(RegisterRef<char*>* node, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogramStrA *pAcc =
        *(reinterpret_cast<WinAggHistogramStrA**>(const_cast<PBuffer>(bind->pData)));

    // Process the new node. 
    pAcc->addRow(node);
}


//! drop - Template function that implements the DROP row function for the
//! specified data type.
//!
//! INPUT:
//! node - Register with data to be removed.  NOTE:The tuple holding the
//!        data value must be the same one submitted to ADD
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void drop(RegisterRef<STDTYPE>* node, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));

    // Process the row
    pAcc->dropRow(node);
}

inline void drop(RegisterRef<char*>* node, RegisterRef<char*>* aggDataBlock)
{
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogramStrA *pAcc =
        *(reinterpret_cast<WinAggHistogramStrA**>(const_cast<PBuffer>(bind->pData)));
    
    pAcc->dropRow(node);
}

//! min - Template function that returns the current MIN value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the MIN value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void min(RegisterRef<STDTYPE>* result, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    // return min value
    pAcc->getMin(result);
}

inline void min(RegisterRef<char*>* result, RegisterRef<char*>* aggDataBlock)
{
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogramStrA *pAcc =
        *(reinterpret_cast<WinAggHistogramStrA**>(const_cast<PBuffer>(bind->pData)));
    pAcc->getMin(result);
}

//! min - Template function that returns the current MAX value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the MAX value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void max(RegisterRef<STDTYPE>* result, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return max value
    pAcc->getMax(result);
}

inline void max(RegisterRef<char*>* result, RegisterRef<char*>* aggDataBlock)
{
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogramStrA *pAcc =
        *(reinterpret_cast<WinAggHistogramStrA**>(const_cast<PBuffer>(bind->pData)));
    pAcc->getMax(result);
}

//! avg - Template function that returns the current AVG value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the AVG value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void avg(
    RegisterRef<STDTYPE>* result,
    RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return average
    pAcc->getAvg(result);
}

//! sum - Template function that returns the current SUM value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the SUM value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void sum(
    RegisterRef<STDTYPE>* result,
    RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return sum
    pAcc->getSum(result);
}

//! COUNT - Template function that returns the current COUNT value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the MIN value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void count(
    RegisterRef<STDTYPE>* result,
    RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return count
    pAcc->getCount(result);
}

// WinAggInit and WinAggCount do not need to be overloaded for each type
// of data.  Interfaces are fixed.
template <typename TDT>
void WinAggInit(RegisterRef<char*>* result, RegisterRef<TDT>* targetDataType)
{
    histogramAlloc(result, targetDataType);
}

//! firstValue - Template function that returns the first value which entered
//! the window
//!
//! INPUT:
//! result - Register returns the MIN value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void firstValue(
    RegisterRef<STDTYPE>* result,
    RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return last value
    pAcc->getFirstValue(result);
}

inline void firstValue(RegisterRef<char*>* result, RegisterRef<char*>* aggDataBlock)
{
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogramStrA *pAcc =
        *(reinterpret_cast<WinAggHistogramStrA**>(const_cast<PBuffer>(bind->pData)));
    pAcc->getFirstValue(result);
}

//! lastValue - Template function that returns the last value which entered
//! the window
//!
//! INPUT:
//! result - Register returns the MIN value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
inline void lastValue(
    RegisterRef<STDTYPE>* result,
    RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return first value
    pAcc->getLastValue(result);
}

inline void lastValue(RegisterRef<char*>* result, RegisterRef<char*>* aggDataBlock)
{
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogramStrA *pAcc =
        *(reinterpret_cast<WinAggHistogramStrA**>(const_cast<PBuffer>(bind->pData)));
    pAcc->getLastValue(result);
}

void WinAggCount(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    count(result, aggDataBlock);
}

// WinAggAdd, WinAggDrop, WinAggMin, WinAggMax, WinAggMin, WinAggSum,
// WinAggAvg
//
// The Following section contains all the function entry points that are
// registered with the calculator to support the INT64_t and DOUBLE data
// types. All parameters match and are passed through to the associated
// template functions defined above.

void WinAggAdd(RegisterRef<int64_t>* node, RegisterRef<char*>* aggDataBlock)
{
    add(node, aggDataBlock);
}

void WinAggDrop(RegisterRef<int64_t>* node, RegisterRef<char*>* aggDataBlock)
{
    drop(node, aggDataBlock);
}

void WinAggMin(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    min(result, aggDataBlock);
}

void WinAggMax(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    max(result, aggDataBlock);
}

void WinAggSum(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    sum(result, aggDataBlock);
}

void WinAggAvg(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    avg(result, aggDataBlock);
}

void WinAggFirstValue(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    firstValue(result, aggDataBlock);
}

void WinAggLastValue(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    lastValue(result, aggDataBlock);
}

//
// Double(real number) interface
//
void WinAggAdd(RegisterRef<double>* node, RegisterRef<char*>* aggDataBlock)
{
    add(node, aggDataBlock);
}

void WinAggDrop(RegisterRef<double>* node, RegisterRef<char*>* aggDataBlock)
{
    drop(node, aggDataBlock);
}

void WinAggAvg(RegisterRef<double>* result, RegisterRef<char*>* aggDataBlock)
{
    avg(result, aggDataBlock);
}

void WinAggSum(RegisterRef<double>* result, RegisterRef<char*>* aggDataBlock)
{
    sum(result, aggDataBlock);
}

void WinAggMin(RegisterRef<double>* result, RegisterRef<char*>* aggDataBlock)
{
    min(result, aggDataBlock);
}

void WinAggMax(RegisterRef<double>* result, RegisterRef<char*>* aggDataBlock)
{
    max(result, aggDataBlock);
}

void WinAggFirstValue(RegisterRef<double>* result, RegisterRef<char*>* aggDataBlock)
{
    firstValue(result, aggDataBlock);
}

void WinAggLastValue(RegisterRef<double>* result, RegisterRef<char*>* aggDataBlock)
{
    lastValue(result, aggDataBlock);
}

//
// Ascii interface  Note: Avg and Sum are not applicable to string data.
//
void WinAggAdd(RegisterRef<char*>* node, RegisterRef<char*>* aggDataBlock)
{
    add( node,aggDataBlock);
}

void WinAggDrop(RegisterRef<char*>* node, RegisterRef<char*>* aggDataBlock)
{
    drop( node,aggDataBlock);
}


void WinAggMin(RegisterRef<char*>* node, RegisterRef<char*>* aggDataBlock)
{
    min( node,aggDataBlock);
}

void WinAggMax(RegisterRef<char*>* node, RegisterRef<char*>* aggDataBlock)
{
    max( node,aggDataBlock);
}

void WinAggFirstValue(RegisterRef<char*>* node, RegisterRef<char*>* aggDataBlock)
{
    firstValue(node, aggDataBlock);
}

void WinAggLastValue(RegisterRef<char*>* node, RegisterRef<char*>* aggDataBlock)
{
    lastValue(node, aggDataBlock);
}

//
// Function that registers the Extended Instructions for windowed
// aggregation support.
//
void
ExtWinAggFuncRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);

    // WinAggInit.  Overloaded for each supported data
    // type.  Note that we have to have a separate init
    // for each data type but the other support functions
    // only have to support int64_t and double.  When the 
    // calc programs are built the small types are cast up
    // to the supported types.
    vector<StandardTypeDescriptorOrdinal> params_mm64_init;
    params_mm64_init.push_back(STANDARD_TYPE_VARBINARY);
    params_mm64_init.push_back(STANDARD_TYPE_INT_64);

    eit->add("WinAggInit", params_mm64_init,
        (ExtendedInstruction2<char*,int64_t>*) NULL,
        &WinAggInit);

    vector<StandardTypeDescriptorOrdinal> params_mm32_init;
    params_mm32_init.push_back(STANDARD_TYPE_VARBINARY);
    params_mm32_init.push_back(STANDARD_TYPE_INT_32);

    eit->add("WinAggInit", params_mm32_init,
        (ExtendedInstruction2<char*,int32_t>*) NULL,
        &WinAggInit);

    vector<StandardTypeDescriptorOrdinal> params_mm16_init;
    params_mm16_init.push_back(STANDARD_TYPE_VARBINARY);
    params_mm16_init.push_back(STANDARD_TYPE_INT_16);

    eit->add("WinAggInit", params_mm16_init,
        (ExtendedInstruction2<char*,int16_t>*) NULL,
        &WinAggInit);

    vector<StandardTypeDescriptorOrdinal> params_mm8_init;
    params_mm8_init.push_back(STANDARD_TYPE_VARBINARY);
    params_mm8_init.push_back(STANDARD_TYPE_INT_8);

    eit->add("WinAggInit", params_mm8_init,
        (ExtendedInstruction2<char*,int8_t>*) NULL,
        &WinAggInit);

    // Now the Add/Drop and functions for integers
    vector<StandardTypeDescriptorOrdinal> params_ad_I64;
    params_ad_I64.push_back(STANDARD_TYPE_INT_64);
    params_ad_I64.push_back(STANDARD_TYPE_VARBINARY);
    
    eit->add("WinAggAdd", params_ad_I64,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggAdd);

    eit->add("WinAggDrop", params_ad_I64,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggDrop);

    vector<StandardTypeDescriptorOrdinal> params_I64_funcs;
    params_I64_funcs.push_back(STANDARD_TYPE_INT_64);
    params_I64_funcs.push_back(STANDARD_TYPE_VARBINARY);

    eit->add("WinAggSum", params_I64_funcs,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggSum);

    eit->add("WinAggCount", params_I64_funcs,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggCount);

    eit->add("WinAggAvg", params_I64_funcs,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggAvg);

    eit->add("WinAggMin", params_I64_funcs,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggMin);

    eit->add("WinAggMax", params_I64_funcs,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggMax);

    eit->add("WinAggFirstValue", params_I64_funcs,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggFirstValue);

    eit->add("WinAggLastValue", params_I64_funcs,
             (ExtendedInstruction2<int64_t, char*>*) NULL,
             &WinAggLastValue);

    // Add in  real number support
    vector<StandardTypeDescriptorOrdinal> params_mmd_init;
    params_mmd_init.push_back(STANDARD_TYPE_VARBINARY);
    params_mmd_init.push_back(STANDARD_TYPE_DOUBLE);
    
    eit->add("WinAggInit", params_mmd_init,
        (ExtendedInstruction2<char*,double>*) NULL,
        &WinAggInit);
    
    vector<StandardTypeDescriptorOrdinal> params_mmr_init;
    params_mmr_init.push_back(STANDARD_TYPE_VARBINARY);
    params_mmr_init.push_back(STANDARD_TYPE_REAL);
    
    eit->add("WinAggInit", params_mmr_init,
        (ExtendedInstruction2<char*,float>*) NULL,
        &WinAggInit);
    
    vector<StandardTypeDescriptorOrdinal> params_DBL_funcs;
    params_DBL_funcs.push_back(STANDARD_TYPE_DOUBLE);
    params_DBL_funcs.push_back(STANDARD_TYPE_VARBINARY);

    eit->add("WinAggAdd", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggAdd);

    eit->add("WinAggDrop", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggDrop);

    eit->add("WinAggMin", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggMin);

    eit->add("WinAggMax", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggMax);

    eit->add("WinAggSum", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggSum);

    eit->add("WinAggAvg", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggAvg);

    eit->add("WinAggFirstValue", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggFirstValue);

    eit->add("WinAggLastValue", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggLastValue);

    // support for CHAR and VARCHAR
    vector<StandardTypeDescriptorOrdinal> params_mmvc_init;
    params_mmvc_init.push_back(STANDARD_TYPE_VARBINARY);
    params_mmvc_init.push_back(STANDARD_TYPE_VARCHAR);
    
    eit->add("WinAggInit", params_mmvc_init,
        (ExtendedInstruction2<char*,char*>*) NULL,
        &WinAggInit);
    
    vector<StandardTypeDescriptorOrdinal> params_StrA_funcs;
    params_StrA_funcs.push_back(STANDARD_TYPE_VARCHAR);
    params_StrA_funcs.push_back(STANDARD_TYPE_VARBINARY);

    eit->add("WinAggAdd", params_StrA_funcs,
             (ExtendedInstruction2<char *, char*>*) NULL,
             &WinAggAdd);

    eit->add("WinAggDrop", params_StrA_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggDrop);

    eit->add("WinAggMin", params_StrA_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggMin);

    eit->add("WinAggMax", params_StrA_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggMax);

    eit->add("WinAggFirstValue", params_StrA_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggFirstValue);

    eit->add("WinAggLastValue", params_StrA_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggLastValue);

    vector<StandardTypeDescriptorOrdinal> params_mmc_init;
    params_mmc_init.push_back(STANDARD_TYPE_VARBINARY);
    params_mmc_init.push_back(STANDARD_TYPE_CHAR);
    
    eit->add("WinAggInit", params_mmc_init,
        (ExtendedInstruction2<char*,char*>*) NULL,
        &WinAggInit);
    
    vector<StandardTypeDescriptorOrdinal> params_StrA2_funcs;
    params_StrA2_funcs.push_back(STANDARD_TYPE_CHAR);
    params_StrA2_funcs.push_back(STANDARD_TYPE_VARBINARY);

    eit->add("WinAggAdd", params_StrA2_funcs,
             (ExtendedInstruction2<char *, char*>*) NULL,
             &WinAggAdd);

    eit->add("WinAggDrop", params_StrA2_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggDrop);

    eit->add("WinAggMin", params_StrA2_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggMin);

    eit->add("WinAggMax", params_StrA2_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggMax);

    eit->add("WinAggFirstValue", params_StrA2_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggFirstValue);

    eit->add("WinAggLastValue", params_StrA2_funcs,
             (ExtendedInstruction2<char*, char*>*) NULL,
             &WinAggLastValue);
}


FENNEL_END_CPPFILE("$Id$");
        
