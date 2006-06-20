/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2006 Disruptive Tech
// Copyright (C) 2005-2006 The Eigenbase Project
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

//! WinAggInit - Allocates and initializes the Accumulator object that is
//! a parameter to all subsequent Windowed Agg operations.
//!
//! INPUT:
//! result - ptr to RegisterRef to be used in later winagg calls
//
void WinAggInit(RegisterRef<char*>* result)
{
    TupleDatum *bind = result->getBinding(false);

    StandardTypeDescriptorOrdinal dType = 
        static_cast<StandardTypeDescriptorOrdinal>(bind->dataType);

    PBuffer histogramObject = NULL;
    if (StandardTypeDescriptor::isExact(dType)) {
        histogramObject = reinterpret_cast<PBuffer>(new WinAggHistogram<int64_t>);
    } else if (StandardTypeDescriptor::isApprox(dType)) {
        histogramObject = reinterpret_cast<PBuffer>(new WinAggHistogram<double>);
    } else {
        // TODO: find out what exception to throw`
        throw 22001;
    }

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
void add(RegisterRef<STDTYPE>* node, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    

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
void drop(RegisterRef<STDTYPE>* node, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));

    // Process the row
    pAcc->dropRow(node);
}

//! min - Template function that returns the current MIN value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the MIN value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
void min(RegisterRef<STDTYPE>* result, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *resBind = result->getBinding(false);
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));

    if (NULL == resBind)
    {
        ;
    }
    
    // return min value
    pAcc->getMin(result);
}

//! min - Template function that returns the current MAX value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the MAX value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
void max(RegisterRef<STDTYPE>* result, RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return min value
    pAcc->getMax(result);
}

//! avg - Template function that returns the current AVG value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the AVG value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
void avg(RegisterRef<STDTYPE>* result,
    RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return min value
    pAcc->getAvg(result);
}

//! sum - Template function that returns the current SUM value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the SUM value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
void sum(RegisterRef<STDTYPE>* result,
    RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return min value
    pAcc->getSum(result);
}

//! COUNT - Template function that returns the current COUNT value for the window
//! specified data type.
//!
//! INPUT:
//! result - Register returns the MIN value
//! aggDataBlock - Aggregation accumulator
template <typename STDTYPE>
void count(RegisterRef<STDTYPE>* result,
    RegisterRef<char*>* aggDataBlock)
{
    // cast otherData buffer pointer to our working structure
    TupleDatum *bind = aggDataBlock->getBinding(false);
    WinAggHistogram<STDTYPE> *pAcc =
        *(reinterpret_cast<WinAggHistogram<STDTYPE>**>(const_cast<PBuffer>(bind->pData)));
    
    // return min value
    pAcc->getCount(result);
}

// WinAggAdd, WinAggDrop, WinAggMin, WinAggMax, WinAggMin, WinAggSum
// WinAggCount, WinAggAvg
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

void WinAggCount(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    count(result, aggDataBlock);
}

void WinAggAvg(RegisterRef<int64_t>* result, RegisterRef<char*>* aggDataBlock)
{
    avg(result, aggDataBlock);
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


//
// Function that registers the Extended Instructions for windowed
// aggregation support.
//
void
ExtWinAggFuncRegister(ExtendedInstructionTable* eit)
{
    assert(eit != NULL);

    // First the generic initialization method
    vector<StandardTypeDescriptorOrdinal> params_mm_init;
    params_mm_init.push_back(STANDARD_TYPE_VARCHAR);

    eit->add("WINAGGINIT", params_mm_init,
        (ExtendedInstruction1<char*>*) NULL,
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

    // Add in the Add/Drop and funcstions for real numbers.
    vector<StandardTypeDescriptorOrdinal> params_ad_DOUBLE;
    params_ad_DOUBLE.push_back(STANDARD_TYPE_DOUBLE);
    params_ad_DOUBLE.push_back(STANDARD_TYPE_VARBINARY);

    eit->add("WinAggAdd", params_ad_DOUBLE,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggAdd);

    eit->add("WinAggDrop", params_ad_DOUBLE,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggDrop);

    vector<StandardTypeDescriptorOrdinal> params_DBL_funcs;
    params_DBL_funcs.push_back(STANDARD_TYPE_DOUBLE);
    params_DBL_funcs.push_back(STANDARD_TYPE_VARBINARY);

    eit->add("WinAggSum", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggSum);

    eit->add("WinAggAvg", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggAvg);

    eit->add("WinAggMin", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggMin);

    eit->add("WinAggMax", params_DBL_funcs,
             (ExtendedInstruction2<double, char*>*) NULL,
             &WinAggMax);
}


FENNEL_END_CPPFILE("$Id$");
        
