/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2009 The Eigenbase Project
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
//
// Test Calculator object directly by instantiating instruction objects,
// creating programs, running them, and checking the register set values.
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/AttributeAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/common/TraceSource.h"

#include <boost/scoped_array.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <limits>
#include <iostream.h>

using namespace fennel;

int const bufferlen = 8;
int const num = 5;

void
tupleFiddle()
{
  bool isNullable = true;    // Can tuple contain nulls?
  int i;

  TupleDescriptor tupleDesc;
  tupleDesc.clear();

  // Build up a description of what we'd like the tuple to look like
  StandardTypeDescriptorFactory typeFactory;
  for (i = 0; i < num; i++) {
    StoredTypeDescriptor const &typeDesc =
        typeFactory.newDataType(STANDARD_TYPE_VARCHAR);
    tupleDesc.push_back(
        TupleAttributeDescriptor(
            typeDesc,
            isNullable,
            bufferlen));
  }
  for (i = 0; i < num; i++) {
    StoredTypeDescriptor const &typeDesc =
        typeFactory.newDataType(STANDARD_TYPE_INT_32);
    tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
  }
  for (i = 0; i < num; i++) {
    StoredTypeDescriptor const &typeDesc =
        typeFactory.newDataType(STANDARD_TYPE_UINT_8);
    tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
  }
  for (i = 0; i < num; i++) {
    StoredTypeDescriptor const &typeDesc =
        typeFactory.newDataType(STANDARD_TYPE_REAL);
    tupleDesc.push_back(TupleAttributeDescriptor(typeDesc, isNullable));
  }

  // Create a tuple accessor from the description
  //
  // Note: Must use an ALL_FIXED accessor when creating a tuple out of
  // the air like this, otherwise unmarshal() does not know what to
  // do. If you need a STANDARD type tuple with variable-lengty
  // fields, it has to be built as a copy.
  TupleAccessor tupleAccessorFixed;
  tupleAccessorFixed.compute(tupleDesc, TUPLE_FORMAT_ALL_FIXED);

  // Allocate memory for the tuple
  boost::scoped_array<FixedBuffer>
    pTupleBufFixed(new FixedBuffer[tupleAccessorFixed.getMaxByteCount()]);

  // Link memory to accessor
  tupleAccessorFixed.setCurrentTupleBuf(pTupleBufFixed.get(), false);

  // Create a vector of TupleDatum objects based on the description we built
  TupleData tupleDataFixed(tupleDesc);

  // Do something mysterious. Probably binding pointers in the accessor to items
  // in the TupleData vector
  tupleAccessorFixed.unmarshal(tupleDataFixed);

  TupleData::iterator itr = tupleDataFixed.begin();

  TupleDatum pDatum;
  PBuffer pData;

  for (i = 0; i < num; i++, itr++) {
      char buf[bufferlen * 10];
      sprintf(buf,"%d-A-%d-B-%d-C-", i, i, i); // longer than buflen
      strncpy(
          (reinterpret_cast<char *>(const_cast<PBuffer>(itr->pData))),
          buf, bufferlen);
  }
  for (i = 0; i < num; i++, itr++) {
    // exploded form
    pDatum = *itr;
    pData = const_cast<PBuffer>(pDatum.pData);
    *(reinterpret_cast<int32_t *>(pData)) = i;
  }
  for (i = 0; i < num; i++, itr++) {
    // condensed form
    *(reinterpret_cast<uint8_t *>(const_cast<PBuffer>(itr->pData))) = i;
  }
  for (i = 0; i < num; i++, itr++) {
    *(reinterpret_cast<float *>(const_cast<PBuffer>(itr->pData))) = i * 0.5;
  }

  // Print out the tuple
  TuplePrinter tuplePrinter;
  tuplePrinter.print(cout, tupleDesc, tupleDataFixed);
  cout << endl;

  // Create another TupleData object that will be nullable
  TupleData tupleDataNullable = tupleDataFixed;

  // null out last element of each type
  for (i = 1; i <= 3; i++) {
    tupleDataNullable[(i*num)-1].pData = NULL;
  }

  // Print out the nullable tuple
  tuplePrinter.print(cout, tupleDesc, tupleDataNullable);
  cout << endl;

  // Re-point second string to first string
  tupleDataNullable[1].pData = tupleDataNullable[0].pData;

  // Re-point third string to part way into first string
  tupleDataNullable[2].pData = (tupleDataNullable[0].pData + 1);

  // Print out the modified nullable tuple
  tuplePrinter.print(cout, tupleDesc, tupleDataNullable);
  cout << endl;

}

int
main(int argc, char *argv[])
{
  tupleFiddle();
  return 0;
}

boost::unit_test_framework::test_suite *init_unit_test_suite(int,char **)
{
    return NULL;
}

// End tuple.cpp
