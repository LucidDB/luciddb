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
#include "fennel/calculator/StringToHex.h"

#include <string>
#include <sstream>
#include <iomanip>

FENNEL_BEGIN_CPPFILE("$Id$");

using namespace fennel;
using namespace std;

// returns a hex-coded string
string
stringToHex(char const * const buf)
{
    assert(buf != NULL);
    uint buflen = strlen(buf);
    return stringToHex(buf, buflen);
}

string
stringToHex(string const & s)
{
    return stringToHex(s.c_str());
}

string
stringToHex(char const * const buf, uint buflen)
{
    assert(buf != NULL);
    ostringstream ostr;
    for (uint i = 0; i < buflen; i++) {
        unsigned char ch = (unsigned char) buf[i];
        ostr << hex << setw(2) << setfill('0') << (uint) ch;
    }
    return ostr.str();
}

FENNEL_END_CPPFILE("$Id$");

// End StringToHex.cpp
