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
#include "fennel/calculator/SqlState.h"

FENNEL_BEGIN_CPPFILE("$Id$");

// static
SqlState SqlState::_instance;

const SqlState &SqlState::instance()
{
    return _instance;
}

SqlState::SqlState()
    : _code21000("21000"),
      _code22000("22000"),
      _code22001("22001"),
      _code22003("22003"),
      _code22004("22004"),
      _code22007("22007"),
      _code2200B("2200B"),
      _code2200C("2200C"),
      _code22011("22011"),
      _code22012("22012"),
      _code22018("22018"),
      _code22019("22019"),
      _code2201B("2201B"),
      _code2201E("2201E"),
      _code2201F("2201F"),
      _code22023("22023"),
      _code22025("22025"),
      _code22027("22027")
{
    _map["21000"] = &_code21000;
    _map["22000"] = &_code22000;
    _map["22001"] = &_code22001;
    _map["22003"] = &_code22003;
    _map["22004"] = &_code22004;
    _map["22007"] = &_code22007;
    _map["2200B"] = &_code2200B;
    _map["2200C"] = &_code2200C;
    _map["22011"] = &_code22011;
    _map["22012"] = &_code22012;
    _map["22018"] = &_code22018;
    _map["22019"] = &_code22019;
    _map["2201B"] = &_code2201B;
    _map["2201E"] = &_code2201E;
    _map["2201F"] = &_code2201F;
    _map["22023"] = &_code22023;
    _map["22025"] = &_code22025;
    _map["22027"] = &_code22027;
}

SqlStateInfo::SqlStateInfo(const char *code)
    : _code(code)
{
}

std::string SqlStateInfo::str() const
{
    return _code;
}

SqlStateInfo const *SqlState::lookup(const char *code) const
{
    return _map.find(std::string(code))->second;
}

FENNEL_END_CPPFILE("$Id$");

// End SqlState.cpp
