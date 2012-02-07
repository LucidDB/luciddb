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
#include "fennel/common/ResourceDefinition.h"
#include "fennel/common/ResourceBundle.h"

#include <cassert>
#include <string>
#include <sstream>
#include <boost/format.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

using namespace std;

ResourceDefinition::ResourceDefinition(
    ResourceBundle *bundle,
    const string &key)
    : _key(key), _bundle(bundle)
{
    assert(!key.empty());
    assert(bundle != NULL);
}

ResourceDefinition::~ResourceDefinition()
{
}

// REVIEW jvs 18-Feb-2004:  Why the special case for no-args?
string ResourceDefinition::format() const
{
    if (_bundle->hasMessage(_key)) {
        return _bundle->getMessage(_key);
    } else {
        boost::format fmt("%1%.%2%.%3%()");
        return (fmt
                % _bundle->getBaseName()
                % _bundle->getLocale().getDisplayName()
                % _key).str();
    }
}


boost::format ResourceDefinition::prepareFormatter(int numArgs) const
{
    if (_bundle->hasMessage(_key)) {
        return boost::format(_bundle->getMessage(_key));
    } else {
        stringstream formatSpecifier;
        formatSpecifier << "%1%.%2%.%3%(";
        for (int i = 0; i < numArgs; i++) {
            if (i != 0) {
                formatSpecifier << ", ";
            }
            formatSpecifier << "%" << (i + 4) << "%";
        }
        formatSpecifier << ")";

        boost::format fmt(formatSpecifier.str());

        fmt
            % _bundle->getBaseName()
            % _bundle->getLocale().getDisplayName()
            % _key;

        // formatter is now ready for numArgs parameters
        return fmt;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End ResourceDefinition.cpp
