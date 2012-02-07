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
#include "fennel/common/Locale.h"

#include <string>
#include <cassert>

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");


#define DEFAULT_LOCALE_DISPLAY ("[default]")


// TODO: base this on the system
Locale Locale::DEFAULT = Locale("en", "US");


Locale::Locale(const string &language)
    : _lang(language)
{
    assert(language.empty() || language.length() == 2);
}

Locale::Locale(const string &language, const string &country)
    : _lang(language), _country(country)
{
    // either both language and country are empty or language must be non-empty
    assert((language.empty() && country.empty()) || !language.empty());

    assert(language.empty() || language.length() == 2);
    assert(country.empty() || country.length() == 2);
}

Locale::Locale(
    const string &language,
    const string &country,
    const string &variant)
    : _lang(language), _country(country), _variant(variant)
{
    // either language, country and variant are empty or language must
    // be non-empty
    assert(
        (language.empty()
         && country.empty()
         && variant.empty())
        || !language.empty());

    assert(language.empty() || language.length() == 2);
    assert(country.empty() || country.length() == 2);
}

Locale::~Locale()
{
}

bool Locale::operator==(const Locale &rhs) const
{
    return _lang == rhs._lang
        && _country == rhs._country
        && _variant == rhs._variant;
}

bool Locale::operator!=(const Locale &rhs) const {
    return _lang != rhs._lang
        || _country != rhs._country
        || _variant != rhs._variant;
}

bool Locale::operator<(const Locale &rhs) const
{
    int langCmp = this->getLanguage().compare(rhs.getLanguage());
    if (langCmp < 0) {
        return true;
    }
    if (langCmp > 0) {
        return false;
    }

    // same language, check country
    int countryCmp = this->getCountry().compare(rhs.getCountry());
    if (countryCmp < 0) {
        return true;
    }
    if (countryCmp > 0) {
        return false;
    }

    // same country, check variant
    int variantCmp = this->getVariant().compare(rhs.getVariant());
    if (variantCmp < 0) {
        return true;
    }
    return false;
}

const string &Locale::getLanguage() const
{
    return _lang;
}

const string &Locale::getCountry() const
{
    return _country;
}

const string &Locale::getVariant() const
{
    return _variant;
}

string Locale::getDisplayName() const
{
    if (_lang.empty()) {
        return DEFAULT_LOCALE_DISPLAY;
    }

    return
        _lang
        + (_country.empty() ? "" : (string("_") + _country))
        + (_variant.empty() ? "" : (string("_") + _variant));
}

bool Locale::hasParentLocale() const
{
    return !(_country.empty() && _variant.empty());
}

Locale Locale::getParentLocale() const
{
    if (hasParentLocale()) {
        // one or both of _country and _variant is non-empty
        if (_country.empty() ^ _variant.empty()) {
            return Locale(_lang);
        }

        return Locale(_lang, _country);
    } else {
        // TODO: gotta give them something -- maybe a locale with no language?
        return *this;
    }
}

const Locale &Locale::getDefault()
{
    return DEFAULT;
}

void Locale::setDefault(const Locale &locale)
{
    DEFAULT = locale;
}

ostream &operator<<(ostream &str, const Locale &loc)
{
    return str << loc.getDisplayName();
}

FENNEL_END_CPPFILE("$Id$");

// End Locale.cpp
