/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

Locale::Locale(const string &language,
               const string &country,
               const string &variant)
    : _lang(language), _country(country), _variant(variant)
{
    // either language, country and variant are empty or language must
    // be non-empty
    assert((language.empty() && country.empty() && variant.empty()) ||
           !language.empty());

    assert(language.empty() || language.length() == 2);
    assert(country.empty() || country.length() == 2);
}

Locale::~Locale()
{
}

bool Locale::operator==(const Locale &rhs) const
{
    return (_lang == rhs._lang &&
            _country == rhs._country &&
            _variant == rhs._variant);
}

bool Locale::operator!=(const Locale &rhs) const {
    return (_lang != rhs._lang ||
            _country != rhs._country ||
            _variant != rhs._variant);
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
