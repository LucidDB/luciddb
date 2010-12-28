/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

#ifndef Fennel_Locale_Included
#define Fennel_Locale_Included

#include <string>
#include <iostream>

FENNEL_BEGIN_NAMESPACE

// Locale represents a user's locale, providing a simplistic mechanism
// for representing language, country and variant.  Locale can compute
// its parent locale.  The parent of a variant locale
// (e.g. "en_US_Southern" or "en_DoubleSpeak") is the same locale sans
// variant ("en_US" and "en" in our example).  The parent of a
// language/country locle (e.g. "en_US" or "fr_CA") is simply the
// language ("en" or "fr" in our example.  Language-only locales have
// no parent.  Language and country codes are always two letters.
// Also, the language should be lowercase and the country uppercase.
class FENNEL_COMMON_EXPORT Locale
{
public:
    explicit Locale(const std::string &language);
    explicit Locale(const std::string &language, const std::string &country);
    explicit Locale(
        const std::string &language,const std::string &country,
        const std::string &variant);
    virtual ~Locale();

    bool operator==(const Locale &rhs) const;
    bool operator!=(const Locale &rhs) const;
    bool operator<(const Locale &rhs) const;

    const std::string &getLanguage() const;
    const std::string &getCountry() const;
    const std::string &getVariant() const;

    // Returns lang, lang_country, lang_variant or lang_country_variant,
    // depending on which fields were specified.
    std::string getDisplayName() const;

    bool hasParentLocale() const;
    Locale getParentLocale() const;

    static const Locale &getDefault();
    static void setDefault(const Locale &locale);

//  static const vector<const Locale> getAvailableLocales();

private:
    std::string _lang;
    std::string _country;
    std::string _variant;

    static Locale DEFAULT;
};

// make it easy to print Locales
std::ostream &operator<<(std::ostream &str, const Locale &loc);

FENNEL_END_NAMESPACE

#endif // not Fennel_Locale_Included

// End Locale.h
