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
