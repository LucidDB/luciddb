/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
#include "fennel/common/ResourceBundle.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <map>
#include <set>
#include <string>
#include <sstream>

#include <boost/lexical_cast.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

using namespace std;
using namespace boost;

static string globalResourceLocation("");

RecursiveMutex ResourceBundle::mutex;

RecursiveMutex &ResourceBundle::getMutex()
{
    return mutex;
}

void ResourceBundle::setGlobalResourceFileLocation(const string &location)
{
    globalResourceLocation = location;
}

ResourceBundle::ResourceBundle(
    const string &baseName,
    const Locale &locale,
    const string &location)
    : _baseName(baseName),
      _locale(locale),
      _location(location),
      _parent(NULL)
{
    loadMessages();
}

ResourceBundle::~ResourceBundle()
{
}

void ResourceBundle::setParent(ResourceBundle *bundle)
{
    _parent = bundle;
}

const set<string> ResourceBundle::getKeys() const
{
    set<string> keys;

    map<string, string>::const_iterator iter = _messages.begin(),
        end = _messages.end();

    while (iter != end) {
        keys.insert((*iter).first);
        iter++;
    }

    if (_parent) {
        set<string> parentKeys = _parent->getKeys();

        keys.insert(parentKeys.begin(), parentKeys.end());
    }

    return keys;
}

static string MISSING_KEY("[[unknown key]]");

const string &ResourceBundle::getMessage(const string &key) const
{
    map<string, string>::const_iterator iter;
    iter = _messages.find(key);
    if (iter == _messages.end()) {
        if (_parent) {
            return _parent->getMessage(key);
        }

        return MISSING_KEY;
    }

    return (*iter).second;
}


bool ResourceBundle::hasMessage(const string &key) const
{
    return
        _messages.find(key) != _messages.end()
        || (_parent && _parent->hasMessage(key));
}

const Locale &ResourceBundle::getLocale() const
{
    return _locale;
}


const string &ResourceBundle::getBaseName() const
{
    return _baseName;
}


static const char APOS = '\'';
static const char LEFT_BRACE = '{';
static const char RIGHT_BRACE = '}';
static const char COMMA = ',';

static string convertPropertyToBoost(string &message)
{
    stringstream ss;

    bool quoted = false;
    char ch;
    char nextCh;

    for (int i = 0, n = message.length(); i < n; i++) {
        ch = message[i];
        nextCh = (i + 1 < n) ? message[i + 1] : 0;

        if (ch == APOS) {
            if (nextCh != APOS) {
                // Bare apostrophes signal start/end of QuotedString in the BNF
                quoted = !quoted;
                continue;
            }

            // Quoted or not, the next character is an apostrophe, so we
            // output an apostrophe.
            ss << APOS;
            i++; // skip nextCh
            continue;
        }

        if (quoted || ch != LEFT_BRACE) {
            ss << ch;
            continue;
        }

        // Handle an argument
        string::size_type commaPos = message.find(COMMA, i);
        string::size_type bracketPos = message.find(RIGHT_BRACE, i);
        if (bracketPos == string::npos) {
            // Bad format -- give up
            return message;
        }

        int argEndIndex = (commaPos == string::npos
                           ? bracketPos
                           : min(commaPos, bracketPos));

        int argIndex = boost::lexical_cast<int>(
            message.substr(i + 1, argEndIndex - (i + 1)));

        // Boost args are 1-based
        ss << '%' << (argIndex + 1) << '%';

        // Find the end of the argument tag
        bool quotedPattern = false;
        int bracketDepth = 0;
        i = argEndIndex;

        bool done = false;
        while (!done && i < n) {
            ch = message[i];

            switch (ch) {
            default:
                i++;
                break;

            case APOS:
                quotedPattern = !quotedPattern;
                i++;
                break;

            case LEFT_BRACE:
                if (!quotedPattern) {
                    bracketDepth++;
                }
                i++;
                break;

            case RIGHT_BRACE:
                if (!quotedPattern) {
                    if (bracketDepth > 0) {
                        bracketDepth--;
                    } else {
                        // end of pattern!
                        done = true;
                        break;
                    }
                }
                i++;
                break;
            }
        }

        if (i == n) {
            // couldn't find end of pattern -- give up
            return message;
        }
    }

    return ss.str();
}

// NOTE jvs 18-Feb-2004:  The conversion from Java resource format
// to Boost format could be done just once as part of the build, instead
// of each time on startup.  However, keeping everything in Java format
// simplifies the packaging/translation/distribution process.  And the
// performance hit should be minuscule unless the number of
// messages is huge.

void ResourceBundle::loadMessages()
{
    fstream in;

    // e.g. GeneratedResourceBundle_en_US.resources
    string fileName;

    if (_locale == Locale("")) {
        fileName.assign(
            _baseName + ".properties");
    } else {
        fileName.assign(
            _baseName + "_" + _locale.getDisplayName() + ".properties");
    }

    // look in _location first
    bool tryGlobalLocation = true;
    if (!_location.empty()) {
        string path = _location + "/" + fileName;
        in.open(path.c_str(), ios::in);
        if (in.good()) {
            tryGlobalLocation = false;
        }
    }

    // failing that, try the gobal location, if any
    if (tryGlobalLocation) {
        bool tryEnvVar = true;

        // TODO jvs 18-Feb-2004: once Fennel starts using Boost's
        // platform-independent filesystem library, use it here too.
        if (!globalResourceLocation.empty()) {
            string path = globalResourceLocation + "/" + fileName;
            in.open(path.c_str(), ios::in);
            if (in.good()) {
                tryEnvVar = false;
            }
        }

        if (tryEnvVar) {
            const char *fennelHome = getenv("FENNEL_HOME");
            if (fennelHome == NULL) {
                return; // give up
            }

            string path = string(fennelHome) + "/common/" + fileName;
            in.open(path.c_str(), ios::in);
            if (!in.good()) {
                return; // give up
            }
        }
    }

    string line, key, message;
    while (in.good()) {
        getline(in, line);

        if (line.length() == 0 || line[0] == '#') {
            // ignore blank lines and comments
            continue;
        }

        string::size_type pos = line.find('=');
        if (pos == string::npos) {
            // bad message format?
            continue;
        }

        key = line.substr(0, pos);
        message = line.substr(pos + 1);

        _messages[key] = convertPropertyToBoost(message);
    }

    in.close();
}

FENNEL_END_CPPFILE("$Id$");

// End ResourceBundle.cpp
