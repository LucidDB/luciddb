/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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

ResourceDefinition::ResourceDefinition(ResourceBundle *bundle,
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
        for(int i = 0; i < numArgs; i++) {
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
