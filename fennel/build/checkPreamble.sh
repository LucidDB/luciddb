#!/bin/sh
# $Id $
# Checks the header for all files.

usage() {
  echo "checkPreamble.sh [options] file[...]"
  echo "Options:"
  echo " -eigenbase       Require Eigenbase copyright"
  echo " -disruptivetech  Require Disruptive Tech copyright"
  echo " -redsquare       Require Red Square copyright"
  echo " -fennel          Require Fennel description, 'Fennel is a library of"
  echo "                  data storage and processing components.'"
  echo " -farrago         Require Farrago description, 'Farrago is an"
  echo "                  extensible data management system."
  echo " -farrago-eigenbase Require Eigenbase description, 'Package"
  echo "                  org.eigenbase is a class library of data management"
  echo "                  components."
}

check() {
    file="$1"
    gawk \
        -v zone=${zone} \
        -v component=${component} \
        '
BEGIN {
    expects[++n] = "/\\*";
    actuals[n]   = "/*";
    expects[++n] = "// \\$I" "d:.*\\$";
    actuals[n]   = "// $I" "d:$";
    if (component == "fennel") {
        expects[++n] = "// Fennel is a library of data storage and processing components\\.";
        actuals[n]   = "// Fennel is a library of data storage and processing components.";
    } else if (component == "farrago") {
        expects[++n] = "// Farrago is an extensible data management system\\.";
        actuals[n]   = "// Farrago is an extensible data management system.";
    } else if (component == "farrago-eigenbase") {
        expects[++n] = "// Package org.eigenbase is a class library of data management components\\.";
        actuals[n]   = "// Package org.eigenbase is a class library of data management components.";
    }

    if (zone == "eigenbase") {
        expects[++n] = "// Copyright \\(C\\) [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9] The Eigenbase Project";
        actuals[n]   = "// Copyright (C) year-year The Eigenbase Project";
        expects[++n] = "// Copyright \\(C\\) [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9] Disruptive Tech";
        actuals[n]   = "// Copyright (C) year-year Disruptive Tech";
        expects[++n] = "// Copyright \\(C\\) [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9] Red Square, Inc\\.";
        actuals[n]   = "// Copyright (C) year-year Red Square, Inc.";
    }
    if (zone == "disruptivetech") {
        expects[++n] = "// Copyright \\(C\\) [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9] Disruptive Tech";
        actuals[n]   = "// Copyright (C) year-year Disruptive Tech";
        expects[++n] = "// Copyright \\(C\\) [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9] The Eigenbase Project";
        actuals[n]   = "// Copyright (C) year-year The Eigenbase Project";
    }
    if (zone == "redsquare") {
        expects[++n] = "// Copyright \\(C\\) [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9] Red Square, Inc\\.";
        actuals[n]   = "// Copyright (C) year-year Red Square, Inc.";
        expects[++n] = "// Copyright \\(C\\) [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9] The Eigenbase Project";
        actuals[n]   = "// Copyright (C) year-year The Eigenbase Project";
    }

    expects[++n] = "//";
    actuals[n]   = "//";
    expects[++n] = "// This program is free software; you can redistribute it and/or modify it";
    actuals[n]   = "// This program is free software; you can redistribute it and/or modify it";
    expects[++n] = "// under the terms of the GNU General Public License as published by the Free";
    actuals[n]   = "// under the terms of the GNU General Public License as published by the Free";
    expects[++n] = "// Software Foundation; either version 2 of the License, or \\(at your option\\)";
    actuals[n]   = "// Software Foundation; either version 2 of the License, or (at your option)";
    expects[++n] = "// any later version approved by The Eigenbase Project\\.";
    actuals[n]   = "// any later version approved by The Eigenbase Project.";
    expects[++n] = "//";
    actuals[n]   = "//";
    expects[++n] = "// This program is distributed in the hope that it will be useful,";
    actuals[n]   = "// This program is distributed in the hope that it will be useful,";
    expects[++n] = "// but WITHOUT ANY WARRANTY; without even the implied warranty of";
    actuals[n]   = "// but WITHOUT ANY WARRANTY; without even the implied warranty of";
    expects[++n] = "// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE\\.  See the";
    actuals[n]   = "// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the";
    expects[++n] = "// GNU General Public License for more details\\.";
    actuals[n]   = "// GNU General Public License for more details.";
    expects[++n] = "//";
    actuals[n]   = "//";
    expects[++n] = "// You should have received a copy of the GNU General Public License";
    actuals[n]   = "// You should have received a copy of the GNU General Public License";
    expects[++n] = "// along with this program; if not, write to the Free Software";
    actuals[n]   = "// along with this program; if not, write to the Free Software";
    expects[++n] = "// Foundation, Inc\\., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA";
    actuals[n]   = "// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA";

    # Correction factor to allow extra lines to be added to the header.
    offset = 0;
}
{
    expect = expects[FNR - offset];
    if (expect) {
        if ($0 !~ expect) {
            actual = actuals[FNR - offset];
            if (!actual) {
                actual = expect;
            }
            printf "%s:%d: Expected %c%s%c\n", FILENAME, FNR, 34, actual, 34;
            exit;
        }
    }
    if ($0 ~ "// Portions Copyright \\(C\\) [0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9] .*") {
        ++offset;
    }
    next;
}
        ' \
        $file
}

zone=eigenbase
component=farrago

while [ $# -gt 0 ]
do
   case "$1" in
   -eigenbase) zone=eigenbase ; shift ;;
   -disruptivetech) zone=disruptivetech ; shift ;;
   -redsquare) zone=redsquare ; shift ;;
   -fennel) component=fennel ; shift ;;
   -farrago) component=farrago ; shift ;;
   -farrago-eigenbase) component=farrago-eigenbase ; shift ;;
   *) break ;;
   esac
done

for i in "$@"
do
    check "$i"
done


# End checkHeader
