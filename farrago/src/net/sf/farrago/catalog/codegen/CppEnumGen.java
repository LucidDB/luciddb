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
package net.sf.farrago.catalog.codegen;

import java.io.*;

import java.lang.reflect.*;

import java.util.*;


// TODO jvs 28-April-2004: move this to a repos-independent codegen utility
// package and add a main method so it can be used from ant; this is just a
// temporary parking space

/**
 * CppEnumGen is a tool for generating a C++ enumeration based on the public
 * static final data members of a Java class.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CppEnumGen
{
    //~ Instance fields --------------------------------------------------------

    private PrintWriter pw;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CppEnumGen.
     *
     * @param pw PrintWriter to which enumeration definitions should be written
     */
    public CppEnumGen(PrintWriter pw)
    {
        this.pw = pw;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Generates a single enumeration. Enumeration values (and their names) is
     * based on the subset of non-inherited public static final data members
     * contained by enumClass and having exact type enumSymbolType. Enumeration
     * order (and hence implied ordinals) is on the current locale's collation
     * order for the enum field names. This ordering may not hold in the future,
     * so no C++ code should be written which depends on the current
     * deterministic ordering.
     *
     * <p>TODO: Support integer ordinals. Also, we'd prefer to preserve the
     * original metamodel ordering in order to relax the ordering condition
     * above.
     *
     * @param enumName name to give C++ enum
     * @param enumClass Java class to be interpreted as an enumeration; this
     * class's name is used as the enumeration name
     * @param enumSymbolType Java class used to determine enumeration membership
     */
    public void generateEnumForClass(
        String enumName,
        Class enumClass,
        Class enumSymbolType)
        throws Exception
    {
        List<String> symbols = new ArrayList<String>();

        Field [] fields = enumClass.getDeclaredFields();
        for (int i = 0; i < fields.length; ++i) {
            Field field = fields[i];
            Class fieldType = field.getType();
            if (!(fieldType.equals(enumSymbolType))) {
                continue;
            }
            symbols.add(field.getName());
        }

        // Force deterministic ordering
        Collections.sort(symbols);

        pw.print("enum ");
        pw.print(enumName);
        pw.println(" {");

        Iterator<String> iter = symbols.iterator();
        while (iter.hasNext()) {
            String symbol = iter.next();
            pw.print("    ");
            pw.print(symbol);
            if (iter.hasNext()) {
                pw.print(",");
            }
            pw.println();
        }

        pw.println("};");
        pw.println();

        // TODO jvs 28-April-2004:  declare as extern rather than static
        pw.print("static std::string ");
        pw.print(enumName);
        pw.print("_names[] = {");

        iter = symbols.iterator();
        while (iter.hasNext()) {
            String symbol = iter.next();
            pw.print('"');
            pw.print(symbol);
            pw.print('"');
            pw.print(",");
        }

        pw.println("\"\"};");
        pw.println();
    }
}

// End CppEnumGen.java
