/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.catalog.codegen;

import net.sf.farrago.util.*;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

// TODO jvs 28-April-2004: move this to a repos-independent codegen utility
// package and add a main method so it can be used from ant; this is just a
// temporary parking space

/**
 * CppEnumGen is a tool for generating a C++ enumeration based on
 * the public static final data members of a Java class.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CppEnumGen
{
    private PrintWriter pw;

    /**
     * Creates a new CppEnumGen.
     *
     * @param pw PrintWriter to which enumeration definitions should be written
     */
    public CppEnumGen(PrintWriter pw)
    {
        this.pw = pw;
    }

    /**
     * Generates a single enumeration.  Enumeration values (and their names) is
     * based on the subset of non-inherited public static final data members
     * contained by enumClass and having exact type enumSymbolType.
     * Enumeration order (and hence implied ordinals) is based on the result of
     * {@link Class#getDeclaredFields}.
     *
     *<p>
     *
     * TODO:  support integer ordinals
     *
     * @param enumName name to give C++ enum
     *
     * @param enumClass Java class to be interpreted as an enumeration;
     * this class's name is used as the enumeration name
     *
     * @param enumSymbolType Java class used to determine enumeration
     * membership
     */
    public void generateEnumForClass(
        String enumName,
        Class enumClass,
        Class enumSymbolType)
        throws Exception
    {
        List symbols = new ArrayList();

        Field [] fields = enumClass.getDeclaredFields();
        for (int i = 0; i < fields.length; ++i) {
            Field field = fields[i];
            Class fieldType = field.getType();
            if (!(fieldType.equals(enumSymbolType))) {
                continue;
            }
            symbols.add(field.getName());
        }

        pw.print("enum ");
        pw.print(enumName);
        pw.println(" {");

        Iterator iter = symbols.iterator();
        while (iter.hasNext()) {
            String symbol = (String) iter.next();
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
            String symbol = (String) iter.next();
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
