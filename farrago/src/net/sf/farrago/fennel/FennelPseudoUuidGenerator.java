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
package net.sf.farrago.fennel;

import org.eigenbase.util.Util;

/**
 * FennelPseudoUuidGenerator handles JNI calls to Fennel that generate
 * universal unique identifiers (UUIDs).  Fennel generates these in a
 * way that abstracts away OS and hardware dependencies.
 *
 * <p>Depends on Fennel's libfarrago.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class FennelPseudoUuidGenerator
{
    private static final int UUID_LENGTH = 16;

    static {
        Util.loadLibrary("farrago");
    }

    /** Inaccessible constrcutor. */
    private FennelPseudoUuidGenerator()
    {
    }

    public static byte[] validUuid()
    {
        return nativeGenerate();
    }

    public static byte[] invalidUuid()
    {
        return nativeGenerateInvalid();
    }

    private static native byte[] nativeGenerate();

    private static native byte[] nativeGenerateInvalid();
}
