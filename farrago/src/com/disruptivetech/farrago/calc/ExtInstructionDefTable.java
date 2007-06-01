/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.disruptivetech.farrago.calc;

/**
 * A class that holds {@link CalcProgramBuilder.ExtInstrDef} and its
 * sub-classes.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Jun 18, 2004
 */
public class ExtInstructionDefTable
{
    //~ Static fields/initializers ---------------------------------------------

    public static final CalcProgramBuilder.ExtInstrDef abs =
        new CalcProgramBuilder.ExtInstrDef("ABS", 2);
    public static final CalcProgramBuilder.ExtInstrDef castA =
        new CalcProgramBuilder.ExtInstrDef("castA", 2);
    public static final CalcProgramBuilder.ExtInstrDef castADecimal =
        new CalcProgramBuilder.ExtInstrDef("castA", 4);
    public static final CalcProgramBuilder.ExtInstrDef castDateToMillis =
        new CalcProgramBuilder.ExtInstrDef("CastDateTimeToInt64", 2);
    public static final CalcProgramBuilder.ExtInstrDef castDateToStr =
        new CalcProgramBuilder.ExtInstrDef("CastDateToStrA", 2);
    public static final CalcProgramBuilder.InstructionDef castStrAToDate =
        new CalcProgramBuilder.ExtInstrDef("CastStrAToDate", 2);
    public static final CalcProgramBuilder.InstructionDef castStrAToTime =
        new CalcProgramBuilder.ExtInstrDef("CastStrAToTime", 2);
    public static final CalcProgramBuilder.InstructionDef castStrAToTimestamp =
        new CalcProgramBuilder.ExtInstrDef("CastStrAToTimestamp", 2);
    public static final CalcProgramBuilder.ExtInstrDef castTimeToStr =
        new CalcProgramBuilder.ExtInstrDef("CastTimeToStrA", 2);
    public static final CalcProgramBuilder.ExtInstrDef castTimestampToStr =
        new CalcProgramBuilder.ExtInstrDef("CastTimestampToStrA", 2);
    public static final CalcProgramBuilder.ExtInstrDef charLength =
        new CalcProgramBuilder.ExtInstrDef("strLenCharA", 2);
    public static final CalcProgramBuilder.ExtInstrDef concat =
        new CalcProgramBuilder.ExtInstrSizeDef("strCatA");
    public static final CalcProgramBuilder.ExtInstrDef dynamicVariable =
        new CalcProgramBuilder.ExtInstrDef("dynamicVariable", 2);
    public static final CalcProgramBuilder.ExtInstrDef like =
        new CalcProgramBuilder.ExtInstrSizeDef("strLikeA");
    public static final CalcProgramBuilder.ExtInstrDef localTime =
        new CalcProgramBuilder.ExtInstrSizeDef("LocalTime");
    public static final CalcProgramBuilder.ExtInstrDef localTimestamp =
        new CalcProgramBuilder.ExtInstrSizeDef("LocalTimestamp");
    public static final CalcProgramBuilder.ExtInstrDef currentTime =
        new CalcProgramBuilder.ExtInstrSizeDef("CurrentTime");
    public static final CalcProgramBuilder.ExtInstrDef currentTimestamp =
        new CalcProgramBuilder.ExtInstrSizeDef("CurrentTimestamp");
    public static final CalcProgramBuilder.ExtInstrDef log =
        new CalcProgramBuilder.ExtInstrDef("LN", 2);
    public static final CalcProgramBuilder.ExtInstrDef log10 =
        new CalcProgramBuilder.ExtInstrDef("LOG10", 2);
    public static final CalcProgramBuilder.ExtInstrDef lower =
        new CalcProgramBuilder.ExtInstrDef("strToLowerA", 2);
    public static final CalcProgramBuilder.ExtInstrDef overlay =
        new CalcProgramBuilder.ExtInstrSizeDef("strOverlayA");
    public static final CalcProgramBuilder.ExtInstrDef position =
        new CalcProgramBuilder.ExtInstrDef("strPosA", 3);
    public static final CalcProgramBuilder.ExtInstrDef similar =
        new CalcProgramBuilder.ExtInstrSizeDef("strSimilarA");
    public static final CalcProgramBuilder.ExtInstrDef strCmpA =
        new CalcProgramBuilder.ExtInstrDef("strCmpA", 3);
    public static final CalcProgramBuilder.ExtInstrDef strCmpOct =
        new CalcProgramBuilder.ExtInstrDef("strCmpOct", 3);
    public static final CalcProgramBuilder.ExtInstrDef substring =
        new CalcProgramBuilder.ExtInstrSizeDef("strSubStringA");
    public static final CalcProgramBuilder.ExtInstrDef trim =
        new CalcProgramBuilder.ExtInstrDef("strTrimA", 5);
    public static final CalcProgramBuilder.ExtInstrDef upper =
        new CalcProgramBuilder.ExtInstrDef("strToUpperA", 2);
    public static final CalcProgramBuilder.ExtInstrDef pow =
        new CalcProgramBuilder.ExtInstrDef("POW", 3);
    public static final CalcProgramBuilder.ExtInstrDef histogramInit =
        new CalcProgramBuilder.ExtInstrDef("WinAggInit", 2);
    public static final CalcProgramBuilder.ExtInstrDef histogramAdd =
        new CalcProgramBuilder.ExtInstrDef("WinAggAdd", 2);
    public static final CalcProgramBuilder.ExtInstrDef histogramDrop =
        new CalcProgramBuilder.ExtInstrDef("WinAggDrop", 2);
    public static final CalcProgramBuilder.ExtInstrDef histogramGetMax =
        new CalcProgramBuilder.ExtInstrDef("WinAggMax", 2);
    public static final CalcProgramBuilder.ExtInstrDef histogramGetMin =
        new CalcProgramBuilder.ExtInstrDef("WinAggMin", 2);
    public static final CalcProgramBuilder.ExtInstrDef histogramGetFirstValue =
        new CalcProgramBuilder.ExtInstrDef("WinAggFirstValue", 2);
    public static final CalcProgramBuilder.ExtInstrDef histogramGetLastValue =
        new CalcProgramBuilder.ExtInstrDef("WinAggLastValue", 2);
}

// End ExtInstructionDefTable.java
