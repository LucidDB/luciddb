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


package net.sf.farrago.type;

import net.sf.farrago.cwm.relational.CwmSqlsimpleType;
import net.sf.farrago.type.runtime.SqlDateTimeWithoutTZ;
import net.sf.farrago.type.runtime.NullableValue;
import net.sf.saffron.util.Util;

import java.sql.Types;

import openjava.mop.OJClass;
import openjava.mop.CannotAlterException;
import openjava.mop.Environment;
import openjava.ptree.*;


/**
 * Extension of precision type for date/time objects.
 *
 * @author lee
 * @since May 1, 2004
 * @version $Id$
 **/
public class FarragoDateTimeType extends FarragoPrecisionType {
    private boolean hasTimeZone;

    public FarragoDateTimeType(
            CwmSqlsimpleType simpleType,
            boolean isNullable,
            boolean hasTZ,
            int precision,
            FarragoTypeFactory typeFactory) {
        super(simpleType,isNullable,precision,0,null,null);
        assertSupportedType(simpleType, precision);
        hasTimeZone = hasTZ;
        computeDateTimeDigest();
        this.factory = typeFactory;
    }

    private void assertSupportedType(CwmSqlsimpleType simpleType, int precision) {
        switch (simpleType.getTypeNumber().intValue()) {
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
            return;
        }
        assert (0 <= precision && precision <= 3) : "Unsupported precision " + precision + " in type " + simpleType.getName();
        throw new AssertionError("Unsupported type:" + simpleType.getName());
    }


    public boolean hasTimeZone() {
        return hasTimeZone;
    }

    protected void computeDateTimeDigest() {

        if (hasTimeZone) {
            digest = digest + "_WITH_TZ";
        }
    }

    public int getOctetLength() {
        return 8; // sizeof long.
    }

    protected OJClass getOjClass(OJClass declarer) {
        if (ojClass != null) {
            return ojClass;
        }

        Class superclass;
        MemberDeclarationList memberDecls = new MemberDeclarationList();

        assert (!hasTimeZone()) : "Time with TimeZone not supported yet";

        switch (this.getSimpleType().getTypeNumber().intValue()) {
        case Types.DATE:
            superclass = SqlDateTimeWithoutTZ.SqlDate.class;
            break;
        case Types.TIME:
            superclass = SqlDateTimeWithoutTZ.SqlTime.class;
            break;
        case Types.TIMESTAMP:
            superclass = SqlDateTimeWithoutTZ.SqlTimestamp.class;
            break;
        default:
           throw new AssertionError("Unsupported type:" + this.getSimpleType().getName());
        }

        TypeName [] superDecl =
            new TypeName [] { TypeName.forClass(superclass) };

        TypeName [] interfaceDecls = null;
        if (isNullable()) {
            interfaceDecls =
                new TypeName [] { TypeName.forClass(NullableValue.class) };
        }
        ClassDeclaration decl =
            new ClassDeclaration(
                new ModifierList(ModifierList.PUBLIC | ModifierList.STATIC),
                "Oj_" + digest,
                superDecl,
                interfaceDecls,
                memberDecls);
        ojClass = new OJTypedClass(declarer,decl,this);
        try {
            declarer.addClass(ojClass);
        } catch (CannotAlterException e) {
            throw Util.newInternal(e,"holder class must be OJClassSourceCode");
        }
        Environment env = declarer.getEnvironment();
        env.recordMemberClass(declarer.getName(),decl.getName());
        env.getGlobalEnvironment().record(ojClass.getName(),ojClass);
        return ojClass;
    }

    public boolean hasClassForPrimitive() {
        return true;
    }

    public Class getClassForPrimitive() {
        return SqlDateTimeWithoutTZ.getPrimitiveClass();
    }
    
    // implement FarragoAtomicType
    public boolean requiresValueAccess()
    {
        return true;
    }
}

// End FarragoDateTimeType.java
