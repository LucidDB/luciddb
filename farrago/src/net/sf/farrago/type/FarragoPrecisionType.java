/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

import java.nio.charset.Charset;
import java.sql.Types;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.RelDataTypeFactoryImpl;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FarragoPrecisionType instantiates a CwmSqlsimpleType that allows for extra
 * attributes such as precision and scale.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoPrecisionType extends FarragoAtomicType
{
    //~ Instance fields -------------------------------------------------------

    private String charsetName;
    private SqlCollation collation;
    private final int precision;
    private final int scale;
    protected OJClass ojClass;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoPrecisionType object.
     *
     * @param simpleType .
     * @param isNullable .
     * @param precision .
     * @param scale .
     * @param charsetName . Must be null if not of char type
     * @param collation . Must be null if not of char type
     */
    FarragoPrecisionType(
        CwmSqlsimpleType simpleType,
        boolean isNullable,
        int precision,
        int scale,
        String charsetName,
        SqlCollation collation)
    {
        super(simpleType, isNullable);
        this.precision = precision;
        this.scale = scale;
        if (!SqlTypeUtil.inCharFamily(this)) {
            assert (null == charsetName);
            assert (null == collation);
        }
        this.charsetName = charsetName;
        this.collation = collation;
        computeDigest();
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelDataType
    public Charset getCharset()
        throws RuntimeException
    {
        if (!SqlTypeUtil.inCharFamily(this)) {
            throw Util.newInternal(digest
                + " is not defined to carry a charset");
        }
        if (null == this.charsetName) {
            return null;
        }
        return Charset.forName(this.charsetName);
    }

    // implement RelDataType
    public SqlCollation getCollation()
        throws RuntimeException
    {
        if (!SqlTypeUtil.inCharFamily(this)) {
            throw Util.newInternal(digest
                + " is not defined to carry a collation");
        }
        return this.collation;
    }

    // implement FarragoAtomicType
    protected boolean requiresValueAccess()
    {
        return false;
    }

    // override FarragoAtomicType
    public int getPrecision()
    {
        return precision;
    }

    // override FarragoAtomicType
    public int getScale()
    {
        return scale;
    }

    // implement FarragoType
    protected OJClass getOjClass(OJClass declarer)
    {
        if (ojClass != null) {
            return ojClass;
        }

        Class superclass;
        MemberDeclarationList memberDecls = new MemberDeclarationList();
        if (charsetName == null) {
            superclass = BytePointer.class;
        } else {
            superclass = EncodedCharPointer.class;
            memberDecls.add(
                new MethodDeclaration(
                    new ModifierList(ModifierList.PROTECTED),
                    OJUtil.typeNameForClass(String.class),
                    "getCharsetName",
                    new ParameterList(),
                    new TypeName[0],
                    new StatementList(
                        new ReturnStatement(
                            Literal.makeLiteral(charsetName)))));
        }

        TypeName [] superDecl =
            new TypeName [] { OJUtil.typeNameForClass(superclass) };

        TypeName [] interfaceDecls = null;
        if (isNullable()) {
            interfaceDecls =
                new TypeName [] {
                    OJUtil.typeNameForClass(NullableValue.class)
                };
        }
        ClassDeclaration decl =
            new ClassDeclaration(new ModifierList(ModifierList.PUBLIC
                        | ModifierList.STATIC),
                "Oj_inner_" + getFactoryImpl().generateClassId(),
                superDecl,
                interfaceDecls,
                memberDecls);
        ojClass = new OJTypedClass(declarer, decl, this);
        try {
            declarer.addClass(ojClass);
        } catch (CannotAlterException e) {
            throw Util.newInternal(e, "holder class must be OJClassSourceCode");
        }
        Environment env = declarer.getEnvironment();
        OJUtil.recordMemberClass(
            env,
            declarer.getName(),
            decl.getName());
        OJUtil.getGlobalEnvironment(env).record(
            ojClass.getName(),
            ojClass);
        return ojClass;
    }

    // override FarragoAtomicType
    protected void generateTypeString(
        StringBuffer sb,
        boolean withDetail)
    {
        super.generateTypeString(sb, withDetail);
        if (!withDetail) {
            return;
        }
        if (charsetName != null) {
            sb.append(" CHARACTER SET \"");
            sb.append(charsetName);
            sb.append("\"");
        }
        if (collation != null) {
            sb.append(" COLLATE \"");
            sb.append(collation.getCollationName());
            sb.append("\"");
        }
    }
}


// End FarragoPrecisionType.java
