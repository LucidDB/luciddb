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

import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.util.*;

import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;
import net.sf.saffron.core.SaffronType;

import openjava.mop.*;

import openjava.ptree.*;


/**
 * FarragoPrecisionType instantiates a CwmSqlsimpleType that allows for extra
 * attributes such as precision and scale.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public final class FarragoPrecisionType extends FarragoAtomicType
{
    //~ Instance fields -------------------------------------------------------

    private final String charsetName;

    private final int precision;

    private final int scale;
    
    private OJClass ojClass;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoPrecisionType object.
     *
     * @param simpleType .
     * @param isNullable .
     * @param precision .
     * @param scale .
     * @param charsetName .
     */
    FarragoPrecisionType(
        CwmSqlsimpleType simpleType,
        boolean isNullable,
        int precision,
        int scale,
        String charsetName)
    {
        super(simpleType,isNullable);
        this.precision = precision;
        this.scale = scale;
        this.charsetName = charsetName;
        computeDigest();
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return name of character set used for stored encoding,
     * or null for binary data
     */
    public String getCharsetName()
    {
        return charsetName;
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
                    TypeName.forClass(String.class),
                    "getCharsetName",
                    new ParameterList(),
                    new TypeName[0],
                    new StatementList(
                        new ReturnStatement(Literal.makeLiteral(charsetName)))));
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

    // implement FarragoType
    protected void computeDigest()
    {
        // NOTE:  this has to come out to a legal Java identifier
        StringBuffer sb = new StringBuffer();
        sb.append(getSimpleType().getName());
        sb.append('_');
        sb.append(precision);
        if (scale != 0) {
            sb.append('_');
            sb.append(scale);
        }
        if (charsetName != null) {
            sb.append('_');
            sb.append(charsetName.replace('-','_'));
        }
        if (isNullable()) {
            sb.append("_NULLABLE");
        }
        digest = sb.toString();
    }

    // override FarragoType
    public void forgetFactory()
    {
        super.forgetFactory();
        ojClass = null;
    }
}


// End FarragoPrecisionType.java
