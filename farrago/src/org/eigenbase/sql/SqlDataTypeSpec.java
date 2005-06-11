/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.sql;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.util.Util;

import java.nio.charset.Charset;
import java.util.TimeZone;


/**
 * Represents a SQL data type specification in a parse tree.
 *
 * <p>todo: This should really be a subtype of {@link SqlCall}.
 *
 * <p>In its full glory, we will have to support complex type expressions like
 *
 * <blockquote><code>
 * ROW(
 *     NUMBER(5,2) NOT NULL AS foo,
 *     ROW(
 *         BOOLEAN AS b,
 *         MyUDT NOT NULL AS i
 *     ) AS rec
 * )</code></blockquote>
 *
 * <p>Currently it only supports simple datatypes, like char, varchar, double,
 * with optional precision and scale.
 *
 * @author Lee Schumacher
 * @since Jun 4, 2004
 * @version $Id$
 **/
public class SqlDataTypeSpec extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    private final SqlIdentifier collectionsTypeName;
    private final SqlIdentifier typeName;
    private final int scale;
    private final int precision;
    private RelDataType type;
    private final String charSetName;
    private final String format;
    private final TimeZone timezone;

    //~ Constructors ----------------------------------------------------------

    public SqlDataTypeSpec(
        final SqlIdentifier typeName,
        int precision,
        int scale,
        String charSetName,
        SqlParserPos pos)
    {
        super(pos);
        this.collectionsTypeName = null;
        this.typeName = typeName;
        this.scale = scale;
        this.precision = precision;
        this.charSetName = charSetName;
        this.format = null;
        this.timezone = null;
    }

    public SqlDataTypeSpec(
        final SqlIdentifier typeName,
        int precision,
        int scale,
        String charSetName,
        String format,
        TimeZone timezone,
        SqlParserPos pos)
    {
        super(pos);
        this.collectionsTypeName = null;
        this.typeName = typeName;
        this.scale = scale;
        this.precision = precision;
        this.charSetName = charSetName;
        this.format = format;
        this.timezone = timezone;
    }

    public SqlDataTypeSpec(
        final SqlIdentifier collectionsTypeName,
        final SqlIdentifier typeName,
        int precision,
        int scale,
        String charSetName,
        SqlParserPos pos)
    {
        super(pos);
        this.collectionsTypeName = collectionsTypeName;
        this.typeName = typeName;
        this.scale = scale;
        this.precision = precision;
        this.charSetName = charSetName;
        this.format = null;
        this.timezone = null;
    }

    //~ Methods ---------------------------------------------------------------

    public RelDataType getType()
    {
        return type;
    }

    public SqlIdentifier getCollectionsTypeName()
    {
        return collectionsTypeName;
    }

    public SqlIdentifier getTypeName()
    {
        return typeName;
    }

    public int getScale()
    {
        return scale;
    }

    public int getPrecision()
    {
        return precision;
    }

    public String getCharSetName()
    {
        return charSetName;
    }

    public String getFormat()
    {
        return format;
    }

    public TimeZone getTimezone()
    {
        return timezone;
    }

    /**
     * Returns a new SqlDataTypeSpec corresponding to the component type if
     * the type spec is a collections type spec.<br>
     * Collection types are <code>ARRAY</code> and <code>MULTISET</code>.
     * @pre null != getCollectionsTypeName();
     */
    public SqlDataTypeSpec getComponentTypeSpec() {
        Util.pre(
            null != getCollectionsTypeName(), "null != getCollectionsTypeName()");
        return new SqlDataTypeSpec(
            typeName, precision, scale, charSetName, getParserPosition());
    }

    /**
     * Writes a SQL representation of this node to a writer.
     *
     * <p>The <code>leftPrec</code> and <code>rightPrec</code> parameters
     * give us enough context to decide whether we need to enclose the
     * expression in parentheses. For example, we need parentheses around
     * "2 + 3" if preceded by "5 *". This is because the precedence of the "*"
     * operator is greater than the precedence of the "+" operator.
     *
     * <p>The algorithm handles left- and right-associative operators by giving
     * them slightly different left- and right-precedence.
     *
     * <p>If {@link SqlWriter#alwaysUseParentheses} is true, we use parentheses
     * even when they are not required by the precedence rules.
     *
     * <p>For the details of this algorithm, see {@link SqlCall#unparse}.
     *
     * @param writer Target writer
     * @param leftPrec The precedence of the {@link SqlNode} immediately
     *   preceding this node in a depth-first scan of the parse tree
     * @param rightPrec The precedence of the {@link SqlNode} immediately
     *   following this node in a depth-first scan of the parse tree
     */
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        String name = typeName.getSimple();
        if (SqlTypeName.containsName(name)) {
            SqlTypeName sqlTypeName = SqlTypeName.get(name);

            //we have a built in data type
            writer.print(name);

            if (sqlTypeName.allowsPrec()) {
                writer.print("(" + precision);
                if (sqlTypeName.allowsScale()) {
                    writer.print(", " + scale);
                }
                writer.print(")");
            }

            if (charSetName != null) {
                writer.print(" CHARACTER SET ");
                writer.printIdentifier(charSetName);
            }

            if (collectionsTypeName != null) {
                writer.print(" ");
                writer.print(collectionsTypeName.getSimple());
            }
        } else {
            // else we have a user defined type
            typeName.unparse(writer, leftPrec, rightPrec);
        }
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateDataType(this);
    }

    public void accept(SqlVisitor visitor)
    {
        visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node)
    {
        if (node instanceof SqlDataTypeSpec) {
            SqlDataTypeSpec that = (SqlDataTypeSpec) node;
            final boolean collectionsTest;
            if (null != this.collectionsTypeName) {
                collectionsTest =
                    this.collectionsTypeName.equalsDeep(
                        that.collectionsTypeName);
            } else {
                collectionsTest =
                    this.collectionsTypeName == that.collectionsTypeName;
            }
            return collectionsTest &&
                this.typeName.equalsDeep(that.typeName) &&
                this.precision == that.precision &&
                this.scale == that.scale &&
                this.format == that.format &&
                this.timezone == that.timezone &&
                Util.equal(this.charSetName, that.charSetName);
        }
        return false;
    }

    /**
     * Throws an error if the type is not built-in.
     */
    public RelDataType deriveType(SqlValidator validator)
    {
        String name = typeName.getSimple();

        //for now we only support builtin datatypes
        if (!SqlTypeName.containsName(name)) {
            throw validator.newValidationError(this,
                EigenbaseResource.instance().newUnknownDatatypeName(name));
        }

        if (null != collectionsTypeName) {
            final String collectionName = collectionsTypeName.getSimple();
            if (!SqlTypeName.containsName(collectionName)) {
                throw validator.newValidationError(this,
                    EigenbaseResource.instance().newUnknownDatatypeName(
                        collectionName));
            }
        }

        RelDataTypeFactory typeFactory = validator.getTypeFactory();
        return deriveType(typeFactory);
    }

    /**
     * Does not throw an error if the type is not built-in.
     */
    public RelDataType deriveType(RelDataTypeFactory typeFactory)
    {
        String name = typeName.getSimple();

        SqlTypeName sqlTypeName = SqlTypeName.get(name);

        // TODO jvs 13-Dec-2004:  these assertions should be real
        // validation errors instead; need to share code with DDL
        if ((precision > 0) && (scale > 0)) {
            assert(sqlTypeName.allowsPrecScale(true, true));
            type = typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision > 0) {
            assert(sqlTypeName.allowsPrecNoScale());
            type = typeFactory.createSqlType(sqlTypeName, precision);
        } else {
            assert(sqlTypeName.allowsNoPrecNoScale());
            type = typeFactory.createSqlType(sqlTypeName);
        }

        if (SqlTypeUtil.inCharFamily(type)) {
            // Applying Syntax rule 10 from SQL:99 spec section 6.22 "If TD is a
            // fixed-length, variable-length or large object character string,
            // then the collating sequence of the result of the <cast
            // specification> is the default collating sequence for the
            // character repertoire of TD and the result of the <cast
            // specification> has the Coercible coercibility characteristic."
            SqlCollation collation =
                new SqlCollation(SqlCollation.Coercibility.Coercible);

            Charset charset;
            if (null == charSetName) {
                charset = Util.getDefaultCharset();
            } else {
                charset = Charset.forName(charSetName);
            }
            type =
                typeFactory.createTypeWithCharsetAndCollation(type, charset,
                    collation);
        }

        if (null != collectionsTypeName) {
            final String collectionName = collectionsTypeName.getSimple();

            SqlTypeName collectionsSqlTypeName =
                SqlTypeName.get(collectionName);

            switch (collectionsSqlTypeName.getOrdinal()) {
            case SqlTypeName.Multiset_ordinal:
                type = typeFactory.createMultisetType(type, -1);
                break;

            default: Util.permAssert(false, "should never come here");
            }
        }

        return type;
    }
}


// End SqlDataTypeSpec.java
