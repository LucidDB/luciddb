/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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
package net.sf.saffron.sql;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.resource.SaffronResource;

/**
 * This should really be a subtype of SqlCall ...
 *
 * In its full glory, we will have to support complex type expressions like
 * ROW(    NUMBER(5,2) NOT NULL AS foo,
 *   ROW(        BOOLEAN AS b,         MyUDT NOT NULL AS i) AS rec)
 * Currently it only supports simple datatypes, like char,varchar, double, with
 * optional precision and scale.
 * @author Lee Schumacher
 * @since Jun 4, 2004
 * @version $Id$
 **/
public class SqlDataType extends SqlNode {

    private final SqlIdentifier typeName;
    private final int scale;
    private final int precision;

    private SaffronType type;

    private final String charSetName;


    public SaffronType getType() {
        return type;
    }
    
    public SqlDataType(SqlIdentifier typeName, int precision , int scale,
            String charSetName) {
        this.typeName = typeName;
        this.scale = scale;
        this.precision = precision;
        this.charSetName = charSetName;
    }

    public SqlIdentifier getTypeName() {
        return typeName;
    }

    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }

    public String getCharSetName() {
        return charSetName;
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
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        typeName.unparse(writer,leftPrec,rightPrec);
        if (precision > 0) {
            writer.print("(" + precision);
            if (scale > 0) {
                writer.print(", " + scale);
            }
            writer.print(")");
        }
        if (charSetName != null) {
            writer.print(" CHARACTER SET " + charSetName);
        }
    }

    public Object clone() {
        return new SqlDataType((SqlIdentifier)typeName.clone(), precision, scale, charSetName);
    }

    public SaffronType deriveType(SqlValidator validator) {
        SqlTypeName typeName;
        SaffronTypeFactory typeFactory = validator.typeFactory;
        String name = this.getTypeName().names[0];
        try {
            typeName = SqlTypeName.get(name);
        } catch (Error e) {
            throw SaffronResource.instance().newUnknownTypeName(name);
        }

        if (getPrecision() > 0 && getScale() > 0) {
            type = typeFactory.createSqlType(typeName, getPrecision(),
                    getScale());
        } else if (getPrecision() > 0) {
            type = typeFactory.createSqlType(typeName, getPrecision());
        } else {
            type = typeFactory.createSqlType(typeName);
        }
        return type;
    }

}

// End SqlDataType.java