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

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorNamespace;
import org.eigenbase.sql.validate.SqlMoniker;
import org.eigenbase.sql.validate.SqlMonikerImpl;
import org.eigenbase.sql.validate.SqlMonikerType;
import org.eigenbase.sql.validate.SqlMonikerComparator;
import org.eigenbase.util.Util;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;


/**
 * A <code>SqlIdentifier</code> is an identifier, possibly compound.
 */
public class SqlIdentifier extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    // REVIEW jvs 16-Jan-2005: I removed the final modifier to make it possibly
    // to expand qualifiers in place.  The original array was final, but its
    // contents weren't, so I've further degraded an imperfect situation.
    public String [] names;

    /** This identifiers collation (if any) */
    SqlCollation collation;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a compound identifier, for example <code>foo.bar</code>.
     *
     * @param names Parts of the identifier, length &gt;= 1
     */
    public SqlIdentifier(
        String [] names,
        SqlCollation collation,
        SqlParserPos pos)
    {
        super(pos);
        this.names = names;
        this.collation = collation;
    }

    public SqlIdentifier(
        String [] names,
        SqlParserPos pos)
    {
        super(pos);
        this.names = names;
        this.collation = null;
    }

    /**
     * Creates a simple identifier, for example <code>foo</code>.
     */
    public SqlIdentifier(
        String name,
        SqlCollation collation,
        SqlParserPos pos)
    {
        this(new String [] { name }, collation, pos);
    }

    /**
     * Creates a simple identifier, for example <code>foo</code>.
     */
    public SqlIdentifier(
        String name,
        SqlParserPos pos)
    {
        this(new String [] { name }, null, pos);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlKind getKind()
    {
        return SqlKind.Identifier;
    }

    public Object clone()
    {
        return new SqlIdentifier(
            Util.clone(names),
            collation,
            getParserPosition());
    }

    public String toString()
    {
        String s = names[0];
        for (int i = 1; i < names.length; i++) {
            s += ("." + names[i]);
        }
        return s;
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameType.Simple);
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            writer.sep(".");
            if (name.equals("*")) {
                writer.print(name);
            } else {
                writer.identifier(name);
            }
        }

        if (null != collation) {
            collation.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateIdentifier(this, scope);
    }

    /**
     * Lists all the valid alternatives for this identifier.
     *
     * @param validator Validator
     * @param scope Validation scope
     * @return an {@link SqlMoniker} array of valid options
     */
    public SqlMoniker[] findValidOptions(SqlValidator validator,
        SqlValidatorScope scope)
    {
        String tableName;
        ArrayList result = new ArrayList();
        if (names.length > 1) {
            tableName = names[names.length-2];
        } else {
            tableName = null;
            // table names are valid completion hints when the identifier
            // has only 1 name part
            scope.findAllTableNames(result);
            findAllValidFunctionNames(validator, scope, result);
        }
        findAllValidUDFNames(names, validator, result);
        // if the identifer has more than 1 part, use the tableName to limit
        // the choices of valid column names
        scope.findAllColumnNames(tableName, result);
        Collections.sort(result, new SqlMonikerComparator());
        return (SqlMoniker [])result.toArray(Util.emptySqlMonikerArray);
    }

    private void findAllValidUDFNames(String[] names,
                                      SqlValidator validator, 
                                      List result)
    {
        SqlMoniker [] objNames = 
            validator.getCatalogReader().getAllSchemaObjectNames(names);
        for (int i = 0; i < objNames.length; i++) {
            if (objNames[i].getType() == SqlMonikerType.Function) {
                result.add(objNames[i]);
            }
        }
    }

    private void findAllValidFunctionNames(SqlValidator validator, 
                                           SqlValidatorScope scope,
                                           List result)
    {   
        // a function name can only be 1 part
        if (names.length > 1) {
            return;
        }
        List opList = validator.getOperatorTable().getOperatorList();
        Iterator i = opList.iterator();
        while (i.hasNext()) {
            final SqlOperator op = (SqlOperator)i.next();
            SqlIdentifier curOpId = 
                new SqlIdentifier(op.getName(), getParserPosition());

            if (SqlUtil.makeCall(validator.getOperatorTable(), curOpId) 
                != null) {
                result.add(new SqlMonikerImpl(
                    op.getName(), SqlMonikerType.Function));
            }
            else {
                if (op.getSyntax() == SqlSyntax.Function ||
                    op.getSyntax() == SqlSyntax.Prefix) {
                    if (op.getOperandTypeChecker() != null) {
                        String sig = op.getAllowedSignatures();
                        sig = sig.replaceAll("'","");
                        result.add(new SqlMonikerImpl(sig, 
                            SqlMonikerType.Function));
                            continue;
                    }
                    result.add(new SqlMonikerImpl(op.getName(), 
                        SqlMonikerType.Function));
                }
            }
           
        }
    }

    public void validateExpr(SqlValidator validator, SqlValidatorScope scope)
    {
        // First check for builtin functions which don't have parentheses,
        // like "LOCALTIME".
        SqlCall call = SqlUtil.makeCall(validator.getOperatorTable(), this);
        if (call != null) {
            return;
        }

        validator.validateIdentifier(this, scope);
    }

    public boolean equalsDeep(SqlNode node)
    {
        if (!(node instanceof SqlIdentifier)) {
            return false;
        }
        SqlIdentifier that = (SqlIdentifier) node;
        if (this.names.length != that.names.length) {
            return false;
        }
        for (int i = 0; i < names.length; i++) {
            if (!this.names[i].equals(that.names[i])) {
                return false;
            }
        }
        return true;
    }

    public void accept(SqlVisitor visitor)
    {
        visitor.visit(this);
    }

    public SqlCollation getCollation()
    {
        return collation;
    }

    public void setCollation(SqlCollation collation)
    {
        this.collation = collation;
    }

    public String getSimple()
    {
        assert (names.length == 1);
        return names[0];
    }

    /**
     * Returns whether this identifier is a star, such as "*" or "foo.bar.*".
     */
    public boolean isStar()
    {
        return names[names.length - 1].equals("*");
    }

    /**
     * Returns whether this is a simple identifier. "FOO" is simple; "*",
     * "FOO.*" and "FOO.BAR" are not.
     */
    public boolean isSimple()
    {
        return names.length == 1 && !names[0].equals("*");
    }

    public boolean isMonotonic(SqlValidatorScope scope)
    {
        // First check for builtin functions which don't have parentheses,
        // like "LOCALTIME".
        SqlCall call = SqlUtil.makeCall(scope.getValidator().getOperatorTable(), this);
        if (call != null) {
            return call.isMonotonic(scope);
        }
        final SqlIdentifier fqId = scope.fullyQualify(this);
        SqlValidatorNamespace ns = null;
        for (int i = 0; i < fqId.names.length - 1; i++) {
            String name = fqId.names[i];
            ns = (i == 0) ?
                    scope.resolve(name, null, null) :
                    ns.lookupChild(name, null, null);
        }
        return ns.isMonotonic(fqId.names[fqId.names.length - 1]);
    }

    public boolean equalsBaseName(String name) {
        return names[names.length - 1].equals(name);
    }

}


// End SqlIdentifier.java
