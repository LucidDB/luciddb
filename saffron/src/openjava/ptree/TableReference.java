/*
 * $Id$
 */
package openjava.ptree;


import openjava.ptree.util.*;
import openjava.mop.*;

import org.eigenbase.reltype.*;
import org.eigenbase.relopt.*;
import org.eigenbase.oj.util.*;

// TODO jvs 12-Aug-2003:  add to visitor interfaces?

/**
 * TableReference specialized FieldAccess to hold information about the table
 * being accessed.
 */
public class TableReference extends FieldAccess
{
    /**
     * An access to the specified table.
     */
    public TableReference(Expression expr, String schemaName, String tableName)
    {
        set(expr,tableName,schemaName);
    }

    public String getQualifier()
    {
        return (String) (elementAt(2));
    }
    
    public boolean equals(ParseTree o)
    {
        if (!(o instanceof TableReference)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TableReference other = (TableReference) o;
        return getQualifier().equals(other.getQualifier());
    }

    public OJClass getType( Environment env )
        throws Exception
    {
        if (getQualifier() != null) {
            RelOptTable table = Toolbox.getTable(
                env,getReferenceExpr(),getQualifier(),getName());
            return OJClass.arrayOf(
                OJUtil.typeToOJClass(
                    table.getRowType(),
                    table.getRelOptSchema().getTypeFactory()));
        } else {
            return super.getType(env);
        }
    }
}
