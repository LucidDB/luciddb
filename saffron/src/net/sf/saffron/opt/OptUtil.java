/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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

package net.sf.saffron.opt;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.core.SaffronField;
import net.sf.saffron.rel.RelVisitor;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.ProjectRel;
import net.sf.saffron.rel.FilterRel;
import net.sf.saffron.rel.JoinRel;
import net.sf.saffron.rex.RexCorrelVariable;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexShuttle;
import net.sf.saffron.rex.RexBuilder;
import net.sf.saffron.rex.RexUtil;
import net.sf.saffron.rex.RexKind;
import net.sf.saffron.rex.RexInputRef;
import net.sf.saffron.rex.RexCall;
import net.sf.saffron.util.Util;
import openjava.ptree.Expression;
import openjava.ptree.FieldAccess;
import openjava.ptree.Variable;
import openjava.ptree.util.SyntheticClass;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;

/**
 * <code>OptUtil</code> defines static utility methods for use in optimizing
 * {@link SaffronRel}s.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 26 September, 2003
 */
public abstract class OptUtil
{
    //~ Static fields/initializers --------------------------------------------

    private static final Variable var0 = new Variable(makeName(0));
    private static final Variable var1 = new Variable(makeName(1));

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns the ordinal of the input represented by the variable
     * <code>name</code>, or -1 if it does not represent an input.
     */
    public static int getInputOrdinal(String name)
    {
        if (name.startsWith("$input")) {
            if (name.equals("$input0")) {
                return 0;
            } else if (name.equals("$input1")) {
                return 1;
            } else {
                throw Util.newInternal("unknown input variable: " + name);
            }
        } else {
            return -1;
        }
    }

    /**
     * Returns a list of variables set by a relational expression or its
     * descendants.
     */
    public static Set getVariablesSet(SaffronRel rel)
    {
        VariableSetVisitor visitor = new VariableSetVisitor();
        go(visitor,rel);
        return visitor.variables;
    }

    /**
     * Returns a set of distinct variables set by <code>rel0</code> and used
     * by <code>rel1</code>.
     */
    public static String [] getVariablesSetAndUsed(
        SaffronRel rel0,
        SaffronRel rel1)
    {
        Set set = getVariablesSet(rel0);
        if (set.size() == 0) {
            return Util.emptyStringArray;
        }
        Set used = getVariablesUsed(rel1);
        if (used.size() == 0) {
            return Util.emptyStringArray;
        }
        ArrayList result = new ArrayList();
        for (Iterator vars = set.iterator(); vars.hasNext();) {
            String s = (String) vars.next();
            if (used.contains(s) && !result.contains(s)) {
                result.add(s);
            }
        }
        if (result.size() == 0) {
            return Util.emptyStringArray;
        }
        String [] resultArray = new String[result.size()];
        result.toArray(resultArray);
        return resultArray;
    }

    /**
     * Returns a list of variables used by a relational expression or its
     * descendants. The list may contain duplicates.
     */
    public static Set getVariablesUsed(SaffronRel rel)
    {
        final VariableUsedVisitor vuv = new VariableUsedVisitor();
        go(
            new VisitorRelVisitor(vuv) {
                // implement RelVisitor
                public void visit(SaffronRel p,int ordinal,SaffronRel parent)
                {
                    if (p instanceof RelSubset) {
                        RelSubset subset = (RelSubset) p;
                        vuv.variables.addAll(subset.getVariablesUsed());
                    } else {
                        super.visit(p,ordinal,parent);
                    }

                    // Important! Remove stopped variables AFTER we visit children.
                    // (which what super.visit() does)
                    vuv.variables.removeAll(p.getVariablesStopped());
                }
            },
            rel);
        return vuv.variables;
    }

    public static SaffronRel clone(SaffronRel rel)
    {
        return (SaffronRel) rel.clone();
    }

    public static SaffronRel [] clone(SaffronRel [] rels)
    {
        rels = (SaffronRel []) rels.clone();
        for (int i = 0; i < rels.length; i++) {
            rels[i] = (SaffronRel) rels[i].clone();
        }
        return rels;
    }

    /**
     * Sets a {@link RelVisitor} going on a given relational expression, and
     * returns the result.
     */
    public static SaffronRel go(RelVisitor visitor,SaffronRel p)
    {
        RelHolder root = new RelHolder(p);
        try {
            visitor.visit(root,-1,null);
        } catch (Throwable e) {
            throw Util.newInternal(e,"while visiting tree");
        }
        return root.get();
    }

    /**
     * Constructs a reference to the <code>field</code><sup>th</sup> field of
     * the <code>ordinal</code><sup>th</sup> input.
     */
    public static FieldAccess makeFieldAccess(int ordinal,int field)
    {
        return new FieldAccess(
            new Variable(makeName(ordinal)),
            SyntheticClass.makeField(field));
    }

    /**
     * Constructs a reference to the <code>field</code><sup>th</sup> field of
     * an expression.
     */
    public static FieldAccess makeFieldAccess(Expression expr,int field)
    {
        return new FieldAccess(
            expr,
            SyntheticClass.makeField(field));
    }

    /**
     * Constructs a reference to a named field of an expression.
     */
    public static Expression makeFieldAccess(Expression expr, String fieldName) {
        return new FieldAccess(expr, fieldName);
    }

    /**
     * Constructs the name for the <code>ordinal</code>th input.  For example,
     * <code>makeName(0)</code> returns "$input0".
     */
    public static String makeName(int ordinal)
    {
        // avoid a memory allocation for the common cases
        switch (ordinal) {
        case 0:
            return "$input0";
        case 1:
            return "$input1";
        default:
            return "$input" + ordinal;
        }
    }

    public static Variable makeReference(int ordinal)
    {
        // save ourselves a memory allocation for the common cases
        switch (ordinal) {
        case 0:
            return var0;
        case 1:
            return var1;
        default:
            return new Variable(makeName(ordinal));
        }
    }

    public static String toString(SaffronRel [] a)
    {
        StringBuffer sb = new StringBuffer();
        sb.append("{");
        for (int i = 0; i < a.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(a[i].toString());
        }
        sb.append("}");
        return sb.toString();
    }

    public static String [] getFieldNames(SaffronType type)
    {
        int n = type.getFieldCount();
        String [] names = new String[n];
        SaffronField [] fields = type.getFields();
        for (int i = 0; i < n; ++i) {
            names[i] = fields[i].getName();
        }
        return names;
    }

    public static SaffronType createTypeFromProjection(
        final SaffronType type,final List columnNameList)
    {
        return type.getFactory().createProjectType(
            new SaffronTypeFactory.FieldInfo() 
            {
                public int getFieldCount()
                {
                    return columnNameList.size();
                }
                
                public String getFieldName(int index)
                {
                    return (String) columnNameList.get(index);
                }

                public SaffronType getFieldType(int index)
                {
                    int iField = type.getFieldOrdinal(getFieldName(index));
                    return type.getFields()[iField].getType();
                }
            });
    }

    public static boolean areRowTypesEqual(
        SaffronType rowType1,SaffronType rowType2)
    {
        if (rowType1 == rowType2) {
            return true;
        }
        int n = rowType1.getFieldCount();
        if (rowType2.getFieldCount() != n) {
            return false;
        }
        SaffronField [] f1 = rowType1.getFields();
        SaffronField [] f2 = rowType2.getFields();
        for (int i = 0; i < n; ++i) {
            if (!f1[i].getType().equals(f2[i].getType())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Create a ProjectRel which accomplishes a rename.
     *
     * @param outputType a row type descriptor whose field names the generated
     * ProjectRel must match
     *
     * @param rel the rel whose output is to be renamed; rel.getRowType() must
     * be the same as outputType except for field names
     *
     * @return generated ProjectRel
     */
    public static ProjectRel createRenameRel(
        SaffronType outputType,
        SaffronRel rel)
    {
        SaffronType inputType = rel.getRowType();

        int n = outputType.getFieldCount();
        RexNode[] renameExps = new RexNode[n];
        String [] renameNames = new String[n];

        SaffronField [] inputFields = inputType.getFields();
        SaffronField [] outputFields = outputType.getFields();

        final RexBuilder rexBuilder = rel.getCluster().rexBuilder;
        for (int i = 0; i < n; ++i) {
            assert(inputFields[i].getType().equals(outputFields[i].getType()));
            renameNames[i] = outputFields[i].getName();
            renameExps[i] = rexBuilder.makeInputRef(
                inputFields[i].getType(),
                inputFields[i].getIndex());
        }
            
        ProjectRel renameRel = new ProjectRel(
            rel.getCluster(),
            rel,
            renameExps,
            renameNames,
            ProjectRel.Flags.Boxed);
        
        return renameRel;
    }

    /**
     * Create a filter which will remove rows containing NULL values.
     *
     * @param rel the rel to be filtered
     *
     * @param fieldOrdinals array of 0-based field ordinals to filter,
     * or null for all fields
     *
     * @return filtered rel
     */
    public static SaffronRel createNullFilter(
        SaffronRel rel,
        Integer [] fieldOrdinals)
    {
        RexNode condition = null;
        RexBuilder rexBuilder = rel.getCluster().rexBuilder;
        SaffronType rowType = rel.getRowType();
        int n;
        if (fieldOrdinals != null) {
            n = fieldOrdinals.length;
        } else {
            n = rowType.getFieldCount();
        }
        SaffronField [] fields = rowType.getFields();
        for (int i = 0; i < n; ++i) {
            int iField;
            if (fieldOrdinals != null) {
                iField = fieldOrdinals[i].intValue();
            } else {
                iField = i;
            }
            SaffronType type = fields[iField].getType();
            if (!type.isNullable()) {
                continue;
            }
            RexNode newCondition = rexBuilder.makeCall(
                rexBuilder.operatorTable.isNotNullOperator,
                rexBuilder.makeInputRef(type,iField));
            if (condition == null) {
                condition = newCondition;
            } else {
                condition = rexBuilder.makeCall(
                    rexBuilder.operatorTable.andOperator,
                    condition,
                    newCondition);
            }
        }
        if (condition == null) {
            // no filtering required
            return rel;
        }
        return new FilterRel(
            rel.getCluster(),
            rel,
            condition);
    }

    /**
     * Create a projection which casts a rel's output to a desired row type.
     *
     * @param rel producer of rows to be converted
     *
     * @param castRowType row type after cast
     *
     * @return conversion rel
     */
    public static SaffronRel createCastRel(
        SaffronRel rel,
        SaffronType castRowType)
    {
        SaffronType rowType = rel.getRowType();
        if (areRowTypesEqual(rowType,castRowType)) {
            // nothing to do
            return rel;
        }
        String [] fieldNames = OptUtil.getFieldNames(rowType);
        RexNode [] castExps = RexUtil.generateCastExpressions(
            rel.getCluster().rexBuilder,
            castRowType,
            rowType);
        return new ProjectRel(
            rel.getCluster(),
            rel,
            castExps,
            fieldNames,
            ProjectRel.Flags.Boxed);
    }

    public static boolean analyzeSimpleEquiJoin(
        JoinRel joinRel,int [] joinFieldOrdinals)
    {
        RexNode joinExp = joinRel.getCondition();
        if (joinExp.getKind() != RexKind.Equals) {
            return false;
        }
        RexCall binaryExpression = (RexCall) joinExp;
        RexNode leftComparand = binaryExpression.operands[0];
        RexNode rightComparand = binaryExpression.operands[1];
        if (!(leftComparand instanceof RexInputRef)) {
            return false;
        }
        if (!(rightComparand instanceof RexInputRef)) {
            return false;
        }

        final int leftFieldCount =
            joinRel.getLeft().getRowType().getFieldCount();
        RexInputRef leftFieldAccess = (RexInputRef) leftComparand;
        if (!(leftFieldAccess.index < leftFieldCount)) {
            // left field must access left side of join
            return false;
        }
        
        RexInputRef rightFieldAccess = (RexInputRef) rightComparand;
        if (!(rightFieldAccess.index >= leftFieldCount)) {
            // right field must access right side of join
            return false;
        }

        joinFieldOrdinals[0] = leftFieldAccess.index;
        joinFieldOrdinals[1] = rightFieldAccess.index - leftFieldCount;
        return true;
    }

    //~ Inner Classes ---------------------------------------------------------

    private static class RelHolder extends SaffronRel
    {
        SaffronRel p;

        RelHolder(SaffronRel p)
        {
            super(p.getCluster());
            this.p = p;
        }

        public SaffronRel [] getInputs()
        {
            return new SaffronRel [] { p };
        }

        public void childrenAccept(RelVisitor visitor)
        {
            visitor.visit(p,0,this);
        }

        public Object clone()
        {
            throw new UnsupportedOperationException();
        }

        public SaffronType deriveRowType()
        {
            throw new UnsupportedOperationException();
        }

        public void replaceInput(int ordinalInParent,SaffronRel p)
        {
            assert(ordinalInParent == 0);
            this.p = p;
        }

        SaffronRel get()
        {
            return p;
        }
    }

    private static class VariableSetVisitor extends RelVisitor
    {
        HashSet variables = new HashSet();

        // implement RelVisitor
        public void visit(SaffronRel p,int ordinal,SaffronRel parent)
        {
            if (p instanceof RelSubset) {
                RelSubset subset = (RelSubset) p;
                variables.addAll(subset.getVariablesSet());
            } else {
                super.visit(p,ordinal,parent);
                String variable = p.getCorrelVariable();
                if (variable != null) {
                    variables.add(variable);
                }
            }

            // Important! Remove stopped variables AFTER we visit children
            // (which what super.visit() does)
            variables.removeAll(p.getVariablesStopped());
        }
    }

    private static class VariableUsedVisitor extends RexShuttle
    {
        HashSet variables = new HashSet();

        public RexNode visit(RexCorrelVariable p)
        {
            variables.add(p.getName());
            return p;
        }
    }
}


// End OptUtil.java
