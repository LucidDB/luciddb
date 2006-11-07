/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package com.lucidera.opt;

import com.lucidera.lcs.*;

import net.sf.farrago.query.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.jdbc.*;
import org.eigenbase.relopt.*;

import java.util.*;
import java.text.*;


/**
 * LoptIterCalcRule decorates an IterCalcRel with an error handling tag, 
 * according to the LucidDb requirements.
 *
 * @author John Pham
 * @version $Id$
 */
public abstract class LoptIterCalcRule extends RelOptRule
{
    public static LoptIterCalcRule tableAccessInstance = 
        new TableAccessRule(
            new RelOptRuleOperand(
                IterCalcRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        ConverterRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(
                                TableAccessRelBase.class, 
                                null)
                    })
        }));

    public static LoptIterCalcRule jdbcQueryInstance = 
        new JdbcQueryRule(
            new RelOptRuleOperand(
                IterCalcRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        ConverterRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(
                                JdbcQuery.class, 
                                null)
                    })
        }));

    public static LoptIterCalcRule javaUdxInstance = 
        new JavaUdxRule(
            new RelOptRuleOperand(
                IterCalcRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        FarragoJavaUdxRel.class,
                        null)
                    }));

    public static LoptIterCalcRule lcsAppendInstance =
        new TableAppendRule(
            new RelOptRuleOperand(
                LcsTableAppendRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        ConverterRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(
                                IterCalcRel.class,
                                null)
                    })
        }));

    public static LoptIterCalcRule lcsMergeInstance =
        new TableMergeRule(
            new RelOptRuleOperand(
                LcsTableMergeRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        ConverterRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(
                                IterCalcRel.class,
                                null)
                    })
        }));

    public static LoptIterCalcRule lcsDeleteInstance =
        new TableDeleteRule(
            new RelOptRuleOperand(
                LcsTableDeleteRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        ConverterRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(
                                IterCalcRel.class,
                                null)
                    })
        }));

    public static LoptIterCalcRule hashJoinInstance =
        new HashJoinRule(
            new RelOptRuleOperand(
                IterCalcRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        ConverterRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(
                                LhxJoinRel.class,
                                null)
                    })
        }));

    public static LoptIterCalcRule defaultInstance =
        new DefaultRule(
            new RelOptRuleOperand(
                IterCalcRel.class, null));

    // index acess rule, hash rules

    private static String tagTimestampFormat = "yyyy-MM-dd_HH:mm:ss";
    private static DateFormat tagTimestampFormatter = 
        new SimpleDateFormat(tagTimestampFormat);

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new LoptIterCalcRule
     */
    public LoptIterCalcRule(RelOptRuleOperand operand)
    {
        super(operand);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Transform call to an IterCalcRel with a replaced tag
     */
    protected void transformToTag(
        RelOptRuleCall call, IterCalcRel calc, String tag)
    {
        call.transformTo(replaceTag(calc, tag));
    }

    /**
     * Sets the tag of an IterCalcRel to the specified tag
     * @return a new IterCalcRel with the specified tag
     */
    protected IterCalcRel replaceTag(IterCalcRel calc, String tag)
    {
        return
            new IterCalcRel(
                calc.getCluster(),
                calc.getChild(),
                calc.getProgram(),
                calc.getFlags(),
                tag);
    }

    /**
     * Replaces the tag of an IterCalcRel underneath an 
     * IteratorToFennelConverter. Replaces the tag, then duplicates 
     * the converter.
     * @return the duplicated converter
     */
    protected IteratorToFennelConverter replaceTagAsFennel(
        IteratorToFennelConverter converter, IterCalcRel calc, String tag)
    {
        IterCalcRel newCalc = replaceTag(calc, tag);
        return 
            new IteratorToFennelConverter(
                converter.getCluster(),
                newCalc);
    }

    /**
     * Gets a tag corresponding to a table name. The tag is built from 
     * elements of the qualified name, joined by dots. The tag is prefixed 
     * with an action name, and is optionally suffixed with a unique 
     * identifier. The tag has the overall format:
     * "<code>[action].catalog.schema.table[.uniqueSuffix]</code>".
     * The unique suffix is appended when the table's relation is provided. 
     * The suffix has a combination of the relation's runtime id and the 
     * current timestamp.
     * @param action an action such as "delete" or "merge"
     * @param qualifiedName a qualified table name
     * @param rel the relation accessing a table for read or write. If 
     *   not null, the relation is used to generate a unique suffix.
     */
    protected String getTableTag(
        String action, String[] qualifiedName, RelNode rel)
    {
        assert (qualifiedName.length == 3);
        StringBuffer sb = new StringBuffer(action);
        for (int i = 0; i < qualifiedName.length; i++) {
            sb.append(".").append(qualifiedName[i]);
        }
        if (rel != null) {
            sb.append("." + rel.getId());
            sb.append("_" + tagTimestampFormatter.format(new Date()));
        }
        return sb.toString();
    }

    /**
     * Gets the default tag for an IterCalcRel, based upon its id.
     * The generated tag will be unique for the server process.
     * @param rel the relation to build a tag for
     */
    protected String getDefaultTag(IterCalcRel rel)
    {
        // the timestamp should guarantee a unique default tag
        // and might be more readable than a uuid
        return "IterCalcRel" + rel.getId() + "_" 
            + tagTimestampFormatter.format(new Date());
    }

    protected void setIterCalcTypeMap(
        FennelRel rel,
        String tag)
    {
        FarragoPreparingStmt stmt = 
            FennelRelUtil.getPreparingStmt(rel);
        stmt.mapIterCalcType(tag, rel.getExpectedInputRowType(0));
    }

    //~ Inner Classes ----------------------------------------------------------

    public static final String TABLE_ACCESS_PREFIX = "Read";
    public static final String JDBC_QUERY_PREFIX = "Jdbc";
    public static final String JAVA_UDX_PREFIX = "JavaUdx";
    public static final String TABLE_APPEND_PREFIX = "Insert";
    public static final String TABLE_MERGE_PREFIX = "Merge";
    public static final String TABLE_DELETE_PREFIX = "Delete";
    public static final String HASH_JOIN_PREFIX = "PostJoin";

    /**
     * A rule for tagging a calculator on top of a table scan.
     */
    private static class TableAccessRule extends LoptIterCalcRule
    {
        public TableAccessRule(RelOptRuleOperand operand)
        {
            super(operand);
        }
        
        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            IterCalcRel calc = (IterCalcRel) call.rels[0];
            if (calc.getTag() != null) {
                return;
            }

            TableAccessRelBase tableRel = (TableAccessRelBase) call.rels[2];
            String tag = getTableTag(
                TABLE_ACCESS_PREFIX,
                tableRel.getTable().getQualifiedName(), null);
            transformToTag(call, calc, tag);
        }
    }

    /**
     * A rule for tagging a calculator on top of a JDBC query
     */
    private static class JdbcQueryRule extends LoptIterCalcRule
    {
        public JdbcQueryRule(RelOptRuleOperand operand)
        {
            super(operand);
        }
        
        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            IterCalcRel calc = (IterCalcRel) call.rels[0];
            if (calc.getTag() != null) {
                return;
            }

            String tag = JDBC_QUERY_PREFIX + getDefaultTag(calc);
            transformToTag(call, calc, tag);
        }
    }

    /**
     * A rule for tagging a calculator on top of a Java UDX
     */
    private static class JavaUdxRule extends LoptIterCalcRule
    {
        public JavaUdxRule(RelOptRuleOperand operand)
        {
            super(operand);
        }
        
        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            IterCalcRel calc = (IterCalcRel) call.rels[0];
            if (calc.getTag() != null) {
                return;
            }

            String tag = JAVA_UDX_PREFIX + getDefaultTag(calc);
            transformToTag(call, calc, tag);
        }
    }

    /**
     * A rule for tagging a calculator beneath a table modification.
     */
    private static class TableAppendRule extends LoptIterCalcRule
    {
        public TableAppendRule(RelOptRuleOperand operand)
        {
            super(operand);
        }
        
        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            IterCalcRel calc = (IterCalcRel) call.rels[2];
            if (calc.getTag() != null) {
                return;
            }

            LcsTableAppendRel tableRel = (LcsTableAppendRel) call.rels[0];
            IteratorToFennelConverter converter = 
                (IteratorToFennelConverter) call.rels[1];
            String tag = getTableTag(
                TABLE_APPEND_PREFIX, 
                tableRel.getTable().getQualifiedName(), 
                tableRel);

            setIterCalcTypeMap(tableRel, tag);
            call.transformTo(
                new LcsTableAppendRel(
                    tableRel.getCluster(),
                    tableRel.getLcsTable(),
                    tableRel.getConnection(),
                    replaceTagAsFennel(converter, calc, tag),
                    tableRel.getOperation(),
                    tableRel.getUpdateColumnList()));
        }
    }

    /**
     * A rule for tagging a calculator beneath a table modification.
     */
    private static class TableMergeRule extends LoptIterCalcRule
    {
        public TableMergeRule(RelOptRuleOperand operand)
        {
            super(operand);
        }
        
        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            IterCalcRel calc = (IterCalcRel) call.rels[2];
            if (calc.getTag() != null) {
                return;
            }

            LcsTableMergeRel tableRel = (LcsTableMergeRel) call.rels[0];
            IteratorToFennelConverter converter = 
                (IteratorToFennelConverter) call.rels[1];
            String tag = getTableTag(
                TABLE_MERGE_PREFIX, 
                tableRel.getTable().getQualifiedName(),
                tableRel);

            setIterCalcTypeMap(tableRel, tag);
            call.transformTo(
                new LcsTableMergeRel(
                    tableRel.getCluster(),
                    tableRel.getLcsTable(),
                    tableRel.getConnection(),
                    replaceTagAsFennel(converter, calc, tag),
                    tableRel.getOperation(),
                    tableRel.getUpdateColumnList(),
                    tableRel.getUpdateOnly()));
        }
    }

    /**
     * A rule for tagging a calculator beneath a table modification.
     */
    private static class TableDeleteRule extends LoptIterCalcRule
    {
        public TableDeleteRule(RelOptRuleOperand operand)
        {
            super(operand);
        }
        
        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            IterCalcRel calc = (IterCalcRel) call.rels[2];
            if (calc.getTag() != null) {
                return;
            }

            LcsTableDeleteRel tableRel = (LcsTableDeleteRel) call.rels[0];
            IteratorToFennelConverter converter = 
                (IteratorToFennelConverter) call.rels[1];
            String tag = getTableTag(
                TABLE_DELETE_PREFIX, 
                tableRel.getTable().getQualifiedName(), 
                tableRel);

            setIterCalcTypeMap(tableRel, tag);
            call.transformTo(
                new LcsTableDeleteRel(
                    tableRel.getCluster(),
                    tableRel.getLcsTable(),
                    tableRel.getConnection(),
                    replaceTagAsFennel(converter, calc, tag),
                    tableRel.getOperation(),
                    tableRel.getUpdateColumnList()));
        }
    }

    /**
     * A rule for tagging a calculator on top of a hash join.
     */
    private static class HashJoinRule extends LoptIterCalcRule
    {
        public HashJoinRule(RelOptRuleOperand operand)
        {
            super(operand);
        }
        
        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            IterCalcRel calc = (IterCalcRel) call.rels[0];
            if (calc.getTag() != null) {
                return;
            }

            String tag = HASH_JOIN_PREFIX + getDefaultTag(calc);
            transformToTag(call, calc, tag);
        }
    }

    /**
     * A default rule for tagging any calculator
     */
    private static class DefaultRule extends LoptIterCalcRule
    {
        public DefaultRule(RelOptRuleOperand operand)
        {
            super(operand);
        }
        
        // implement RelOptRule
        public void onMatch(RelOptRuleCall call)
        {
            IterCalcRel calc = (IterCalcRel) call.rels[0];
            if (calc.getTag() != null) {
                return;
            }
            transformToTag(call, calc, getDefaultTag(calc));
        }
    }
}

// End FlatFileIterRule.java
