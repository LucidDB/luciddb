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

    /*
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
        */

    public static LoptIterCalcRule defaultInstance =
        new DefaultRule(
            new RelOptRuleOperand(
                IterCalcRel.class, null));

    // index acess rule, hash rules

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
     * the elements qualified name, joined by dots, in other words:
     * "<code>catalog.schema.table</code>".
     * @param qualifiedName a qualified table name
     */
    protected String getTableTag(String[] qualifiedName)
    {
        assert (qualifiedName.length == 3);
        StringBuffer sb = new StringBuffer(qualifiedName[0]);
        for (int i = 1; i < qualifiedName.length; i++) {
            sb.append(".").append(qualifiedName[i]);
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
        return "IterCalcRel" + rel.getId();
    }

    //~ Inner Classes ----------------------------------------------------------

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
                tableRel.getTable().getQualifiedName());
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

            String tag = "Jdbc_" + getDefaultTag(calc);
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

            String tag = "JavaUdx_" + getDefaultTag(calc);
            transformToTag(call, calc, tag);
        }
    }

    /**
     * A rule for tagging a calculator beneath a table modification.
     */
    /*
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
            String tag = getTableTag(tableRel.getTable().getQualifiedName());
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
    */

    /**
     * A rule for tagging a calculator beneath a table modification.
     */
    /*
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
            String tag = getTableTag(tableRel.getTable().getQualifiedName());
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
    */

    /**
     * A rule for tagging a calculator beneath a table modification.
     */
    /*
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
            String tag = getTableTag(tableRel.getTable().getQualifiedName());
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
    */

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
