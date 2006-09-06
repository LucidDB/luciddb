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

import com.lucidera.farrago.namespace.flatfile.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;

/**
 * LoptIterCalcRule decorates an IterCalcRel with an error handling tag, 
 * according to the LucidDb requirements. Initially it decorates an 
 * IterCalcRel if it is adjacent to a TableAccessRelBase.
 *
 * @author John Pham
 * @version $Id$
 */
public class LoptIterCalcRule extends RelOptRule
{
    public static LoptIterCalcRule flatfileInstance = 
        new LoptIterCalcRule(
            new RelOptRuleOperand(
                IterCalcRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        ConverterRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(
                                FlatFileFennelRel.class, 
                                null)
                    })
        }));

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsIndexAccessRule object.
     */
    public LoptIterCalcRule(RelOptRuleOperand operand)
    {
        super(operand);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        IterCalcRel calc = (IterCalcRel) call.rels[0];
        FlatFileFennelRel flatfile = (FlatFileFennelRel) call.rels[2];

        if (calc.getTag() != null) {
            return;
        }
        
        String tag = makeTag(flatfile.getTable().getQualifiedName());
        
        IterCalcRel newCalc = 
            new IterCalcRel(
                calc.getCluster(),
                calc.getChild(),
                calc.getProgram(),
                calc.getFlags(),
                tag);
        call.transformTo(newCalc);
    }

    String makeTag(String[] qualifiedName)
    {
        assert (qualifiedName.length == 3);
        StringBuffer sb = new StringBuffer(qualifiedName[0]);
        for (int i = 1; i < qualifiedName.length; i++) {
            sb.append(".").append(qualifiedName[i]);
        }
        return sb.toString();
    }
}

// End FlatFileIterRule.java
