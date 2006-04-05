/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.rel.metadata;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import java.util.*;

/**
 * RelMdColumnOrigins supplies a default implementation of
 * {@link RelMetadataQuery#getColumnOrigins} for the standard
 * logical algebra.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelMdColumnOrigins extends ReflectiveRelMetadataProvider
{
    public RelMdColumnOrigins()
    {
        // Tell superclass reflection about parameter types expected
        // for various metadata queries.

        // This corresponds to getColumnOrigins(rel, int iOutputColumn); note
        // that we don't specify the rel type because we always overload on
        // that.
        mapParameterTypes(
            "getColumnOrigins",
            Collections.singletonList((Class) Integer.TYPE));
    }

    public Set<RelColumnOrigin> getColumnOrigins(
        AggregateRelBase rel,
        int iOutputColumn)
    {
        if (iOutputColumn < rel.getGroupCount()) {
            // Group columns pass through directly.
            return RelMetadataQuery.getColumnOrigins(
                rel.getChild(),
                iOutputColumn);
        }
        // Aggregate columns are derived from input columns
        AggregateRel.Call call = rel.getAggCalls()[
            iOutputColumn - rel.getGroupCount()];
        
        Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
        for (int iInput : call.getArgs()) {
            Set<RelColumnOrigin> inputSet =
                RelMetadataQuery.getColumnOrigins(
                    rel.getChild(),
                    iInput);
            inputSet = createDerivedColumnOrigins(inputSet);
            if (inputSet != null) {
                set.addAll(inputSet);
            }
        }
        return set;
    }
    
    public Set<RelColumnOrigin> getColumnOrigins(
        JoinRelBase rel,
        int iOutputColumn)
    {
        int nLeftColumns = rel.getLeft().getRowType().getFieldList().size();
        Set<RelColumnOrigin> set;
        boolean derived = false;
        if (iOutputColumn < nLeftColumns) {
            set = RelMetadataQuery.getColumnOrigins(
                rel.getLeft(),
                iOutputColumn);
            if (rel.getJoinType().generatesNullsOnLeft()) {
                derived = true;
            }
        } else {
            set = RelMetadataQuery.getColumnOrigins(
                rel.getRight(),
                iOutputColumn - nLeftColumns);
            if (rel.getJoinType().generatesNullsOnRight()) {
                derived = true;
            }
        }
        if (derived) {
            // nulls are generated due to outer join; that counts
            // as derivation
            set = createDerivedColumnOrigins(set);
        }
        return set;
    }
    
    public Set<RelColumnOrigin> getColumnOrigins(
        SetOpRel rel,
        int iOutputColumn)
    {
        Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
        for (RelNode input : rel.getInputs()) {
            Set inputSet = 
                RelMetadataQuery.getColumnOrigins(
                    input,
                    iOutputColumn);
            if (inputSet == null) {
                return null;
            }
            set.addAll(inputSet);
        }
        return set;
    }
    
    public Set<RelColumnOrigin> getColumnOrigins(
        ProjectRelBase rel,
        int iOutputColumn)
    {
        final RelNode child = rel.getChild();
        RexNode rexNode = rel.getProjectExps()[iOutputColumn];
        
        if (rexNode instanceof RexInputRef) {
            // Direct reference:  no derivation added.
            RexInputRef inputRef = (RexInputRef) rexNode;
            return RelMetadataQuery.getColumnOrigins(
                child,
                inputRef.getIndex());
        }

        // Anything else is a derivation, possibly from multiple
        // columns.
        final Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
        RexVisitor visitor = new RexVisitorImpl(true) 
            {
                public void visitInputRef(RexInputRef inputRef)
                {
                    Set<RelColumnOrigin> inputSet =
                        RelMetadataQuery.getColumnOrigins(
                            child,
                            inputRef.getIndex());
                    if (inputSet != null) {
                        set.addAll(inputSet);
                    }
                }
            };
        rexNode.accept(visitor);

        return createDerivedColumnOrigins(set);
    }
    
    public Set<RelColumnOrigin> getColumnOrigins(
        FilterRelBase rel,
        int iOutputColumn)
    {
        return RelMetadataQuery.getColumnOrigins(
            rel.getChild(),
            iOutputColumn);
    }
    
    public Set<RelColumnOrigin> getColumnOrigins(
        SortRel rel,
        int iOutputColumn)
    {
        return RelMetadataQuery.getColumnOrigins(
            rel.getChild(),
            iOutputColumn);
    }
    
    // Catch-all rule when none of the others apply.
    public Set<RelColumnOrigin> getColumnOrigins(
        RelNode rel,
        int iOutputColumn)
    {
        // NOTE jvs 28-Mar-2006: We may get this wrong for a physical table
        // expression which supports projections.  In that case,
        // it's up to the plugin writer to override with the
        // correct information.

        if (rel.getInputs().length > 0) {
            // No generic logic available for non-leaf rels.
            return null;
        }
        
        Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
        
        RelOptTable table = rel.getTable();
        if (table == null) {
            // Somebody is making column values up out of thin air, like a
            // VALUES clause, so we return an empty set.
            return set;
        }

        // Detect the case where a physical table expression is performing
        // projection, and say we don't know instead of making any assumptions.
        // (Theoretically we could try to map the projection using column
        // names.)  This detection assumes the table expression doesn't handle
        // rename as well.
        if (table.getRowType() != rel.getRowType()) {
            return null;
        }
        
        set.add(new RelColumnOrigin(table, iOutputColumn, false));
        return set;
    }

    private Set<RelColumnOrigin> createDerivedColumnOrigins(
        Set<RelColumnOrigin> inputSet)
    {
        if (inputSet == null) {
            return null;
        }
        Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
        for (RelColumnOrigin rco : inputSet) {
            RelColumnOrigin derived = new RelColumnOrigin(
                rco.getOriginTable(),
                rco.getOriginColumnOrdinal(),
                true);
            set.add(derived);
        }
        return set;
    }
}

// End RelMdColumnOrigins.java
