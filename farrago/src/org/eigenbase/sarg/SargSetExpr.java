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
package org.eigenbase.sarg;

import org.eigenbase.reltype.*;
import org.eigenbase.util.*;
import org.eigenbase.rex.*;

import java.util.*;

/**
 * SargSetExpr represents the application of a {@link SargSetOp set operator}
 * to zero or more child {@link SargExpr sarg expressions}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SargSetExpr implements SargExpr
{
    private final SargFactory factory;

    private final RelDataType dataType;
    
    private final SargSetOperator setOp;
    
    private final List<SargExpr> children;

    /**
     * @see SargFactory.newSetExpr
     */
    SargSetExpr(
        SargFactory factory, RelDataType dataType, SargSetOperator setOp)
    {
        this.factory = factory;
        this.dataType = dataType;
        this.setOp = setOp;
        children = new ArrayList<SargExpr>();
    }

    /**
     * @return a read-only list of this expression's children
     * (the returned children themselves are modifiable)
     */
    public List<SargExpr> getChildren()
    {
        return Collections.unmodifiableList(children);
    }

    /**
     * Adds a child to this expression.
     *
     * @param child child to add
     */
    public void addChild(SargExpr child)
    {
        assert(child.getDataType() == dataType);
        if (setOp == SargSetOperator.COMPLEMENT) {
            assert(children.isEmpty());
        }
        children.add(child);
    }

    // implement SargExpr
    public SargFactory getFactory()
    {
        return factory;
    }
    
    // implement SargExpr
    public RelDataType getDataType()
    {
        return dataType;
    }

    // implement SargExpr
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(setOp);
        sb.append("(");
        for (SargExpr child : children) {
            sb.append(" ");
            sb.append(child);
        }
        sb.append(" )");
        return sb.toString();
    }
    
    // implement SargExpr
    public SargIntervalSequence evaluate()
    {
        switch(setOp) {
        case UNION:
            return evaluateUnion();
        case INTERSECTION:
            return evaluateIntersection();
        case COMPLEMENT:
            return evaluateComplement();
        default:
            throw Util.newInternal(setOp.toString());
        }
    }

    // implement SargExpr
    public void collectDynamicParams(Set<RexDynamicParam> dynamicParams)
    {
        for (SargExpr child : children) {
            child.collectDynamicParams(dynamicParams);
        }
    }

    private SargIntervalSequence evaluateUnion()
    {
        SargIntervalSequence seq = new SargIntervalSequence();
        
        // Evaluate all children and toss the results into one big sorted set.
        SortedSet<SargInterval> intervals =
            new TreeSet<SargInterval>(new IntervalComparator());
        for (SargExpr child : children) {
            intervals.addAll(child.evaluate().getList());
        }

	// Now, overlapping ranges are consecutive in the set.  Merge them by
	// increasing the upper bound of the first; discard the others.  In the
	// example, [4, 6] and [5, 7) are combined to form [4, 7).  (7, 8] is
	// not merged with the new range because neither range contains the
	// value 7.
	//
	// Input:
	//          1  2  3  4  5  6  7  8  9
	// 1 [1, 3] [-----]
	// 2 [4, 6]          [-----]
	// 3 [5, 7)             [-----)
	// 4 (7, 8]                   (--]
	// 
	// Output:
	// 1 [1, 3] [-----]
	// 2 [4, 7)          [--------)
	// 3 (7, 8]                   (--]
        SargInterval accumulator = null;
        for (SargInterval interval : intervals) {
            // Empty intervals should have been previously filtered out.
            assert(!interval.isEmpty());
            
            if (accumulator == null) {
                // The very first interval:  start accumulating.
                accumulator = new SargInterval(factory, getDataType());
                accumulator.copyFrom(interval);
                seq.addInterval(accumulator);
                continue;
            }

            if (accumulator.contains(interval)) {
                // Just drop new interval because it's already covered
                // by accumulator.
                continue;
            }

            // Test for overlap.
            int c = interval.getLowerBound().compareTo(
                accumulator.getUpperBound());

            // If no overlap, test for touching instead.
            if (c > 0) {
                if (interval.getLowerBound().isTouching(
                        accumulator.getUpperBound()))
                {
                    // Force test below to pass.
                    c = -1;
                }
            }

            if (c <= 0) {
                // Either touching or overlap:  grow the accumulator.
                accumulator.upperBound.copyFrom(interval.getUpperBound());
            } else {
                // Disjoint:  start accumulating a new interval
                accumulator = new SargInterval(factory, getDataType());
                accumulator.copyFrom(interval);
                seq.addInterval(accumulator);
            }
        }

        return seq;
    }

    private SargIntervalSequence evaluateIntersection()
    {
        SargIntervalSequence seq = null;
        
        if (children.isEmpty()) {
            // Counterintuitive but true: intersection of no sets is the
            // universal set (kinda like 2^0=1).  One way to prove this to
            // yourself is to apply DeMorgan's law.  The union of no sets is
            // certainly the empty set.  So the complement of that union is the
            // universal set.  That's equivalent to the intersection of the
            // complements of no sets, which is the intersection of no sets.
            // QED.
            seq = new SargIntervalSequence();
            seq.addInterval(new SargInterval(factory, getDataType()));
            return seq;
        }

        // The way we evaluate the intersection is to start with the first
        // child as a baseline, and then keep deleting stuff from it by
        // intersecting the other children in turn.  Whatever makes it through
        // this filtering remains as the final result.
        for (SargExpr child : children) {
            SargIntervalSequence newSeq = child.evaluate();
            if (seq == null) {
                // first child
                seq = child.evaluate();
                continue;
            }
            intersectSequences(seq, newSeq);
        }

        return seq;
    }

    private void intersectSequences(
        SargIntervalSequence targetSeq,
        SargIntervalSequence sourceSeq)
    {
        ListIterator<SargInterval> targetIter = targetSeq.list.listIterator();
        if (!targetIter.hasNext()) {
            // No target intervals at all, so quit.
            return;
        }

        ListIterator<SargInterval> sourceIter = sourceSeq.list.listIterator();
        if (!sourceIter.hasNext()) {
            // No source intervals at all, so result is empty
            targetSeq.list.clear();
            return;
        }
        
        // Start working on first source and target intervals
        SargInterval target = targetIter.next();
        SargInterval source = sourceIter.next();

        // loop invariant:  both source and target are non-null on entry
        for (;;) {
            if (source.getUpperBound().compareTo(target.getLowerBound()) < 0) {
                // Source is completely below target; discard it and
                // move on to next one.
                if (!sourceIter.hasNext()) {
                    // No more sources.
                    break;
                }
                source = sourceIter.next();
                continue;
            }

            if (target.getUpperBound().compareTo(source.getLowerBound()) < 0) {
                // Target is completely below source; discard it and
                // move on to next one.
                targetIter.remove();
                if (!targetIter.hasNext()) {
                    // All done.
                    return;
                }
                target = targetIter.next();
                continue;
            }

            // Overlap case:  perform intersection of the two intervals.
            
            if (source.getLowerBound().compareTo(target.getLowerBound()) > 0) {
                // Source starts after target starts, so trim the target.
                target.setLower(
                    source.getLowerBound().getCoordinate(),
                    source.getLowerBound().getStrictness());
            }

            int c = source.getUpperBound().compareTo(target.getUpperBound());
            if (c < 0) {
                // The source ends before the target ends, so split the target
                // into two parts.  The first part will be kept for sure; the
                // second part will be compared against further source ranges.
                SargInterval newTarget = new SargInterval(
                    factory, dataType);
                newTarget.setLower(
                    source.getUpperBound().getCoordinate(),
                    source.getUpperBound().getStrictnessComplement());

                // Trim current target to exclude the part of the range
                // which will move to newTarget.
                target.setUpper(
                    source.getUpperBound().getCoordinate(),
                    source.getUpperBound().getStrictness());

                // Insert newTarget after target.  This makes newTarget
                // into previous().
                targetIter.add(newTarget);

                // Next time through, work on newTarget.  By doing it
                // this way, we work around the fact that remove() can't
                // be called after add() without an intervening
                // call to next/previous().
                target = targetIter.previous();
                
                // Advance source.
                if (!sourceIter.hasNext()) {
                    break;
                }
                source = sourceIter.next();
            } else if (c == 0) {
                // Source and target ends coincide, so advance both source and
                // target.
                if (!targetIter.hasNext()) {
                    return;
                }
                target = targetIter.next();
                if (!sourceIter.hasNext()) {
                    break;
                }
                source = sourceIter.next();
            } else {
                // Source ends after target ends, so advance target.
                assert(c > 0);
                if (!targetIter.hasNext()) {
                    return;
                }
                target = targetIter.next();
            }
        }

        // Discard any remaining targets since they didn't have corresponding
        // sources.
        for (;;) {
            targetIter.remove();
            if (!targetIter.hasNext()) {
                break;
            }
            targetIter.next();
        }
    }

    private SargIntervalSequence evaluateComplement()
    {
        assert(children.size() == 1);
        SargExpr child = children.get(0);
        if (child instanceof SargIntervalExpr) {
            return evaluateIntervalComplement((SargIntervalExpr) child);
        }

        SargSetExpr setChild = (SargSetExpr) child;
            
        // Complement is its own inverse
        if (setChild.setOp == SargSetOperator.COMPLEMENT) {
            return setChild.children.get(0).evaluate();
        }

        // Use DeMorgan's Law:  complement of union is intersection of
        // complements, and vice versa
        SargSetOperator dualOp =
            (setChild.setOp == SargSetOperator.UNION)
            ? SargSetOperator.INTERSECTION
            : SargSetOperator.UNION;
        SargSetExpr dual = new SargSetExpr(factory, dataType, dualOp);
        for (SargExpr grandChild : setChild.children) {
            SargSetExpr comp = new SargSetExpr(
                factory, dataType, SargSetOperator.COMPLEMENT);
            comp.addChild(grandChild);
            dual.addChild(comp);
        }

        return dual.evaluate();
    }

    private SargIntervalSequence evaluateIntervalComplement(
        SargIntervalExpr intervalExpr)
    {
        SargIntervalSequence originalSeq = intervalExpr.evaluate();
        SargIntervalSequence seq = new SargIntervalSequence();

        // Complement of empty set is unconstrained set.
        if (originalSeq.getList().isEmpty()) {
            seq.addInterval(new SargInterval(factory, getDataType()));
            return seq;
        }

        assert(originalSeq.getList().size() == 1);
        SargInterval originalInterval = originalSeq.getList().get(0);

        // Complement of universal set is empty set.
        if (originalInterval.isUnconstrained()) {
            return seq;
        }

        SargInterval interval = new SargInterval(factory, getDataType());
        seq.addInterval(interval);
        
        if (originalInterval.getUpperBound().isFinite()
            && originalInterval.getLowerBound().isFinite())
        {
            // REVIEW jvs 18-Jan-2006:  handle (-infinity,null) case
            
            // Complement of a fully bounded range is the union of two
            // disjoint half-bounded ranges.
            interval.setUpper(
                originalInterval.getLowerBound().getCoordinate(),
                originalInterval.getLowerBound().getStrictnessComplement());

            interval = new SargInterval(factory, getDataType());
            seq.addInterval(interval);

            interval.setLower(
                originalInterval.getUpperBound().getCoordinate(),
                originalInterval.getUpperBound().getStrictnessComplement());
        } else if (originalInterval.getLowerBound().isFinite()) {
            // Complement of a half-bounded range is the opposite
            // half-bounded range (with open for closed and vice versa)
            interval.setUpper(
                originalInterval.getLowerBound().getCoordinate(),
                originalInterval.getLowerBound().getStrictnessComplement());
        } else {
            // Mirror image of previous case.
            assert(originalInterval.getUpperBound().isFinite());
            interval.setLower(
                originalInterval.getUpperBound().getCoordinate(),
                originalInterval.getUpperBound().getStrictnessComplement());
        }

        return seq;
    }

    /**
     * Comparator used in evaluateUnion.  Intervals collate based on
     * {lowerBound, upperBound}.
     */
    private static class IntervalComparator
        implements Comparator<SargInterval>
    {
        IntervalComparator()
        {
        }

        // implement Comparator
        public int compare(SargInterval i1, SargInterval i2)
        {
            int c = i1.getLowerBound().compareTo(i2.getLowerBound());
            if (c != 0) {
                return c;
            }

            return i1.getUpperBound().compareTo(i2.getUpperBound());
        }

        public boolean equals(Object obj)
        {
            return (obj instanceof IntervalComparator);
        }
    }
}

// End SargSetExpr.java
