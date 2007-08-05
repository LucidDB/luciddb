/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package net.sf.farrago.query;

import java.util.*;

import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;
import org.eigenbase.sql.type.*;


/**
 * FennelNestedLoopJoinRule is a rule for converting a {@link JoinRel} with
 * a join condition into a {@link FennelNestedLoopJoinRel}.  The nested loop
 * join is executed by creating a temporary index on the right join input and
 * using join keys from the left input to do lookups against that index.
 * 
 * <p>
 * Any sargable join predicates that aren't part of the index lookup are
 * processed by a {@link FennelReshapeRel}.  And then finally, any remaining
 * join predicates are processed by a Calc node.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelNestedLoopJoinRule
    extends RelOptRule
{

    //~ Constructors -----------------------------------------------------------

    public FennelNestedLoopJoinRule()
    {
        super(new RelOptRuleOperand(
            JoinRel.class,
            null));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];

        RelNode leftRel = joinRel.getLeft();
        RelNode rightRel = joinRel.getRight();

        // Not possible to process FULL outer joins using nested loop joins
        JoinRelType joinType = joinRel.getJoinType();
        if (joinType == JoinRelType.FULL) {
            return;
        }
        RexNode condition = joinRel.getCondition();
        // If there's no join condition, process the join as a cartesian
        // product join
        if (condition.isAlwaysTrue()) {
            return;
        }

        // Only left outer joins are handled, so we need to swap the join
        // inputs
        boolean swapped = false;
        if (joinType == JoinRelType.RIGHT) {
            RelNode swappedRelNode = SwapJoinRule.swap(joinRel, true);
            assert(swappedRelNode != null);
            JoinRel swappedJoinRel = (JoinRel) swappedRelNode.getInput(0);
            swapped = true;
            joinType = swappedJoinRel.getJoinType();
            leftRel = swappedJoinRel.getLeft();
            rightRel = swappedJoinRel.getRight();
            condition = swappedJoinRel.getCondition();
        }

        // Look for sargable predicates.  Note that sargable in this case
        // means that instead of comparing the RHS input against
        // literals, we're comparing the RHS against the LHS.
        //
        // NOTE zfong 5/30/07 - Currently, we're calling SargRexAnalyzer in
        // simple mode, which means we'll only treat a single predicate on
        // each column from the RHS input as sargable.  That means if you
        // have filters like: (RHS.col > LHS.col1 and RHS.col < LHS.col2),
        // we won't create two endpoints for the seach on RHS.col.
        SargFactory sargFactory =
            new SargFactory(joinRel.getCluster().getRexBuilder());
        SargRexAnalyzer rexAnalyzer =
            sargFactory.newRexAnalyzer(0, leftRel.getRowType().getFieldCount());
        List<SargBinding> sargBindingList = rexAnalyzer.analyzeAll(condition);

        // Find the subset of sargable predicates that will be used
        // with the temporary index
        List<Integer> indexCols = new ArrayList<Integer>();
        List<Integer> indexOperands = new ArrayList<Integer>();
        List<CompOperatorEnum> indexOpList = new ArrayList<CompOperatorEnum>();
        int nLeftFields = leftRel.getRowType().getFieldCount();
        RelDataType[] castFromTypes = new RelDataType[nLeftFields];
        RelDataTypeFactory typeFactory = joinRel.getCluster().getTypeFactory();
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        condition = getIndexablePredicates(
            sargBindingList,
            nLeftFields,
            indexCols,
            indexOperands,
            indexOpList,
            castFromTypes,
            typeFactory,
            rexBuilder,
            rexAnalyzer);
        
        // Apply the RexAnalyzer on the remaining predicates to locate 
        // predicates that we can apply in a FennelReshapeRel
        List<Integer> filterCols = new ArrayList<Integer>(); 
        List<Integer> filterOperands = new ArrayList<Integer>();
        List<CompOperatorEnum> filterOpList = new ArrayList<CompOperatorEnum>();
        sargBindingList = rexAnalyzer.analyzeAll(condition);
        RexNode residualCondition = getFilterPredicates(
            sargBindingList,
            nLeftFields,
            filterCols,
            filterOperands,
            filterOpList,
            castFromTypes,
            typeFactory,
            rexBuilder,
            rexAnalyzer);       
        
        // Cast the left input, if needed
        RelNode castedLeftRel = castLeftRel(leftRel, castFromTypes);
        boolean castRequired = (castedLeftRel != leftRel);
        
        // Create the nested loop join rel with additional projections as
        // needed due to casting and/or swapping
        CompOperatorEnum indexOp;
        if (indexCols.isEmpty()) {
            indexOp = CompOperatorEnum.COMP_NOOP;
        } else {
            indexOp = indexOpList.get(0);
        }
        CompOperatorEnum filterOp;
        if (filterCols.isEmpty()) {
            filterOp = CompOperatorEnum.COMP_NOOP;
        } else {
            filterOp = filterOpList.get(0);
        }
        RelNode nestedLoopRel =
            createNestedLoopRel(
                joinRel,
                castedLeftRel,
                rightRel,
                swapped,
                castRequired,
                joinType,
                indexCols,
                indexOperands,
                indexOp,
                filterCols,
                filterOperands,
                filterOp,
                residualCondition);
            
        if (nestedLoopRel == null) {
            return;
        }
        call.transformTo(nestedLoopRel);
    }
    
    /**
     * Creates a nested loop join tree with the appropriate inputs.
     * 
     * <p>In the new tree, the first input is the left join input.  The left
     * join input is assumed to have been casted, as needed, to match the
     * right join input.
     * 
     * <p>The right input does all the necessary lookups and filtering on
     * the right input by reading dynamic parameters corresponding to the
     * left input.
     * 
     * <p>An optional third input creates the temporary index used in join
     * lookups if a temporary index is to be used
     * 
     * @param origJoinRel original join tree
     * @param leftRel left input into the new join
     * @param rightRel right input into the new join
     * @param swapped true if the original inputs were swapped
     * @param castRequired true if the left input had to be cast
     * @param joinType join type
     * @param indexCols list of column offsets from the RHS input that
     * are to be indexed
     * @param indexOperands list of column offsets from the LHS input
     * that will be used as index lookup keys
     * @param indexOp the operator to be used in the index lookup
     * @param filterCols list of column offsets from the RHS input that can
     * be applied in FennelReshapeRel
     * @param filterOperands list of the LHS input that the filterCols are
     * compared against
     * @param filterOp comparison operator that will be applied in
     * FennelReshapeRel
     * @param residualCondition remaining filter that must be applied in a
     * Calc node
     * 
     * @return created nested loop join tree
     */
    private RelNode createNestedLoopRel(
        JoinRel origJoinRel,
        RelNode leftRel,
        RelNode rightRel,
        boolean swapped,
        boolean castRequired,
        JoinRelType joinType,
        List<Integer> indexCols,
        List<Integer> indexOperands,
        CompOperatorEnum indexOp,
        List<Integer> filterCols,
        List<Integer> filterOperands,
        CompOperatorEnum filterOp,
        RexNode residualCondition)
    {
        int nInputs = (indexCols.isEmpty()) ? 2 : 3;
        RelNode [] inputs = new RelNode[nInputs];
        
        leftRel = convertInput(origJoinRel.getTraits(), leftRel);
        if (leftRel == null) {
            return null;
        }
        inputs[0] = leftRel;

        // Create the dynamic parameters corresponding to the left join keys,
        // allocating only one parameter even if a key is referenced multiple
        // times
        Map<Integer, FennelRelParamId> joinKeyParamMap =
            new HashMap<Integer, FennelRelParamId>();
        FennelRelImplementor implementor =
            FennelRelUtil.getRelImplementor(leftRel);
        createJoinKeyParameters(implementor, indexOperands, joinKeyParamMap);
        createJoinKeyParameters(implementor, filterOperands, joinKeyParamMap);
        
        // Find all references to the LHS in the residual condition.  
        // Dynamic parameters need to be created for these as well.
        List<Integer> residualRefs = new ArrayList<Integer>();
        if (residualCondition != null) {
            findLeftInputRefs(
                residualCondition,
                leftRel.getRowType().getFieldCount(),
                residualRefs);
            createJoinKeyParameters(implementor, residualRefs, joinKeyParamMap);
        }
        
        FennelRelParamId rootPageIdParamId = null;
        if (nInputs == 3) {
            rootPageIdParamId = implementor.allocateRelParamId();
        }
        
        // Create the second input into the nested loop join -- the input
        // that does the index lookup and any other additional filtering 
        rightRel = convertInput(origJoinRel.getTraits(), rightRel);
        if (rightRel == null) {
            return null;
        }
        inputs[1] =
            createSecondInput(
                leftRel,
                rightRel,
                indexCols,
                indexOperands,
                indexOp,
                joinKeyParamMap,
                rootPageIdParamId,
                filterCols,
                filterOperands,
                filterOp,
                residualRefs,
                residualCondition);
        if (inputs[1] == null) {
            return null;
        }
        
        // Create the third input, which builds the temporary index
        if (nInputs == 3) {
            inputs[2] =
                createThirdInput(
                    rightRel,
                    indexCols,
                    rootPageIdParamId);
        }

        Set<Integer> leftJoinKeys = joinKeyParamMap.keySet();
        FennelRelParamId [] joinKeyParamIds =
            new FennelRelParamId[leftJoinKeys.size()];
        int joinKey = 0;
        for (Integer leftJoinKey : leftJoinKeys) {
            joinKeyParamIds[joinKey++] = joinKeyParamMap.get(leftJoinKey);
        }
        RelDataType nestedLoopRowType =
            JoinRel.deriveJoinRowType(
                leftRel.getRowType(),
                rightRel.getRowType(),
                joinType,
                origJoinRel.getCluster().getTypeFactory(),
                null);
        Double rowCount = RelMetadataQuery.getRowCount(origJoinRel);
        assert(rowCount != null);
        FennelNestedLoopJoinRel nestedLoopRel =
            new FennelNestedLoopJoinRel(
                origJoinRel.getCluster(),
                inputs,
                joinType,
                RelOptUtil.getFieldNameList(nestedLoopRowType),
                leftJoinKeys.toArray(new Integer[leftJoinKeys.size()]),
                joinKeyParamIds,
                rootPageIdParamId,
                rowCount);
        
        // If we had to cast the LHS, then we need to cast the join result
        // back to the original rowtype
        RelNode finalRel;
        RelDataType preCastRowType;
        if (swapped) {         
            preCastRowType =
                JoinRel.deriveJoinRowType(
                    origJoinRel.getRight().getRowType(),
                    origJoinRel.getLeft().getRowType(),
                    joinType,
                    origJoinRel.getCluster().getTypeFactory(),
                    null);
        } else {
            preCastRowType = origJoinRel.getRowType();
        }
        if (castRequired) {
            finalRel = RelOptUtil.createCastRel(
                nestedLoopRel,
                preCastRowType,
                true);
        } else {
            finalRel = nestedLoopRel;
        }
        
        // If the inputs were swapped, create a projection reflecting the
        // original input ordering
        if (swapped) {
            final RexNode[] exps =
                RelOptUtil.createSwappedJoinExprs(finalRel, origJoinRel, true);
            finalRel = CalcRel.createProject(
                finalRel,
                exps,
                RelOptUtil.getFieldNames(origJoinRel.getRowType()),
                true);
        }
        
        return finalRel;
    }
  
    /**
     * Extracts from a list of sargable bindings the subset that can be used
     * as lookups using a temporary index to be built on the right join input
     * 
     * @param sargBindingList original list of sargable bindings
     * @param nLeftFields number of fields in LHS input
     * @param indexCols returns list of column offsets from the right
     * join input that are to be indexed
     * @param indexOperands returns list of column offsets from the left
     * join input that will be used as index lookup keys
     * @param indexOp returns the operator to be used in the index lookup
     * @param castFromTypes stores the type that the LHS input needs to be
     * cast from if its type doesn't match the RHS input it is being compared
     * against
     * @param typeFactory type factory
     * @param rexBuilder rex builder
     * @param rexAnalyzer rex analyzer
     * 
     * @return excess predicates that cannot be processed by the index
     */
    private RexNode getIndexablePredicates(
        List<SargBinding> sargBindingList,
        int nLeftFields,
        List<Integer> indexCols,
        List<Integer> indexOperands,
        List<CompOperatorEnum> indexOp,
        RelDataType [] castFromTypes,
        RelDataTypeFactory typeFactory,
        RexBuilder rexBuilder,
        SargRexAnalyzer rexAnalyzer)
    {
        boolean rangeFound = false;
        Integer rangeInputRef = null;
        SargIntervalSequence rangeSargSeq = null;
        List<SargBinding> nonIndexBindingList = new ArrayList<SargBinding>();
        for (SargBinding sargBinding : sargBindingList) {
            SargIntervalSequence sargSeq =
                FennelRelUtil.evaluateSargExpr(sargBinding.getExpr());
            
            if (sargSeq.isPoint()) {
                if (checkSargIntervalTypes(
                    typeFactory,
                    castFromTypes,
                    sargBinding.getInputRef(),
                    sargSeq,
                    true))
                {
                    indexCols.add(
                        sargBinding.getInputRef().getIndex() - nLeftFields);
                    SargEndpoint lowerBound =
                        sargSeq.getList().get(0).getLowerBound();
                    RexNode coordinate = lowerBound.getCoordinate();
                    indexOperands.add(((RexInputRef) coordinate).getIndex());
                } else {
                    nonIndexBindingList.add(sargBinding);
                }
            
            // Only one range predicate should have been found and it can't
            // be a multi-range predicate
            } else {
                assert(!rangeFound);
                if (sargSeq.getList().size() == 1 &&
                    checkSargIntervalTypes(
                        typeFactory,
                        castFromTypes,
                        sargBinding.getInputRef(),
                        sargSeq,
                        false))
                {
                    rangeFound = true;
                    rangeInputRef =
                        sargBinding.getInputRef().getIndex() - nLeftFields;
                    rangeSargSeq = sargSeq;
                } else {
                    nonIndexBindingList.add(sargBinding);
                }                
            }
        }
        
        // Add the range predicate to the end of our lists
        if (!rangeFound) {
            indexOp.add(CompOperatorEnum.COMP_EQ);
        } else {
            indexCols.add(rangeInputRef);
            SargInterval sargInterval = rangeSargSeq.getList().get(0);
            SargEndpoint lowerBound = sargInterval.getLowerBound();
            SargEndpoint upperBound = sargInterval.getUpperBound();
            RexNode rangeOperand = null;
            CompOperatorEnum rangeOp = null;
            if (upperBound.isFinite()) {
                rangeOperand = upperBound.getCoordinate();
                if (upperBound.getStrictness() == SargStrictness.OPEN) {
                    rangeOp = CompOperatorEnum.COMP_LT;
                } else {
                    rangeOp = CompOperatorEnum.COMP_LE;
                }
            } else if (lowerBound.isFinite()) {
                rangeOperand = lowerBound.getCoordinate();
                if (lowerBound.getStrictness() == SargStrictness.OPEN) {
                    rangeOp = CompOperatorEnum.COMP_GT;
                } else {
                    rangeOp = CompOperatorEnum.COMP_GE;
                }
            }
            indexOperands.add(((RexInputRef) rangeOperand).getIndex());
            indexOp.add(rangeOp);
        }
        return
            getExcessPredicates(nonIndexBindingList, rexBuilder, rexAnalyzer);
    }
    
    /**
     * Retrieves the excess predicates by combining an unused sargable binding
     * list with the non-sargable predicates
     * 
     * @param sargBindingList unused sarg binding list
     * @param rexBuilder rex builder
     * @param rexAnalyzer rex analyzer
     * 
     * @return combined predicate
     */
    private RexNode getExcessPredicates(
        List<SargBinding> sargBindingList,
        RexBuilder rexBuilder,
        SargRexAnalyzer rexAnalyzer)
    {        
        // AND together the excess sargable predicates that can't be used
        // along with the non-sargable predicates
        RexNode excessSargPredicates =
            rexAnalyzer.getSargBindingListToRexNode(sargBindingList);
        RexNode nonSargPredicates = rexAnalyzer.getNonSargFilterRexNode();
        if (excessSargPredicates == null) {
            return nonSargPredicates;
        } else {
            return
                RelOptUtil.andJoinFilters(
                    rexBuilder, excessSargPredicates, nonSargPredicates);
        }
    }
    
    /**
     * From the remainining sargable predicates that cannot be used in the
     * index lookup, determine which can be applied in a
     * {@link FennelReshapeRel}
     * 
     * @param sargBindingList remaining sargable predicates
     * @param nLeftFields number of inputs in the LHS input
     * @param filterCols returns the list of column offsets corresponding to
     * RHS input that can be filtered in FennelReshapeRel
     * @param filterOperands returns a list of the column offsets corresponding
     * to LHS input that the filterCols are compared against
     * @param filterOp returns the comparison operator that will be applied in
     * FennelReshapeRel
     * @param castFromTypes stores the type that the LHS input needs to be
     * cast from if its type doesn't match the RHS input it is being compared
     * against
     * @param rexBuilder rex builder
     * @param rexAnalyzer rex analyzer
     * 
     * @return excess predicates that cannot be processed by FennelReshapeRel
     */
    private RexNode getFilterPredicates(
        List<SargBinding> sargBindingList,
        int nLeftFields,
        List<Integer> filterCols,
        List<Integer> filterOperands,
        List<CompOperatorEnum> filterOp,
        RelDataType [] castFromTypes,
        RelDataTypeFactory typeFactory,
        RexBuilder rexBuilder,
        SargRexAnalyzer rexAnalyzer)
    {   
        Map<RexInputRef, SargBinding> rex2SargBindingMap =
            new HashMap<RexInputRef, SargBinding>();
        for (SargBinding sargBinding : sargBindingList) {
            rex2SargBindingMap.put(sargBinding.getInputRef(), sargBinding);
        }

        List<RexInputRef> filterRefs = new ArrayList<RexInputRef>();
        List<RexNode> filterOperandRexs = new ArrayList<RexNode>();
        boolean reshapeable = FennelRelUtil.extractSimplePredicates(
            sargBindingList,
            filterRefs,
            filterOperandRexs,
            filterOp);
        if (!reshapeable) {
            return
                getExcessPredicates(sargBindingList, rexBuilder, rexAnalyzer);           
        }
        
        // Determine if we need to cast the left inputs that will be used
        // in the filters.  If an input needs to be cast to multiple types or
        // the RHS needs to be cast to the type of the LHS rather than the
        // reverse, then leave it up to the Calc node to process that particular
        // filter.
        List<SargBinding> nonFilterBindingList = new ArrayList<SargBinding>();
        ListIterator<RexNode> filterIter = filterOperandRexs.listIterator();
        int loopIdx = 0;
        while (filterIter.hasNext()) {
            RexNode filterOperand = filterIter.next();
            RexInputRef filterRef = filterRefs.get(loopIdx++);
            int filterOpIdx = ((RexInputRef) filterOperand).getIndex();
            assert(filterOperand instanceof RexInputRef);
            if (filterRef.getType() != filterOperand.getType()) {
                RelDataType castType =
                    findCastType(
                        typeFactory,
                        filterOperand.getType(),
                        filterRef.getType());
                if (castType == null ||
                    (castFromTypes[filterOpIdx] != null &&
                        castFromTypes[filterOpIdx] != castType))
                {
                    nonFilterBindingList.add(rex2SargBindingMap.get(filterRef));
                    filterIter.remove();
                    continue;
                }
                castFromTypes[filterOpIdx] = castType;
            }
            filterCols.add(filterRef.getIndex() - nLeftFields);
            filterOperands.add(filterOpIdx);
        }
        
        return
            getExcessPredicates(nonFilterBindingList, rexBuilder, rexAnalyzer);
    }
    
    /**
     * Determines the type that a LHS reference should be cast to in order to
     * compare against a RHS reference.  If the RHS has a more restrictive
     * type than the LHS, then it's not possible to cast the LHS.
     * 
     * @param typeFactory type factory
     * @param leftType type of the LHS reference
     * @param rightType type of the RHS reference
     * 
     * @return type that the LHS should be cast to if casting is feasible
     */
    private RelDataType findCastType(
        RelDataTypeFactory typeFactory,
        RelDataType leftType,
        RelDataType rightType)
    {
        RelDataType castType = typeFactory.leastRestrictive(
            new RelDataType [] { leftType, rightType });
        RelDataType notNullRightType =
            typeFactory.createTypeWithNullability(rightType, true);
        RelDataType notNullCastType =
            typeFactory.createTypeWithNullability(castType, true);
        // Casting is only possible if it's the LHS that needs to be cast,
        // not the RHS
        if (notNullRightType != notNullCastType) {
            return null;
        } else {
            return castType;
        }
    }
        
    /**
     * Creates a projection on top of the left input if the columns from the
     * left input that will participate in filtering do not match the types of
     * the right input columns that they will be compared to.
     * 
     * @param leftRel left input
     * @param castFromTypes the types that the LHS inputs need to be cast to
     * 
     * @return ProjectRel that does the necessary casting if casting is
     * required; otherwise the original left input is returned
     */
    private RelNode castLeftRel(RelNode leftRel, RelDataType[] castFromTypes)
    {
        boolean castRequired = false;
        RelDataTypeField[] leftFields = leftRel.getRowType().getFields();
        for (int i = 0; i < leftRel.getRowType().getFieldCount(); i++) {
            if (castFromTypes[i] == null) {
                castFromTypes[i] = leftFields[i].getType();
            } else {
                castRequired = true;
            }
        }
        // If we do need to cast, create a ProjectRel on top of the existing
        // RelNode
        if (castRequired) {
            return RelOptUtil.createCastRel(
                leftRel,
                leftRel.getCluster().getTypeFactory().createStructType(
                    castFromTypes,
                    RelOptUtil.getFieldNames(leftRel.getRowType())),
                true);
        } else {
            return leftRel;
        }
    }
        
    /**
     * Determines if casting is required for the endpoints in a
     * SargIntervalSequence that contain only a single sequence
     * 
     * @param typeFactory type factory
     * @param castFromTypes array containing cast types
     * @param inputRef the input that the SargIntervalSequence is being
     * compared against
     * @param sargIntervalSequence the SargIntervalSequence
     * @param checkLowerBound true if we only want to check the lower bound
     * endpoint
     * 
     * @return true if the endpoints can be cast; false if they cannot be
     * because an endpoint is already being cast to another type
     */
    private boolean checkSargIntervalTypes(
        RelDataTypeFactory typeFactory,
        RelDataType [] castFromTypes,
        RexInputRef inputRef,
        SargIntervalSequence sargIntervalSequence,
        boolean checkLowerBound)
    {
        List<SargInterval> sargIntvList = sargIntervalSequence.getList();
        assert(sargIntvList.size() == 1);
        SargInterval sargInterval = sargIntvList.get(0);
        if (!checkEndpointType(
            typeFactory,
            castFromTypes,
            inputRef,
            sargInterval.getLowerBound()))
        {
            return false;
        }
        if (checkLowerBound) {
            return true;
        } else {
            return checkEndpointType(
                typeFactory,
                castFromTypes,
                inputRef,
                sargInterval.getUpperBound());  
        }
    }
    
    /**
     * If the endpoint coordinate in a SargInterval is a RexInputRef and its
     * type doesn't match the type of the input it's being compared against,
     * then store the type so we can later cast to that type
     * 
     * @param typeFactory type factory
     * @param castFromTypes array containing the cast types
     * @param inputRef the input that the endpoint is being compared against
     * @param endpoint the endpoint
     * 
     * @return true if the endpoint can be cast to the type of the RexInputRef;
     * false if it cannot be because the endpoint is already being cast to
     * another type or casting isn't possible
     */
    private boolean checkEndpointType(
        RelDataTypeFactory typeFactory,
        RelDataType [] castFromTypes,
        RexInputRef inputRef,
        SargEndpoint endpoint)
    {       
        RexNode coordinate = endpoint.getCoordinate();
        // If the coordinate is either NULL or a RexInputRef.  It cannot be
        // a RexLiteral because the filter originates from a join condition
        // and therefore, any predicates comparing the RHS against a constant
        // should have been pushed out of the join condition
        if (coordinate instanceof RexInputRef) {
            int idx = ((RexInputRef) coordinate).getIndex();
            if (coordinate.getType() != inputRef.getType()) {
                RelDataType castType = 
                    findCastType(
                        typeFactory,
                        coordinate.getType(),
                        inputRef.getType());
                if (castType == null ||
                    (castFromTypes[idx] != null &&
                        castFromTypes[idx] != castType))
                {
                    return false;
                }
                castFromTypes[idx] = castType;
            }       
        }
        return true;
    }
    
    /**
     * Converts an input so its traits include FENNEL_EXEC_CONVENTION
     * 
     * @param origTraits traits of the original input
     * @param inputRel the input
     * 
     * @return a new RelNode with the merged traits
     */
    private RelNode convertInput(RelTraitSet origTraits, RelNode inputRel)
    {
        return
            mergeTraitsAndConvert(
                origTraits,
                FennelRel.FENNEL_EXEC_CONVENTION,
                inputRel);
    }
    
    /**
     * Creates a new dynamic parameter for each new join key encountered
     * 
     * @param implementor FennelRelImplementor
     * @param joinKeys list of join keys for which we want to create dynamic
     * parameters; if a dynamic parameter has already been created for a key,
     * don't create another one
     * @param joinKeyParamMap mapping from join keys to dynamic parameters;
     * used to keep track of which keys already have corresponding dynamic
     * parameters
     */
    private void createJoinKeyParameters(
        FennelRelImplementor implementor,
        List<Integer> joinKeys,
        Map<Integer, FennelRelParamId> joinKeyParamMap)
    {
        for (Integer joinKey : joinKeys) {
            if (!joinKeyParamMap.containsKey(joinKey)) {
                FennelRelParamId paramId = implementor.allocateRelParamId();
                joinKeyParamMap.put(joinKey, paramId);
            }
        }
    }
    
    /**
     * Locates all references to the left input in an expression representing
     * the portion of the join condition that must be processed by a Calc node
     * 
     * @param residualCondition remaining join condition to be processed by
     * Calc node
     * @param nLeftFields number of fields in the left join input
     * @param leftInputRefs list of column offsets corresponding to left
     * input references
     */
    private void findLeftInputRefs(
        RexNode residualCondition,
        int nLeftFields,
        List<Integer> leftInputRefs)
    {
        BitSet inputRefs = new BitSet();
        RelOptUtil.InputFinder inputFinder =
            new RelOptUtil.InputFinder(inputRefs);
        residualCondition.accept(inputFinder);
        
        for (int field = inputRefs.nextSetBit(0); field >= 0;
            field = inputRefs.nextSetBit(field + 1))
        {
            if (field < nLeftFields) {
                leftInputRefs.add(field);
            }
        }
    }
    
    /**
     * Creates the second input into the nested loop join tree that processes
     * the RHS input.  If a temporary index is used in the join lookup,
     * create an index lookup RelNode.  The output from either that (or
     * the RHS input, if no temp index lookup is required) is then optionally
     * fed into a FennelReshapeRel and/or a Calc node for additional join
     * filtering.
     *
     * @param leftRel left input into the new join
     * @param rightRel right input into the new join
     * @param indexCols list of column offsets from the RHS input that
     * are to be indexed
     * @param indexOperands list of column offsets from the LHS input
     * that will be used as index lookup keys
     * @param indexOp the operator to be used in the index lookup
     * @param joinKeyParamMap mapping from LHS inputs to dynamic parameters
     * @param rootPageIdParamId dynamic parameter corresponding to the
     * rootPageId of the temp index
     * @param filterCols list of column offsets from the RHS input that can
     * be applied in FennelReshapeRel
     * @param filterOperands list of the LHS input that the filterCols are
     * compared against
     * @param filterOp comparison operator that will be applied in
     * FennelReshapeRel
     * @param residualRefs LHS inputs that must be processed in the Calc node
     * @param residualCondition remaining filter that must be applied in a
     * Calc node
     *
     * @return RelNode tree corresponding to the nested loop join's second
     * input or null if traits couldn't be converted
     */
    private RelNode createSecondInput(
        RelNode leftRel,
        RelNode rightRel,
        List<Integer> indexCols,
        List<Integer> indexOperands,
        CompOperatorEnum indexOp,
        Map<Integer, FennelRelParamId> joinKeyParamMap,
        FennelRelParamId rootPageIdParamId,
        List<Integer> filterCols,
        List<Integer> filterOperands,
        CompOperatorEnum filterOp,
        List<Integer> residualRefs,
        RexNode residualCondition)       
    {
        RelNode secondInput = null;
        if (indexCols.isEmpty()) {
            // Since we're not using a temporary index, see if it makes
            // sense to buffer the RHS
            FennelBufferRel bufferRel =
                FennelRelUtil.bufferRight(leftRel, rightRel);
            secondInput = (bufferRel != null) ? bufferRel : rightRel;
        } else {
            secondInput =
                createTempIdxLookup(
                    rightRel,
                    indexCols,
                    indexOperands,
                    indexOp,
                    joinKeyParamMap,
                    rootPageIdParamId);
        }
        
        Map<Integer, Integer> residualRefMap = new HashMap<Integer, Integer>();
        if (!filterCols.isEmpty() || !residualRefs.isEmpty()) {
            secondInput =
                createReshapeRel(
                    leftRel,
                    secondInput,
                    joinKeyParamMap,
                    filterCols,
                    filterOperands,
                    filterOp,
                    residualRefs,
                    residualCondition,
                    residualRefMap);
        }
        
        if (residualCondition != null) {
            // No need to worry about outer joins when creating this rowtype
            // because we haven't executed the outer join yet
            RelDataType joinRowType =
                JoinRel.deriveJoinRowType(
                    leftRel.getRowType(),
                    rightRel.getRowType(),
                    JoinRelType.INNER,
                    leftRel.getCluster().getTypeFactory(),
                    null);
            secondInput =
                createResidualFilter(
                    joinRowType.getFields(),
                    leftRel.getRowType().getFieldCount(),
                    secondInput,
                    residualCondition,
                    rightRel,
                    residualRefMap);
            secondInput = convertInput(secondInput.getTraits(), secondInput);
        }
        
        return secondInput;
    }
    
    /**
     * Creates a temporary index lookup RelNode
     *
     * @param rightRel right input into the new join
     * @param indexCols list of column offsets from the RHS input that
     * are to be indexed
     * @param indexOperands list of column offsets from the LHS input
     * that will be used as index lookup keys
     * @param indexOp the operator to be used in the index lookup
     * @param joinKeyParamMap mapping from LHS inputs to dynamic parameters
     * @param rootPageIdParamId dynamic parameter corresponding to the
     * rootPageId of the temp index
     *
     * @return temporary index lookup RelNode
     */
    private FennelTempIdxSearchRel createTempIdxLookup(
        RelNode rightRel,
        List<Integer> indexCols,
        List<Integer> indexOperands,
        CompOperatorEnum indexOp,
        Map<Integer, FennelRelParamId> joinKeyParamMap,
        FennelRelParamId rootPageIdParamId)       
    {
        // Setup the input search parameters
        int keyLen = indexCols.size();
        Integer [] indexKeys = indexCols.toArray(new Integer[keyLen]);       
        Integer [] inputKeyProj = new Integer[keyLen * 2];       
        for (int i = 0; i < keyLen; i++) {
            inputKeyProj[i] = i + 1;
            inputKeyProj[i + keyLen] = keyLen + 2 + i;
            
        }
        Integer [] inputDirectiveProj = new Integer[2];
        inputDirectiveProj[0] = 0;
        inputDirectiveProj[1] = keyLen + 1;
        
        // Setup the search key parameters.  The number depends on the
        // type of lookup.  Note that even though the same parameter is 
        // used as the lower and upper bounds of an equality lookup, we
        // create a separate search parameter for each, referencing the
        // same dynamic parameter.  In the examples referenced in the
        // comments below, assume we're comparing against parameters X
        // and Y.
        int nSearchParams;
        if (indexOp == CompOperatorEnum.COMP_EQ) {
            nSearchParams = keyLen * 2;
        } else {
            nSearchParams = keyLen * 2 - 1;
        }
        FennelRelParamId [] searchKeyParamIds =
            new FennelRelParamId[nSearchParams];
        Integer [] keyOffsets = new Integer[nSearchParams];
        FennelSearchEndpoint lowerDirective;
        FennelSearchEndpoint upperDirective;
        if (indexOp == CompOperatorEnum.COMP_EQ) {
            // Search parameters are (X, Y, X, Y)
            lowerDirective = FennelSearchEndpoint.SEARCH_CLOSED_LOWER;
            upperDirective = FennelSearchEndpoint.SEARCH_CLOSED_UPPER;
            for (int i = 0; i < keyLen; i++) {
                searchKeyParamIds[i] =
                    joinKeyParamMap.get(indexOperands.get(i));
                keyOffsets[i] = i;
                searchKeyParamIds[i + keyLen] =
                    joinKeyParamMap.get(indexOperands.get(i));       
                keyOffsets[i + keyLen] = keyLen + i;
            }
        } else if (indexOp == CompOperatorEnum.COMP_LE ||
            indexOp == CompOperatorEnum.COMP_LT)
        {
            // Set the lower bound directive to skip past nulls.
            // Search parameters are (X, -, X, Y)
            lowerDirective = FennelSearchEndpoint.SEARCH_OPEN_LOWER;
            upperDirective =
                (indexOp == CompOperatorEnum.COMP_LE) ?
                    FennelSearchEndpoint.SEARCH_CLOSED_UPPER :
                    FennelSearchEndpoint.SEARCH_OPEN_UPPER;
            for (int i = 0; i < keyLen - 1; i++) {
                searchKeyParamIds[i] =
                    joinKeyParamMap.get(indexOperands.get(i));
                keyOffsets[i] = i;
                searchKeyParamIds[i + keyLen - 1] =
                    joinKeyParamMap.get(indexOperands.get(i));
                keyOffsets[i + keyLen - 1] = keyLen + i;
            }
            searchKeyParamIds[(keyLen - 1) * 2] =
                joinKeyParamMap.get(indexOperands.get(keyLen - 1));
            keyOffsets[(keyLen - 1) * 2] = (keyLen * 2) - 1;
        } else {
            // Search parameters are (X, Y, X, -)
            upperDirective = FennelSearchEndpoint.SEARCH_UNBOUNDED_UPPER;
            lowerDirective =
                (indexOp == CompOperatorEnum.COMP_GE) ?
                    FennelSearchEndpoint.SEARCH_CLOSED_LOWER :
                    FennelSearchEndpoint.SEARCH_OPEN_LOWER;
            for (int i = 0; i < keyLen - 1; i++) {
                searchKeyParamIds[i] =
                    joinKeyParamMap.get(indexOperands.get(i));
                keyOffsets[i] = i;
                searchKeyParamIds[i + keyLen] =
                    joinKeyParamMap.get(indexOperands.get(i));
                keyOffsets[i + keyLen] = i + keyLen;
            }
            searchKeyParamIds[keyLen - 1] =
                joinKeyParamMap.get(indexOperands.get(keyLen - 1));
            keyOffsets[keyLen - 1] = keyLen - 1;
        }
        
        // Setup the ValuesRel that provides the index directives and
        // index key types.  Make the types of the search keys nullable
        // because we're passing in null key values as placeholders for
        // the dynamic parameters.
        RelDataType [] types = new RelDataType[keyLen * 2 + 2];
        String [] fieldNames = new String[keyLen * 2 + 2];
        RelDataTypeFactory typeFactory =
            rightRel.getCluster().getTypeFactory();
        RelDataType directiveType =
            typeFactory.createSqlType(
                SqlTypeName.CHAR,
                1);
        types[0] = directiveType;
        types[keyLen + 1] = directiveType;
        fieldNames[0] = "lowerDirective";
        fieldNames[keyLen + 1] = "upperDirective";
        RelDataTypeField [] fields = rightRel.getRowType().getFields();
        for (int i = 0; i < keyLen; i++) {
            RelDataTypeField field = fields[indexKeys[i]];
            RelDataType nullableType =
                typeFactory.createTypeWithNullability( 
                    field.getType(),
                    true);
            types[i + 1] = nullableType;
            types[i + 2 + keyLen] = nullableType;
            fieldNames[i + 1] = field.getName();
            fieldNames[i + 2 + keyLen] = field.getName();
        }
        RelDataType keyRowType =
            typeFactory.createStructType(types, fieldNames);
        
        List<List<RexLiteral>> inputTuples = new ArrayList<List<RexLiteral>>();
        List<RexLiteral> inputTuple = new ArrayList<RexLiteral>();
        RexBuilder rexBuilder = rightRel.getCluster().getRexBuilder();
        RexLiteral nullLiteral = rexBuilder.constantNull();
        inputTuple.add(rexBuilder.makeLiteral(lowerDirective.getSymbol()));
        for (int i = 0; i < keyLen; i++) {
            inputTuple.add(nullLiteral);
        }
        inputTuple.add(rexBuilder.makeLiteral(upperDirective.getSymbol()));
        for (int i = 0; i < keyLen; i++) {
            inputTuple.add(nullLiteral);
        }
        inputTuples.add(inputTuple);        
        RelNode searchInput =
            new FennelValuesRel(
                rightRel.getCluster(),
                keyRowType,
                (List) inputTuples);
        
        // Finally, create the temp index search RelNode
        return
            new FennelTempIdxSearchRel(
                rightRel,
                indexKeys,
                searchInput,
                inputKeyProj,
                inputDirectiveProj,
                searchKeyParamIds,
                keyOffsets,
                rootPageIdParamId);
    }
    
    /**
     * Creates a FennelReshapeRel for applying simple join filters that can't
     * be applied as part of the temp index lookup
     * 
     * @param leftRel left join input
     * @param reshapeInput input into FennelReshapeRel
     * @param joinKeyParamMap mapping from LHS inputs to dynamic parameters
     * @param filterCols list of column offsets from the RHS input that can
     * be applied in FennelReshapeRel
     * @param filterOperands list of the LHS input that the filterCols are
     * compared against
     * @param filterOp comparison operator that will be applied in
     * FennelReshapeRel
     * @param residualRefs LHS inputs that must be processed in Calc node
     * @param residualCondition remaining filter that must be applied in a
     * Calc node
     * @param residualRefMap map used to keep track of where each residual
     * reference will appear in the output for the ReshapeRel; the map is
     * populated within this method
     * 
     * @return FennelReshapeRel
     */
    private FennelReshapeRel createReshapeRel(
        RelNode leftRel,
        RelNode reshapeInput,
        Map<Integer, FennelRelParamId> joinKeyParamMap,
        List<Integer> filterCols,
        List<Integer> filterOperands,
        CompOperatorEnum filterOp,
        List<Integer> residualRefs,
        RexNode residualCondition,
        Map<Integer, Integer> residualRefMap)
    {     
        // We aren't comparing to any literals, just dynamic parameters
        List<RexLiteral> filterLiterals = new ArrayList<RexLiteral>();
 
        // All fields from the index lookup need to be projected
        RelDataTypeField [] reshapeInputFields =
            reshapeInput.getRowType().getFields();
        int nReshapeInputFields = reshapeInputFields.length;
        Integer [] projection =
            FennelRelUtil.newIotaProjection(nReshapeInputFields);
        
        // The output consists of the input plus references from the LHS that
        // need to be filtered downstream
        RelDataType [] types =
            new RelDataType[nReshapeInputFields + residualRefs.size()];
        String [] fieldNames =
            new String[nReshapeInputFields + residualRefs.size()];   
        for (int i = 0; i < nReshapeInputFields; i++) {
            types[i] = reshapeInputFields[i].getType();
            fieldNames[i] = reshapeInputFields[i].getName();
        }
        RelDataTypeField [] leftFields = leftRel.getRowType().getFields();
        
        // The dynamic parameters that will be read by the ReshapeRel
        // include the ones that need to be filtered in the ReshapeRel plus
        // any additional ones that also need to be outputted for additional
        // filtering downstream       
        int currOutputIdx = nReshapeInputFields;
        List<FennelRelParamId> dynamicParamIds =
            new ArrayList<FennelRelParamId>();
        BitSet paramOutput = new BitSet();
        for (Integer filterOperand : filterOperands) {
            dynamicParamIds.add(joinKeyParamMap.get(filterOperand));
            if (residualRefs.contains(filterOperand)) {
                // This input will be filtered in this ReshapeRel and then
                // again downstream in a Calc node
                paramOutput.set(dynamicParamIds.size() - 1);
                residualRefMap.put(filterOperand, currOutputIdx);
                types[currOutputIdx] = leftFields[filterOperand].getType();
                fieldNames[currOutputIdx++] =
                    leftFields[filterOperand].getName();
            }
        }

        // Go through the residualRefs and find the ones that aren't being
        // filtered by ReshapeRel since we've already processed them above
        int nFilterParams = filterOperands.size();
        int nDynamicParams = nFilterParams;
        for (Integer residualRef : residualRefs) {
            if (!filterOperands.contains(residualRef)) {
                paramOutput.set(nDynamicParams++);
                dynamicParamIds.add(joinKeyParamMap.get(residualRef));
                residualRefMap.put(residualRef, currOutputIdx);
                types[currOutputIdx] = leftFields[residualRef].getType();
                fieldNames[currOutputIdx++] =
                    leftFields[residualRef].getName();
            }
        }
        
        Integer [] paramCompareOffsets = new Integer[nDynamicParams];
        for (int i = 0; i < nDynamicParams; i++) {
            if (i < nFilterParams) {
                paramCompareOffsets[i] = filterCols.get(i);
            } else {
                paramCompareOffsets[i] = -1;
            }
        }
        
        RelDataType outputRowType =
            leftRel.getCluster().getTypeFactory().createStructType(
                types,
                fieldNames);
        
        return
            new FennelReshapeRel(
                reshapeInput.getCluster(),
                reshapeInput,
                projection,
                outputRowType,
                filterOp,
                filterCols.toArray(new Integer[filterCols.size()]),
                filterLiterals,
                dynamicParamIds.toArray(
                    new FennelRelParamId[dynamicParamIds.size()]),
                paramCompareOffsets,
                paramOutput);
    }
    
    /**
     * Creates RelNodes to apply remaining filters
     * 
     * @param joinFields fields from join of the left and right inputs
     * @param nLeftFields number of fields in the left input
     * @param filterInput input to be filtered
     * @param residualCondition filter to be applied
     * @param rightRel right join input
     * @param residualRefMap map used to keep track of where each residual
     * reference will appear in the output for the ReshapeRel
     * 
     * @return filtering RelNodes
     */
    private RelNode createResidualFilter(
        RelDataTypeField [] joinFields,
        int nLeftFields,
        RelNode filterInput,
        RexNode residualCondition,
        RelNode rightRel,
        Map<Integer, Integer> residualRefMap)
    {
        // Convert the residualCondition so that the LHS references are
        // replaced with the position of their corresponding dynamic parameter
        // in the input passed into the Calc node.  Also shift RHS references
        // to the left.
        int [] adjustments = new int[joinFields.length];
        for (Integer residualRef : residualRefMap.keySet()) {
            adjustments[residualRef] =
                residualRefMap.get(residualRef) - residualRef;
        }
        for (int i = 0; i < rightRel.getRowType().getFieldCount(); i++) {
            adjustments[nLeftFields + i] -= nLeftFields;
        }
        RexBuilder rexBuilder = filterInput.getCluster().getRexBuilder();
        RexNode newCondition =
            residualCondition.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    joinFields,
                    adjustments));
        
        RelNode filterRel =
            new FilterRel(
                filterInput.getCluster(),
                filterInput,
                newCondition);

        // Project out the extra input parameters that are only needed
        // because of residual filtering.  The extra input is always
        // appended at the end of the input tuple.
        int nRightFields = rightRel.getRowType().getFieldCount();
        RelDataTypeField [] inputFields = filterInput.getRowType().getFields();
        RexNode [] projExprs = new RexNode[nRightFields];
        String [] fieldNames = new String[nRightFields];
        for (int i = 0; i < projExprs.length; i++) {
            projExprs[i] = rexBuilder.makeInputRef(inputFields[i].getType(), i);
            fieldNames[i] = inputFields[i].getName();
        }
        
        return
            CalcRel.createProject(
                filterRel,
                projExprs,
                fieldNames);
    }
    
    /**
     * Creates the third input into the nested loop join, which builds the
     * temporary index on the right join input
     * 
     * @param rightRel right join input
     * @param indexCols columns to be indexed
     * @param rootPageIdParamId dynamic parameter corresponding to the root
     * of the temporary index
     * 
     * @return RelNodes corresponding to the third input
     */
    private RelNode createThirdInput(
        RelNode rightRel,
        List<Integer> indexCols,
        FennelRelParamId rootPageIdParamId)
    {
        // Sort the data on the index keys so we can use monotonic inserts
        // to build the temp index
        Integer [] indexColsArray =
            (Integer []) indexCols.toArray(new Integer[indexCols.size()]);
        FennelSortRel sortRel =
            new FennelSortRel(
                rightRel.getCluster(),
                rightRel,
                indexColsArray,
                false);
        
        return new FennelIdxWriteRel(
            sortRel,
            false,
            true,
            rootPageIdParamId,
            indexColsArray);  
    }
}

// End FennelNestedLoopJoinRule.java
