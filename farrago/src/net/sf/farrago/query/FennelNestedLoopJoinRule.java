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
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
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

        // Find the subset of predicates that will be used either with
        // the temporary index or a reshapeRel
        List<Integer> indexCols = new ArrayList<Integer>();
        List<Integer> indexOperands = new ArrayList<Integer>();
        List<CompOperatorEnum> indexOpList = new ArrayList<CompOperatorEnum>();
        List<Integer> filterCol = new ArrayList<Integer>(); 
        List<Integer> filterOperand = new ArrayList<Integer>();
        List<CompOperatorEnum> filterOpList = new ArrayList<CompOperatorEnum>();
        List<Integer> outputProj = new ArrayList<Integer>();
        RelNode [] joinInputs = new RelNode [] { leftRel, rightRel };
        RexNode residualCondition =
            getPredicates(
                joinInputs,
                condition,
                indexCols,
                indexOperands,
                indexOpList,
                filterCol,
                filterOperand,
                filterOpList,
                outputProj);
        
        // Create the nested loop join rel with additional projections as
        // needed due to casting and/or swapping
        CompOperatorEnum indexOp;
        if (indexCols.isEmpty()) {
            indexOp = CompOperatorEnum.COMP_NOOP;
        } else {
            indexOp = indexOpList.get(0);
        }
        CompOperatorEnum filterOp;
        if (filterCol.isEmpty()) {
            filterOp = CompOperatorEnum.COMP_NOOP;
        } else {
            filterOp = filterOpList.get(0);
        }
        RelNode nestedLoopRel =
            createNestedLoopRel(
                joinRel,
                joinInputs[0],
                joinInputs[1],
                swapped,
                outputProj,
                joinType,
                indexCols,
                indexOperands,
                indexOp,
                filterCol,
                filterOperand,
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
     * @param outputProj required projection of join result (post-swap, if
     * inputs have been swapped)
     * @param joinType join type
     * @param indexCols list of column offsets from the RHS input that
     * are to be indexed
     * @param indexOperands list of column offsets from the LHS input
     * that will be used as index lookup keys
     * @param indexOp the operator to be used in the index lookup
     * @param filterCol the column offset from the RHS input that can
     * be applied in FennelReshapeRel
     * @param filterOperand the LHS input that filterCol is compared against
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
        List<Integer> outputProj,
        JoinRelType joinType,
        List<Integer> indexCols,
        List<Integer> indexOperands,
        CompOperatorEnum indexOp,
        List<Integer> filterCol,
        List<Integer> filterOperand,
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
        createJoinKeyParameters(implementor, filterOperand, joinKeyParamMap);
        
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
                filterCol,
                filterOperand,
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
        
        // If additional projections were added, create a projection on top
        // of the nested loop join to project out the original columns
        RelNode finalRel =
            RelOptUtil.createProjectJoinRel(outputProj, nestedLoopRel);
        
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
     * Locates from the join condition all equi-join predicates and a single
     * range predicate for use with a temporary index and then if possible,
     * an additional range predicate for a FennelReshapeRel
     * 
     * @param joinInputs join inputs
     * @param joinCondition the join condition
     * @param indexCols returns list of column offsets from the right
     * join input that are to be indexed
     * @param indexOperands returns list of column offsets from the left
     * join input that will be used as index lookup keys
     * @param indexOp returns the operator to be used in the index lookup
     * @param filterCol returns the column offset corresponding to
     * RHS input that can be filtered in FennelReshapeRel
     * @param filterOperand returns the column offset corresponding
     * to LHS input that filterCol is compared against
     * @param filterOp returns the comparison operator that will be applied in
     * FennelReshapeRel
     * 
     * @return excess predicates that cannot be processed by the index or
     * FennelReshapeRel
     */
    private RexNode getPredicates(
        RelNode[] joinInputs,
        RexNode joinCondition,
        List<Integer> indexCols,
        List<Integer> indexOperands,
        List<CompOperatorEnum> indexOp,
        List<Integer> filterCol,
        List<Integer> filterOperand,
        List<CompOperatorEnum> filterOp,
        List<Integer> outputProj)
    {
        // First find filters that can be used with the temp index
        List<RexNode> leftIndexKeys = new ArrayList<RexNode>();
        List<RexNode> rightIndexKeys = new ArrayList<RexNode>();
        List<SqlOperator> indexOpList = new ArrayList<SqlOperator>();
        RexNode nonIndexablePreds =
            RelOptUtil.splitJoinCondition(
                joinInputs[0],
                joinInputs[1],
                joinCondition,
                leftIndexKeys,
                rightIndexKeys,
                null,
                indexOpList);
        
        // If no filters can be used with the temp index, don't bother trying
        // to look for filters to use with FennelReshapeRel
        if (leftIndexKeys.isEmpty()) {
            return nonIndexablePreds;
        }       
        mapSqlOpToCompOp(indexOpList, indexOp);
        
        // Next, find the filters that can be used with a FennelReshapeRel
        List<RexNode> leftFilterKeys = new ArrayList<RexNode>();
        List<RexNode> rightFilterKeys = new ArrayList<RexNode>();
        List<SqlOperator> filterOpList = new ArrayList<SqlOperator>();
        RexNode extraPreds = null;
        if (nonIndexablePreds != null) {
            extraPreds =
                RelOptUtil.splitJoinCondition(
                    joinInputs[0],
                    joinInputs[1],
                    nonIndexablePreds,
                    leftFilterKeys,
                    rightFilterKeys,
                    null,
                    filterOpList);
            if (!leftFilterKeys.isEmpty()) {
                mapSqlOpToCompOp(filterOpList, filterOp);
            }
        }

        // Combine the keys for indexing and reshape and create the
        // projections required to produce the join keys that were located
        // by the two calls to splitJoinCondition
        List<RexNode> leftJoinKeys = new ArrayList<RexNode>();
        List<RexNode> rightJoinKeys = new ArrayList<RexNode>();
        List<Integer> cols = new ArrayList<Integer>();
        List<Integer> operands = new ArrayList<Integer>();
        leftJoinKeys.addAll(leftIndexKeys);
        leftJoinKeys.addAll(leftFilterKeys);
        rightJoinKeys.addAll(rightIndexKeys);
        rightJoinKeys.addAll(rightFilterKeys);
        RelNode [] inputRels = { joinInputs[0], joinInputs[1] };
        RelOptUtil.projectJoinInputs(
            inputRels,
            leftJoinKeys,
            rightJoinKeys,
            0,
            operands,
            cols,
            outputProj);

        // Now that we have the new key offsets, assign them respectively
        // to the index and filter return parameters
        int keyIdx;
        for (keyIdx = 0; keyIdx < leftIndexKeys.size(); keyIdx++) {
            indexCols.add(cols.get(keyIdx));
            indexOperands.add(operands.get(keyIdx));
        }
        assert(leftFilterKeys.size() <= 1);
        if (leftFilterKeys.size() == 1) {
            filterCol.add(cols.get(keyIdx));
            filterOperand.add(operands.get(keyIdx));
        }
        
        // Adjust references to the right join input in the remaining
        // filter if additional columns were added to the left join input
        if (extraPreds != null) {
            int nLeftFieldsOrig = joinInputs[0].getRowType().getFieldCount();
            int nLeftFieldsNew = inputRels[0].getRowType().getFieldCount();
            int adjustment = nLeftFieldsNew - nLeftFieldsOrig;
            if (adjustment > 0) {
                int nTotalFields =
                    nLeftFieldsOrig + joinInputs[1].getRowType().getFieldCount();
                int [] adjustments = new int[nTotalFields];
                for (int i = nLeftFieldsOrig; i < nTotalFields; i++) {
                    adjustments[i] = adjustment;
                }
                RelDataType joinRowType =
                    JoinRelBase.createJoinType(
                        joinInputs[0].getCluster().getTypeFactory(),
                        joinInputs[0].getRowType(),
                        joinInputs[1].getRowType(),
                        null);
                extraPreds =
                    extraPreds.accept(
                        new RelOptUtil.RexInputConverter(
                            joinInputs[0].getCluster().getRexBuilder(),
                            joinRowType.getFields(),
                            null,
                            adjustments));
            }
        }
        
        // Pass back the new join inputs
        joinInputs[0] = inputRels[0];
        joinInputs[1] = inputRels[1];
        
        return extraPreds;
    }
   
    private void mapSqlOpToCompOp(
        List<SqlOperator> sqlOpList,
        List<CompOperatorEnum> compOpList)
    {
        // If no SQL operator, default to equality.  Note that when mapping,
        // we reverse the operator because the LHS is the operand in the index
        // lookup and the RHS is the column
        if (sqlOpList.isEmpty()) {
            compOpList.add(CompOperatorEnum.COMP_EQ);
        } else {
            assert(sqlOpList.size() == 1);
            SqlOperator sqlOp = sqlOpList.get(0);
            if (sqlOp == SqlStdOperatorTable.greaterThanOperator) {
                compOpList.add(CompOperatorEnum.COMP_LT);
            } else if (sqlOp ==
                SqlStdOperatorTable.greaterThanOrEqualOperator)
            {
                compOpList.add(CompOperatorEnum.COMP_LE);
            } else if (sqlOp == SqlStdOperatorTable.lessThanOperator) {
                compOpList.add(CompOperatorEnum.COMP_GT);
            } else {
                compOpList.add(CompOperatorEnum.COMP_GE);
            }
        }
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
     * @param filterCol the column offset from the RHS input that can
     * be applied in FennelReshapeRel
     * @param filterOperand the LHS input that filterCol is compared against
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
        List<Integer> filterCol,
        List<Integer> filterOperand,
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
        if (!filterCol.isEmpty() || !residualRefs.isEmpty()) {
            secondInput =
                createReshapeRel(
                    leftRel,
                    secondInput,
                    joinKeyParamMap,
                    filterCol,
                    filterOperand,
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
     * @param filterCols the column offset from the RHS input that can
     * be applied in FennelReshapeRel
     * @param filterOperands the LHS input that filterCol is compared against
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
