package com.disruptivetech.farrago.rel;

import net.sf.farrago.query.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.catalog.FarragoRepos;
import org.eigenbase.relopt.*;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.CollectRel;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.CorrelatorRel;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeName;

import java.util.ArrayList;

/**
 * FennelPullCorrelatorRel is the relational expression corresponding to a
 * correlation between two streams implemented inside of Fennel.
 *
 * @author Wael Chatila
 * @since Feb 1, 2005
 * @version $Id$
 */
public class FennelPullCorrelatorRel extends FennelPullDoubleRel
{
    //~ Instance fields -------------------------------------------------------

    protected ArrayList correlations;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a Correlator.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param left left input relational expression
     * @param right right  input relational expression
     * @param correlations set of expressions to set as variables each time a
     *        row arrives from the left input
     */
    public FennelPullCorrelatorRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        ArrayList correlations)
    {
        super(cluster, left, right);
        this.correlations = correlations;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Cloneable
    public Object clone()
    {
        return new FennelPullCorrelatorRel(
            cluster,
            RelOptUtil.clone(left),
            RelOptUtil.clone(right),
            (ArrayList) correlations.clone());
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "left", "right" },
            new Object [] {  });
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double rowCount = getRows();
        return planner.makeCost(rowCount, 0,
            rowCount * getRowType().getFieldList().size());
    }

    // implement RelNode
    public double getRows()
    {
        return left.getRows() * right.getRows();
    }

    protected RelDataType deriveRowType()
    {
        return JoinRel.deriveJoinRowType(left, right, JoinRel.JoinType.LEFT, cluster.typeFactory);
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemCorrelationJoinStreamDef streamDef =
            repos.newFemCorrelationJoinStreamDef();

        for (int i = 0; i < correlations.size(); i++) {
            CorrelatorRel.Correleation correlation =
                (CorrelatorRel.Correleation) correlations.get(i);
            FemCorrelation newFemCorrelation = repos.newFemCorrelation();
            newFemCorrelation.setId(correlation.id);
            newFemCorrelation.setOffset(correlation.offset);
            streamDef.getCorrelations().add(newFemCorrelation);
        }

        FemExecutionStreamDef leftInput =
            implementor.visitFennelChild((FennelRel) left);
        streamDef.getInput().add(leftInput);
        FemExecutionStreamDef rightInput =
            implementor.visitFennelChild((FennelRel) right);
        streamDef.getInput().add(rightInput);

        return streamDef;
    }
}


// End FennelPullCorrelatorRel.java
