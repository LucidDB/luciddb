package net.sf.farrago.query;

import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.OneRowRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.oj.rel.IterOneRowRel;
import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.farrago.fem.fennel.FemMockTupleStreamDef;
import net.sf.farrago.fem.fennel.FemTupleDescriptor;
import net.sf.farrago.catalog.FarragoRepos;
import openjava.mop.OJClass;
import openjava.ptree.*;

import java.util.Collections;

/**
 * <code>OneRowRel</code> always returns one row, one column (containing
 * the value 0).
 *
 * @author Wael Chatila
 * @since Feb 4, 2005
 * @version $Id$
 */
public class FennelPullOneRowRel extends AbstractRelNode implements FennelPullRel
{
    public FennelPullOneRowRel(RelOptCluster cluster)
    {
        super(cluster);
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public Object clone()
    {
        return new FennelPullOneRowRel(cluster);
    }

    protected RelDataType deriveRowType()
    {
        return OneRowRel.deriveOneRowType(cluster.typeFactory);
    }

    public CallingConvention getConvention()
    {
        return FENNEL_PULL_CONVENTION;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemMockTupleStreamDef streamDef =
            repos.newFemMockTupleStreamDef();

        streamDef.setRowCount(1);
        return streamDef;
    }

    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    public RelFieldCollation[] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }
}
