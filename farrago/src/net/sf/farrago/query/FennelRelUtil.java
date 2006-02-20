/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2003-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.util.*;
import net.sf.farrago.session.*;
import net.sf.farrago.*;

import org.eigenbase.sql.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.rex.*;
import org.eigenbase.sarg.*;

/**
 * Static utilities for FennelRel implementations.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FennelRelUtil
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Generates a FemTupleAccessor from a FemTupleDescriptor.
     *
     * @param repos repos for storing transient objects
     * @param fennelDbHandle handle to Fennel database being accessed
     * @param tupleDesc source FemTupleDescriptor
     *
     * @return FemTupleAccessor for accessing tuples conforming to tupleDesc
     */
    public static FemTupleAccessor getAccessorForTupleDescriptor(
        FarragoMetadataFactory repos,
        FennelDbHandle fennelDbHandle,
        FemTupleDescriptor tupleDesc)
    {
        if (fennelDbHandle == null) {
            String tupleAccessorXmiString = "<xmiAccessor/>";
            return tupleDescriptorToAccessor(repos, tupleDesc);
        }
        String tupleAccessorXmiString =
            fennelDbHandle.getAccessorXmiForTupleDescriptorTraced(tupleDesc);
        // TODO: Move FarragoRepos.getTransientFarragoPackage up to the base
        //   class, FarragoMetadataFactory (which is generated, by the way).
        FarragoPackage transientFarragoPackage;
        if (repos instanceof FarragoRepos) {
            FarragoRepos farragoRepos = (FarragoRepos) repos;
            transientFarragoPackage = farragoRepos.getTransientFarragoPackage();
        } else {
            transientFarragoPackage = repos.getRootPackage();
        }
        Collection c =
            JmiUtil.importFromXmiString(
                transientFarragoPackage,
                tupleAccessorXmiString);
        assert (c.size() == 1);
        FemTupleAccessor accessor = (FemTupleAccessor) c.iterator().next();
        return accessor;
    }

    /**
     * Converts a {@link FemTupleDescriptor} into a {@link FemTupleAccessor}
     * without invoking native methods.
     */
    private static FemTupleAccessor tupleDescriptorToAccessor(
        FarragoMetadataFactory repos,
        FemTupleDescriptor tupleDesc)
    {
        FemTupleAccessor tupleAccessor = repos.newFemTupleAccessor();
        tupleAccessor.setMinByteLength(-1);
        tupleAccessor.setBitFieldOffset(-1);
        java.util.List attrDescriptors = tupleDesc.getAttrDescriptor();
        for (int i = 0; i < attrDescriptors.size(); i++) {
            FemTupleAttrDescriptor attrDescriptor =
                (FemTupleAttrDescriptor) attrDescriptors.get(i);
            FemTupleAttrAccessor attrAccessor =
                repos.newFemTupleAttrAccessor();
            attrAccessor.setNullBitIndex(-1);
            attrAccessor.setFixedOffset(-1);
            attrAccessor.setEndIndirectOffset(-1);
            attrAccessor.setBitValueIndex(-1);
            tupleAccessor.getAttrAccessor().add(attrAccessor);
        }
        return tupleAccessor;
    }

    /**
     * Creates a FemTupleDescriptor for a RelDataType which is a row.
     *
     * @param repos repos storing object definitions
     * @param rowType row type descriptor
     *
     * @return generated tuple descriptor
     */
    public static FemTupleDescriptor createTupleDescriptorFromRowType(
        FarragoMetadataFactory repos,
        RelDataTypeFactory typeFactory,
        RelDataType rowType)
    {
        rowType = SqlTypeUtil.flattenRecordType(
            typeFactory,
            rowType,
            null);
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        RelDataTypeField [] fields = rowType.getFields();
        for (int i = 0; i < fields.length; ++i) {
            addTupleAttrDescriptor(repos, tupleDesc, fields[i].getType());
        }
        return tupleDesc;
    }

    /**
     * Creates a FemTupleDescriptor from RexNode's which is a row.
     *
     * @param repos repos storing object definitions
     * @param nodes RexNode's
     *
     * @return generated tuple descriptor
     */
    public static FemTupleDescriptor createTupleDescriptorFromRexNode
            (FarragoMetadataFactory repos, RexNode[] nodes)
    {
        FemTupleDescriptor tupleDesc = repos.newFemTupleDescriptor();
        for (int i = 0; i < nodes.length; i++) {
            addTupleAttrDescriptor(repos, tupleDesc, nodes[i].getType());
        }
        return tupleDesc;
    }

    /**
     * Generates a FemTupleProjection from an array of Integers.
     *
     * @param repos the repos for storing transient objects
     * @param projection the projection to generate
     *
     * @return generated FemTupleProjection
     */
    public static FemTupleProjection createTupleProjection(
        FarragoMetadataFactory repos,
        Integer [] projection)
    {
        return createTupleProjection(repos, Arrays.asList(projection));
    }

    /**
     * Generates a FemTupleProjection from a list of Integers.
     *
     * @param repos the repos for storing transient objects
     * @param projection the projection to generate
     *
     * @return generated FemTupleProjection
     */
    public static FemTupleProjection createTupleProjection(
        FarragoMetadataFactory repos,
        List<Integer> projection)
    {
        FemTupleProjection tupleProj = repos.newFemTupleProjection();

        for (Integer p : projection) {
            FemTupleAttrProjection attrProj =
                repos.newFemTupleAttrProjection();
            tupleProj.getAttrProjection().add(attrProj);
            attrProj.setAttributeIndex(p);
        }
        return tupleProj;
    }

    /**
     * Generates a projection of attribute indices in sequence from 0 to n-1.
     *
     * @param n length of array to generate
     *
     * @return generated array
     */
    public static Integer [] newIotaProjection(int n)
    {
        Integer [] array = new Integer[n];
        for (int i = 0; i < n; ++i) {
            array[i] = new Integer(i);
        }
        return array;
    }

    /**
     * Generates a projection of attribute indices in sequence from
     * (base) to (base + n-1).
     *
     * @param n length of array to generate
     *
     * @param base first value to generate
     *
     * @return generated array
     */
    public static Integer [] newBiasedIotaProjection(
        int n,
        int base)
    {
        Integer [] array = new Integer[n];
        for (int i = 0; i < n; ++i) {
            array[i] = new Integer(base + i);
        }
        return array;
    }

    public static void addTupleAttrDescriptor(
        FarragoMetadataFactory repos,
        FemTupleDescriptor tupleDesc,
        RelDataType type)
    {
        FemTupleAttrDescriptor attrDesc = repos.newFemTupleAttrDescriptor();
        tupleDesc.getAttrDescriptor().add(attrDesc);
        final FennelStandardTypeDescriptor fennelType =
            convertSqlTypeNameToFennelType(type.getSqlTypeName());
        attrDesc.setTypeOrdinal(fennelType.getOrdinal());
        int byteLength = SqlTypeUtil.getMaxByteSize(type);
        attrDesc.setByteLength(byteLength);
        attrDesc.setNullable(type.isNullable());
    }

    /**
     * Creates a FennelTupleDescriptor for a RelDataType which is a row.
     *
     * @param rowType row type descriptor
     *
     * @return generated tuple descriptor
     */
    public static FennelTupleDescriptor convertRowTypeToFennelTupleDesc(
        RelDataType rowType)
    {
        FennelTupleDescriptor tupleDesc = new FennelTupleDescriptor();
        for (RelDataTypeField field : rowType.getFields()) {
            RelDataType type = field.getType();
            FennelTupleAttributeDescriptor attrDesc =
                new FennelTupleAttributeDescriptor(
                    FennelRelUtil.convertSqlTypeNameToFennelType(
                        type.getSqlTypeName()),
                    type.isNullable(),
                    SqlTypeUtil.getMaxByteSize(type));
            tupleDesc.add(attrDesc);
        }
        return tupleDesc;
    }

    /**
     * Converts a SQL type to a Fennel type.
     *
     * <p>The mapping is as follows:
     *
     * <table border="1">
     *
     * <tr>
     * <th>SQL type</th>
     * <th>Fennel type</th>
     * <th>Comments</th>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Boolean}</td>
     * <td>{@link FennelStandardTypeDescriptor#BOOL BOOL}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Tinyint}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_8 INT_8}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Smallint}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_16 INT_16}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Integer}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_32 INT_32}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Decimal}(precision, scale)</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>
     *     <p>We plan to use a shifted representation. For example, the
     *     <code>DECIMAL(6, 2)</code> value 1234.5 would be represented as
     *     an {@link FennelStandardTypeDescriptor#INT_32 INT_32} value 123450
     *     (which is 1234.5 * 10 ^ 2)</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Date}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Milliseconds since the epoch.</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Time}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Milliseconds since midnight.</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Timestamp}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Milliseconds since the epoch.</td>
     * </tr>
     *
     * <tr>
     * <td>Timestamp with timezone</td>
     * <td>&nbsp;</td>
     * <td>Not implemented. We will probably use a user-defined type consisting
     *     of a TIMESTAMP and a VARCHAR.</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#IntervalDayTime}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Not implemented.
     *
     *     <p>All types of day-time interval are represented in the same way:
     *     an integer milliseconds value. For example,
     *     <code>INTERVAL '1' HOUR</code> and
     *     <code>INTERVAL '3600' MINUTE</code> are both represented as
     *     3,600,000.
     *
     *     <p>TBD: How to represent fractions of seconds smaller than a
     *     millisecond, for example, <code>INTERVAL SECOND(6)</code>.</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#IntervalYearMonth}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>Not implemented.
     *
     *     <p>All types of year-month interval are represented in the same way:
     *     an integer value which holds the number of months. For example,
     *     <code>INTERVAL '2' YEAR</code> and
     *     <code>INTERVAL '24' MONTH</code> are both represented as 24.
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Bigint}</td>
     * <td>{@link FennelStandardTypeDescriptor#INT_64 INT_64}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Varchar}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#VARCHAR VARCHAR}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Varbinary}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#VARBINARY VARBINARY}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Multiset}</td>
     * <td>{@link FennelStandardTypeDescriptor#VARBINARY VARBINARY}</td>
     * <td>The fields are serialized into the VARBINARY field in the standard
     *     Fennel serialization format. There is no 'count' field. To deduce
     *     the number of records, deserialize values until you reach the
     *     length of the field. Of course, this requires that every value
     *     takes at least one byte.
     *
     *     <p>The length of a multiset value is limited by the capacity of the
     *     <code>VARBINARY</code> datatype. This limitation will be liften
     *     when <code>LONG VARBINARY</code> is implemented.</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Row}</td>
     * <td>&nbsp;</td>
     * <td>The fields are 'flattened' so that they become top-level fields of
     *     the relation.
     *
     *     <p>If the row is nullable, then all fields will be
     *     nullable after flattening. An extra 'null indicator' field is added
     *     to discriminate between a NULL row and a not-NULL row which happens
     *     to have all fields NULL.</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Char}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#CHAR CHAR}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Binary}(precision)</td>
     * <td>{@link FennelStandardTypeDescriptor#BINARY BINARY}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Real}</td>
     * <td>{@link FennelStandardTypeDescriptor#REAL REAL}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Float}</td>
     * <td>{@link FennelStandardTypeDescriptor#DOUBLE DOUBLE}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * <tr>
     * <td>{@link SqlTypeName#Double}</td>
     * <td>{@link FennelStandardTypeDescriptor#DOUBLE DOUBLE}</td>
     * <td>&nbsp;</td>
     * </tr>
     *
     * </table>
     */
    public static FennelStandardTypeDescriptor convertSqlTypeNameToFennelType(
        SqlTypeName sqlType)
    {
        switch (sqlType.getOrdinal()) {
        case SqlTypeName.Boolean_ordinal:
            return FennelStandardTypeDescriptor.BOOL;
        case SqlTypeName.Tinyint_ordinal:
            return FennelStandardTypeDescriptor.INT_8;
        case SqlTypeName.Smallint_ordinal:
            return FennelStandardTypeDescriptor.INT_16;
        case SqlTypeName.Integer_ordinal:
            return FennelStandardTypeDescriptor.INT_32;
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
        case SqlTypeName.Bigint_ordinal:
        case SqlTypeName.IntervalDayTime_ordinal:
        case SqlTypeName.IntervalYearMonth_ordinal:
            return FennelStandardTypeDescriptor.INT_64;
        case SqlTypeName.Varchar_ordinal:
            return FennelStandardTypeDescriptor.VARCHAR;
        case SqlTypeName.Varbinary_ordinal:
        case SqlTypeName.Multiset_ordinal:
            return FennelStandardTypeDescriptor.VARBINARY;
        case SqlTypeName.Char_ordinal:
            return FennelStandardTypeDescriptor.CHAR;
        case SqlTypeName.Binary_ordinal:
            return FennelStandardTypeDescriptor.BINARY;
        case SqlTypeName.Real_ordinal:
            return FennelStandardTypeDescriptor.REAL;
        case SqlTypeName.Decimal_ordinal:
            return FennelStandardTypeDescriptor.INT_64;
        case SqlTypeName.Float_ordinal:
        case SqlTypeName.Double_ordinal:
            return FennelStandardTypeDescriptor.DOUBLE;
        default:
            throw sqlType.unexpected();
        }
    }

    /**
     * @return the preparing stmt that a relational expression belongs to
     */
    public static FarragoPreparingStmt getPreparingStmt(FennelRel rel)
    {
        RelOptCluster cluster = rel.getCluster();
        RelOptPlanner planner = cluster.getPlanner();
        if (planner instanceof FarragoSessionPlanner) {
            FarragoSessionPlanner farragoPlanner =
                (FarragoSessionPlanner) planner;
            return (FarragoPreparingStmt)farragoPlanner.getPreparingStmt();
        } else {
            return null;
        }
    }

    /**
     * @return the repository that a relational expression belongs to
     */
    public static FarragoRepos getRepos(FennelRel rel)
    {
        return getPreparingStmt(rel).getRepos();
    }

    /**
     * Converts a {@link SargExpr} into a relational expression which produces
     * a representation for the sequence of resolved intervals expected by
     * Fennel BTree searches.
     *
     * @param callTraits traits to apply to new rels generated
     *
     * @param keyRowType input row type expected by BTree search
     *
     * @param cluster query cluster
     *
     * @param sargExpr expression to be converted
     *
     * @return corresponding relational expression
     */
    public static RelNode convertSargExpr(
        RelTraitSet callTraits,
        RelDataType keyRowType,
        RelOptCluster cluster,
        SargExpr sargExpr)
    {
        SargIntervalSequence seq = sargExpr.evaluate();

        if (seq.getList().isEmpty()) {
            // TODO jvs 27-Jan-2006: in this case, should replace the entire
            // original expression with a NoRowRel instead, and then teach the
            // optimizer how to propagate that empty set up as far as it can
            // go.  For now we just make up an interval which is
            // guaranteed not to find anything.
            SargFactory factory = sargExpr.getFactory();
            SargIntervalExpr emptyIntervalExpr =
                factory.newIntervalExpr(
                    sargExpr.getDataType(),
                    SqlNullSemantics.NULL_MATCHES_NULL);
            emptyIntervalExpr.setLower(
                factory.newNullLiteral(),
                SargStrictness.OPEN);
            emptyIntervalExpr.setUpper(
                factory.newNullLiteral(),
                SargStrictness.OPEN);

            seq = emptyIntervalExpr.evaluate();
            assert(seq.getList().size() == 1);
        }

        List<List<RexNode>> inputTuples = new ArrayList<List<RexNode>>();

        boolean allLiteral = true;
        for (SargInterval interval : seq.getList()) {
            List<RexNode> inputTuple =
                convertIntervalToTuple(
                    cluster.getRexBuilder(),
                    interval);
            inputTuples.add(inputTuple);
            for (RexNode value : inputTuple) {
                if (!(value instanceof RexLiteral)) {
                    allLiteral = false;
                    break;
                }
            }
        }

        if (allLiteral) {
            // Since all tuple values are literals, we can optimize
            // to use the FennelValuesRel representation.
            return new FennelValuesRel(
                cluster,
                keyRowType,
                (List) inputTuples);
        }

        // Otherwise, we have to convert to UnionAll ({PROJECT(ONEROW)}*)
        // representation.
        
        // TODO jvs 19-Feb-2006:  It doesn't have to be all-or-nothing.
        // We could split it up with one FennelValuesRel in the UnionAll
        // taking care of all the literals.  Typical case would be
        // something like WHERE x IN (10, 20, ?)

        // FIXME jvs 23-Jan-2006:  But should not be using a UnionAll here,
        // because order may be important.  What to do?
        
        List<RelNode> inputs = new ArrayList<RelNode>();

        for (List<RexNode> inputTuple : inputTuples) {
            inputs.add(
                convertIntervalTupleToRel(
                    callTraits,
                    keyRowType,
                    cluster,
                    inputTuple));
        }

        if (inputs.size() == 1) {
            return inputs.get(0);
        }
        
        UnionRel unionRel = new UnionRel(
            cluster,
            inputs.toArray(new RelNode[0]),
            true);
        return unionRel;
    }

    /**
     * Converts a {@link SargInterval} into the directive tuple representation
     * expected by Fennel BTree searches.
     *
     * @param rexBuilder builder for tuple values
     *
     * @param interval interval to be converted
     *
     * @return corresponding tuple
     */
    public static List<RexNode> convertIntervalToTuple(
        RexBuilder rexBuilder,
        SargInterval interval)
    {
        RexLiteral lowerBoundDirective = convertEndpoint(
            rexBuilder,
            interval.getLowerBound());

        RexLiteral upperBoundDirective = convertEndpoint(
            rexBuilder,
            interval.getUpperBound());

        RexNode lowerBoundCoordinate = convertCoordinate(
            rexBuilder,
            interval.getLowerBound());
        
        RexNode upperBoundCoordinate = convertCoordinate(
            rexBuilder,
            interval.getUpperBound());

        return Arrays.asList(
            new RexNode [] {
                lowerBoundDirective,
                lowerBoundCoordinate,
                upperBoundDirective,
                upperBoundCoordinate,
            });
    }
    
    /**
     * Converts the tuple representation of a {@link SargInterval} into a
     * relational expression.
     *
     * @param callTraits traits to apply to new rels generated
     *
     * @param keyRowType input row type expected by BTree search
     *
     * @param cluster query cluster
     *
     * @param tuple tuple to be converted
     *
     * @return corresponding relational expression
     */
    public static RelNode convertIntervalTupleToRel(
        RelTraitSet callTraits,
        RelDataType keyRowType,
        RelOptCluster cluster,
        List<RexNode> tuple)
    {
        // Generate a one-row relation producing the key to search for.
        OneRowRel oneRowRel = new OneRowRel(cluster);
        RelNode keyRel = CalcRel.createProject(
            oneRowRel, tuple.toArray(RexNode.EMPTY_ARRAY), null);

        // For dynamic parameters, add a filter to remove nulls, since they can
        // never match in a comparison.  FIXME:  This isn't quite right,
        // since the other bound may not be a dynamic parameter.
        if ((tuple.get(1) instanceof RexDynamicParam)
            || (tuple.get(3) instanceof RexDynamicParam))
        {
            keyRel = RelOptUtil.createNullFilter(keyRel, null);
        }
        
        // Generate code to cast the keys to the index column type.
        RelNode castRel = RelOptUtil.createCastRel(
            keyRel, keyRowType, false);
        return castRel;
    }

    /**
     * Converts a {@link SargEndpoint} into the literal representation for a
     * lower/upper bound directive expected by Fennel BTree searches.
     *
     * @param rexBuilder builder for rex expressions
     *
     * @param endpoint endpoint to be converted
     *
     * @return literal representation
     */
    public static RexLiteral convertEndpoint(
        RexBuilder rexBuilder,
        SargEndpoint endpoint)
    {
        FennelSearchEndpoint fennelEndpoint;
        if (endpoint.getBoundType() == SargBoundType.LOWER) {
            if (endpoint.getStrictness() == SargStrictness.CLOSED) {
                fennelEndpoint = FennelSearchEndpoint.SEARCH_CLOSED_LOWER;
            } else if (endpoint.isFinite()) {
                fennelEndpoint = FennelSearchEndpoint.SEARCH_OPEN_LOWER;
            } else {
                fennelEndpoint = FennelSearchEndpoint.SEARCH_UNBOUNDED_LOWER;
            }
        } else {
            if (endpoint.getStrictness() == SargStrictness.CLOSED) {
                fennelEndpoint = FennelSearchEndpoint.SEARCH_CLOSED_UPPER;
            } else if (endpoint.isFinite()) {
                fennelEndpoint = FennelSearchEndpoint.SEARCH_OPEN_UPPER;
            } else {
                fennelEndpoint = FennelSearchEndpoint.SEARCH_UNBOUNDED_UPPER;
            }
        }
        return rexBuilder.makeLiteral(fennelEndpoint.getSymbol());
    }

    /**
     * Converts a {@link SargEndpoint} into the rex coordinate representation
     * expected by Fennel BTree searches.
     *
     * @param rexBuilder builder for rex expressions
     *
     * @param endpoint endpoint to be converted
     *
     * @return rex representation
     */
    public static RexNode convertCoordinate(
        RexBuilder rexBuilder,
        SargEndpoint endpoint)
    {
        if (endpoint.isFinite()) {
            return endpoint.getCoordinate();
        } else {
            // infinity gets represented as null
            return rexBuilder.constantNull();
        }
    }
}


// End FennelRelUtil.java
