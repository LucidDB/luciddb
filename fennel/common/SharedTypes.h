/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_SharedTypes_Included
#define Fennel_SharedTypes_Included

#include <boost/shared_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

// shared_ptr declarations to ease usage of handle/body pattern

class RandomAccessDevice;
typedef boost::shared_ptr<RandomAccessDevice> SharedRandomAccessDevice;

class Cache;
typedef boost::shared_ptr<Cache> SharedCache;

class CacheAccessor;
typedef boost::shared_ptr<CacheAccessor> SharedCacheAccessor;

class Segment;
typedef boost::shared_ptr<Segment> SharedSegment;

class SegmentAccessor;
typedef boost::shared_ptr<SegmentAccessor> SharedSegmentAccessor;

class SegmentFactory;
typedef boost::shared_ptr<SegmentFactory> SharedSegmentFactory;

class SegmentMap;
typedef boost::shared_ptr<SegmentMap> SharedSegmentMap;

class ByteOutputStream;
typedef boost::shared_ptr<ByteOutputStream> SharedByteOutputStream;

class ByteInputStream;
typedef boost::shared_ptr<ByteInputStream> SharedByteInputStream;

class ByteStreamMarker;
typedef boost::shared_ptr<ByteStreamMarker> SharedByteStreamMarker;

class ByteArrayOutputStream;
typedef boost::shared_ptr<ByteArrayOutputStream> SharedByteArrayOutputStream;

class ByteArrayInputStream;
typedef boost::shared_ptr<ByteArrayInputStream> SharedByteArrayInputStream;

class StatsSource;
typedef boost::shared_ptr<StatsSource> SharedStatsSource;

class StatsTarget;
typedef boost::shared_ptr<StatsTarget> SharedStatsTarget;

class SegOutputStream;
typedef boost::shared_ptr<SegOutputStream> SharedSegOutputStream;

class SegInputStream;
typedef boost::shared_ptr<SegInputStream> SharedSegInputStream;

class SegStreamAllocation;
typedef boost::shared_ptr<SegStreamAllocation> SharedSegStreamAllocation;

class SpillOutputStream;
typedef boost::shared_ptr<SpillOutputStream> SharedSpillOutputStream;

class CheckpointProvider;
typedef boost::shared_ptr<CheckpointProvider> SharedCheckpointProvider;

class LogicalTxn;
typedef boost::shared_ptr<LogicalTxn> SharedLogicalTxn;

class LogicalTxnLog;
typedef boost::shared_ptr<LogicalTxnLog> SharedLogicalTxnLog;

class LogicalRecoveryLog;
typedef boost::shared_ptr<LogicalRecoveryLog> SharedLogicalRecoveryLog;

class LogicalTxnParticipant;
typedef boost::shared_ptr<LogicalTxnParticipant> SharedLogicalTxnParticipant;

class BTreeAccessBase;
typedef boost::shared_ptr<BTreeAccessBase> SharedBTreeAccessBase;

class BTreeReader;
typedef boost::shared_ptr<BTreeReader> SharedBTreeReader;

class BTreeWriter;
typedef boost::shared_ptr<BTreeWriter> SharedBTreeWriter;

class BTreeVerifier;
typedef boost::shared_ptr<BTreeVerifier> SharedBTreeVerifier;

class BTreeBuilder;
typedef boost::shared_ptr<BTreeBuilder> SharedBTreeBuilder;

class BTreeRecoveryFactory;
typedef boost::shared_ptr<BTreeRecoveryFactory> SharedBTreeRecoveryFactory;

class Database;
typedef boost::shared_ptr<Database> SharedDatabase;

class PseudoUuidGenerator;
typedef boost::shared_ptr<PseudoUuidGenerator> SharedPseudoUuidGenerator;

class TraceTarget;
typedef boost::shared_ptr<TraceTarget> SharedTraceTarget;

class ErrorTarget;
typedef boost::shared_ptr<ErrorTarget> SharedErrorTarget;

class CheckpointThread;
typedef boost::shared_ptr<CheckpointThread> SharedCheckpointThread;

class ExecStream;
typedef boost::shared_ptr<ExecStream> SharedExecStream;

class ExecStreamEmbryo;
typedef boost::shared_ptr<ExecStreamEmbryo> SharedExecStreamEmbryo;

class ExecStreamBufAccessor;
typedef boost::shared_ptr<ExecStreamBufAccessor> SharedExecStreamBufAccessor;

class ExecStreamScheduler;
typedef boost::shared_ptr<ExecStreamScheduler> SharedExecStreamScheduler;

class ExecStreamParams;
typedef boost::shared_ptr<ExecStreamParams> SharedExecStreamParams;

class ExecStreamGraph;
typedef boost::shared_ptr<ExecStreamGraph> SharedExecStreamGraph;

class ExecStreamGraphEmbryo;
typedef boost::shared_ptr<ExecStreamGraphEmbryo> SharedExecStreamGraphEmbryo;

class ExecStreamFactory;
typedef boost::shared_ptr<ExecStreamFactory> SharedExecStreamFactory;

class ExecStreamSubFactory;
typedef boost::shared_ptr<ExecStreamSubFactory> SharedExecStreamSubFactory;

class ExecStreamResourceQuantity;
typedef boost::shared_ptr<ExecStreamResourceQuantity>
    SharedExecStreamResourceQuantity;

class ExecStreamGovernor;
typedef boost::shared_ptr<ExecStreamGovernor> SharedExecStreamGovernor;

class FtrsTableWriter;
typedef boost::shared_ptr<FtrsTableWriter> SharedFtrsTableWriter;

class FtrsTableWriterFactory;
typedef boost::shared_ptr<FtrsTableWriterFactory> SharedFtrsTableWriterFactory;

class Calculator;
typedef boost::shared_ptr<Calculator> SharedCalculator;

class DynamicParam;
typedef boost::shared_ptr<DynamicParam> SharedDynamicParam;

class DynamicParamManager;
typedef boost::shared_ptr<DynamicParamManager> SharedDynamicParamManager;

class SizeBuffer;
typedef boost::shared_ptr<SizeBuffer> SharedSizeBuffer;

class LcsClusterDump;
typedef boost::shared_ptr<LcsClusterDump> SharedLcsClusterDump;

class LcsClusterNodeWriter;
typedef boost::shared_ptr<LcsClusterNodeWriter> SharedLcsClusterNodeWriter;

class LcsCompareColKeyUsingOffsetIndex;
typedef boost::shared_ptr<LcsCompareColKeyUsingOffsetIndex>
    SharedLcsCompareColKeyUsingOffsetIndex;

class LcsClusterReader;
typedef boost::shared_ptr<LcsClusterReader> SharedLcsClusterReader;

class LbmEntry;
typedef boost::shared_ptr<LbmEntry> SharedLbmEntry;

class LbmTupleReader;
typedef boost::shared_ptr<LbmTupleReader> SharedLbmTupleReader;

class ByteBuffer;
typedef boost::shared_ptr<ByteBuffer> SharedByteBuffer;

class LhxPartition;
typedef boost::shared_ptr<LhxPartition> SharedLhxPartition;

class LhxPartitionWriter;
typedef boost::shared_ptr<LhxPartitionWriter> SharedLhxPartitionWriter;

class LhxPlan;
typedef boost::shared_ptr<LhxPlan> SharedLhxPlan;

class LhxPlan;
typedef boost::weak_ptr<LhxPlan> WeakLhxPlan;

struct LcsResidualFilter;
typedef boost::shared_ptr<LcsResidualFilter> SharedLcsResidualFilter;

FENNEL_END_NAMESPACE

#endif

// End SharedTypes.h
