/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_SharedTypes_Included
#define Fennel_SharedTypes_Included

#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>

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
typedef boost::weak_ptr<Segment> WeakSegment;

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

class SegPageBackupRestoreDevice;
typedef boost::shared_ptr<SegPageBackupRestoreDevice>
    SharedSegPageBackupRestoreDevice;
typedef boost::weak_ptr<SegPageBackupRestoreDevice>
    WeakSegPageBackupRestoreDevice;

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

class BTreeNonLeafReader;
typedef boost::shared_ptr<BTreeNonLeafReader> SharedBTreeNonLeafReader;

class BTreeLeafReader;
typedef boost::shared_ptr<BTreeLeafReader> SharedBTreeLeafReader;

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

class SegBufferReader;
typedef boost::shared_ptr<SegBufferReader> SharedSegBufferReader;

class SegBufferWriter;
typedef boost::shared_ptr<SegBufferWriter> SharedSegBufferWriter;

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
