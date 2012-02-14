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

#ifndef Fennel_LcsClusterVerifier_Included
#define Fennel_LcsClusterVerifier_Included

#include "fennel/lcs/LcsClusterReader.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Information about the cluster page
 */
struct ClusterPageData
{
    /**
     * cluster pageid
     */
    PageId clusterPageId;

    /**
     * rid stored in btree, used to access page
     */
    LcsRid bTreeRid;
};

/**
 * LcsClusterVerifier is a class for verifying cluster pages.  It reads
 * cluster pages and gathers data regarding the page.
 */
class FENNEL_LCS_EXPORT LcsClusterVerifier
    : public LcsClusterReader
{
    /**
     * Information gathered for the current cluster page
     */
    ClusterPageData pageData;

public:
    explicit LcsClusterVerifier(BTreeDescriptor const &treeDescriptor);

    /**
     * Retrieves cluster page data for the current cluster page
     */
    ClusterPageData &getPageData();
};

FENNEL_END_NAMESPACE

#endif

// End LcsClusterVerifier.h
