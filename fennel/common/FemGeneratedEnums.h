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

enum AggFunction {
    AGG_FUNC_COUNT,
    AGG_FUNC_MAX,
    AGG_FUNC_MIN,
    AGG_FUNC_SINGLE_VALUE,
    AGG_FUNC_SUM
};

static std::string AggFunction_names[] = {"AGG_FUNC_COUNT","AGG_FUNC_MAX","AGG_FUNC_MIN","AGG_FUNC_SINGLE_VALUE","AGG_FUNC_SUM",""};

enum BarrierReturnMode {
    BARRIER_RET_ALL_INPUTS,
    BARRIER_RET_ANY_INPUT,
    BARRIER_RET_FIRST_INPUT
};

static std::string BarrierReturnMode_names[] = {"BARRIER_RET_ALL_INPUTS","BARRIER_RET_ANY_INPUT","BARRIER_RET_FIRST_INPUT",""};

enum CompOperator {
    COMP_EQ,
    COMP_GE,
    COMP_GT,
    COMP_LE,
    COMP_LT,
    COMP_NE,
    COMP_NOOP
};

static std::string CompOperator_names[] = {"COMP_EQ","COMP_GE","COMP_GT","COMP_LE","COMP_LT","COMP_NE","COMP_NOOP",""};

enum Distinctness {
    DUP_ALLOW,
    DUP_DISCARD,
    DUP_FAIL
};

static std::string Distinctness_names[] = {"DUP_ALLOW","DUP_DISCARD","DUP_FAIL",""};

enum TableSamplingMode {
    SAMPLING_BERNOULLI,
    SAMPLING_OFF,
    SAMPLING_SYSTEM
};

static std::string TableSamplingMode_names[] = {"SAMPLING_BERNOULLI","SAMPLING_OFF","SAMPLING_SYSTEM",""};

