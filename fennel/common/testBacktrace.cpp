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

// a primitive, manual, stand-alone test of Backtrace

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/Backtrace.h"
#include "fennel/common/ConfigMap.h"
#include <ostream>
#include <cassert>
#include <stdexcept>

FENNEL_BEGIN_CPPFILE("$Id$");

class foo {
    void g(int);
    void h(int);
    void k(int);
public:
    foo() {}
    ~foo() {}
    void f(int);
};

void foo::f(int n)
{
    g(n);
}

void foo::g(int n)
{
    h(n);
    k(n);
}

void foo::h(int n)
{
    std::cerr << "\ntesting Backtrace(" << n << ")\n";
    Backtrace bt(n);
    std::cerr << bt;
}

void foo::k(int n)
{
    void *addrs[32];
    assert(n < 32);
    std::cerr << "\ntesting Backtrace(" << n << ", addrs)\n";
    Backtrace bt(n, addrs);
    std::cerr << bt;
}

FENNEL_END_CPPFILE("$Id");

// testBacktrace tests Backtrace()
// testBacktrace a tests AutoBacktrace of assert()
// testBacktrace e tests AutoBacktrace of an uncaught exception
// testBacktrace s tests AutoBacktrace of a SIGSEGV
int main(int argc, char **argv)
{
    if (argc == 1) {
        fennel::foo o;
        o.f(16);
        o.f(2);
    } else {
        fennel::AutoBacktrace::install();
        switch (argv[1][0]) {
        case 'a':
            assert(false);
            std::cerr << "not reached\n";
            break;
        case 'p':
            permAssert(false);
            std::cerr << "not reached\n";
            break;
        case 'e':
            std::cerr
                << "throw new std::runtime_error(\"testing AutoBacktrace\")\n";
            throw new std::runtime_error("testing AutoBacktrace");
            std::cerr << "not reached\n";
            break;
        case 's':
            {
                fennel::ConfigMap configMap;
                configMap.readParams(*(std::istream *) NULL);
            }
            break;
        default:
            ;
        }
    }
    exit(0);
}

// End testBacktrace.cpp
