/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#ifndef Fennel_TestBase_Included
#define Fennel_TestBase_Included

#include "fennel/common/ConfigMap.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/common/TraceTarget.h"
#include "fennel/common/FileStatsTarget.h"
#include "fennel/synch/StatsTimer.h"

#include <boost/shared_ptr.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <fstream>

typedef boost::unit_test_framework::test_suite TestSuite;
typedef boost::unit_test_framework::test_case TestCase;

FENNEL_BEGIN_NAMESPACE

/**
 * TestBase is the common base for all fennel tests.
 */
class TestBase
    : public TraceTarget,
        public boost::enable_shared_from_this<TestBase>
    
{
    // implement TraceTarget
    virtual void notifyTrace(
        std::string source,TraceLevel level,std::string message);
    virtual TraceLevel getSourceTraceLevel(std::string source);
    
protected:
    /**
     * Boost test suite.
     */
    TestSuite *pTestSuite;
    
    boost::shared_ptr<TestBase> pTestObj;

    /**
     * Output file stream for tracing.
     */
    std::ofstream traceStream;

    /**
     * Protects traceStream.
     */
    StrictMutex traceMutex;
    
    /**
     * Name of test.
     */
    std::string testName;

    /**
     * Level at which to trace test execution.
     */
    TraceLevel traceLevel;

    /**
     * Output for stats.
     */
    FileStatsTarget statsTarget;

    /**
     * Timer for stats collection.
     */
    StatsTimer statsTimer;

    // TODO:  move to SynchObj
    void snooze(uint nSeconds);

public:
    static ParamName paramTestSuiteName;
    static ParamName paramTraceFileName;
    static ParamName paramDictionaryFileName;
    static ParamName paramTraceLevel;
    static ParamName paramStatsFileName;
    
    /**
     * Configuration parameters.  The reason this is static is so that no
     * constructor parameters (which burden virtual bases) are needed.
     */
    static ConfigMap configMap;

    explicit TestBase();
    virtual ~TestBase();

    // helpers for FENNEL_UNIT_TEST_SUITE etc. below
    static void readParams(int argc,char **argv);
    TestSuite *releaseTestSuite();
    void beforeTestCase(std::string testCaseName);
    void afterTestCase(std::string testCaseName);

    /**
     * Equivalent to JUnit TestCase.setUp; this is called before each test case
     * method is invoked.  Default is no-op.
     */
    virtual void testCaseSetUp();
    
    /**
     * Equivalent to JUnit TestCase.tearDown; this is called after each test
     * case method is invoked.  Default is no-op.
     */
    virtual void testCaseTearDown();
};

/**
 * BaseTestCase enhances test_case with setUp/tearDown hooks around
 * each test case method invocation.
 *
 *<p>
 *
 * NOTE:  Code adapted from boost::unit_test_framework::class_test_case.
 */
template<class UserTestClass>
class BaseTestCase : public TestCase
{
public:
    typedef void  (UserTestClass::*function_type)();
    
    // Constructor
    BaseTestCase(
        function_type f_,
        std::string name_,
        boost::shared_ptr<UserTestClass>& user_test_case_ )
        : TestCase(name_,true,1),
          m_user_test_case(user_test_case_),
          m_function(f_)
    {
    }

private:
    // test case implementation
    void do_run()
    {
        m_user_test_case->beforeTestCase(p_name);
        m_user_test_case->testCaseSetUp();
        try {
            ((*m_user_test_case).*m_function)();
        } catch (...) {
            try {
                m_user_test_case->testCaseTearDown();
            } catch (...) {
                // ignore teardown errors after failure
            }
            throw;
        }
        try {
            m_user_test_case->testCaseTearDown();
        } catch (...) {
            m_user_test_case->afterTestCase(p_name);
            throw;
        }
        m_user_test_case->afterTestCase(p_name);
    }
    
    void do_destroy()
    {
        m_user_test_case.reset();
    }

    boost::shared_ptr<UserTestClass> m_user_test_case;
    function_type       m_function;
};

// FENNEL_UNIT_TEST_SUITE should be invoked at top scope within the .cpp file
// defining a test class derived from TestBase, with UserTestClass equal to the
// derived class name.

#define FENNEL_UNIT_TEST_SUITE(UserTestClass) \
TestSuite* init_unit_test_suite(int argc,char **argv) \
{ \
    TestBase::readParams(argc,argv); \
    std::string paramKey(TestBase::paramTestSuiteName); \
    std::string paramVal(#UserTestClass); \
    TestBase::configMap.setStringParam(paramKey,paramVal); \
    UserTestClass *pTestObj = new UserTestClass(); \
    return pTestObj->releaseTestSuite(); \
}

// FENNEL_UNIT_TEST_CASE should be invoked within the test
// class constructor, once for each test case method

#define FENNEL_UNIT_TEST_CASE(UserTestClass,testMethodName) \
do { \
    boost::shared_ptr<UserTestClass> pDerivedTestObj = \
        boost::dynamic_pointer_cast<UserTestClass>(pTestObj); \
        pTestSuite->add(new BaseTestCase<UserTestClass>( \
            &UserTestClass::testMethodName, \
            #testMethodName, \
            pDerivedTestObj)); \
} while (0)

FENNEL_END_NAMESPACE

#endif

// End TestBase.h
