/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/TestBase.h"
#include "fennel/common/TraceSource.h"

#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/calc/CalcCommon.h"
#include "fennel/calc/StringToHex.h"
#include "fennel/calc/InstructionFactory.h"
#include "fennel/common/FennelExcn.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <string>
#include <limits>


using namespace fennel;
using namespace std;


class CalcExtContextTest : virtual public TestBase, public TraceSource
{
    void setupExtendedTestInstructions();
    void testCalcExtContext();
    void testCalcExtContextPost();
    
public:
    explicit CalcExtContextTest()
        : TraceSource(this,"CalcExtContextTest")
    {
        CalcInit::instance();
        FENNEL_UNIT_TEST_CASE(CalcExtContextTest, testCalcExtContext);
        FENNEL_UNIT_TEST_CASE(CalcExtContextTest, testCalcExtContextPost);
    }
     
    virtual ~CalcExtContextTest()
    {
    }
};

// Simple context class for testing purposes. Not thread safe.
class EICtx : public ExtendedInstructionContext
{
public:
    EICtx()
    {
        mCount++;
        BOOST_MESSAGE(mCount);
    }
    ~EICtx() 
    {
        mCount--;
        BOOST_MESSAGE(mCount);
    }
    static int mCount;
};

int EICtx::mCount = 0;

void
ctxInst1(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
ctxInst2(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op,
         RegisterRef<bool>* dummy2)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
ctxInst3(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op,
         RegisterRef<bool>* dummy2,
         RegisterRef<bool>* dummy3)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
ctxInst4(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op,
         RegisterRef<bool>* dummy2,
         RegisterRef<bool>* dummy3,
         RegisterRef<bool>* dummy4)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
ctxInst5(boost::scoped_ptr<ExtendedInstructionContext>& context,
         RegisterRef<bool>* op,
         RegisterRef<bool>* dummy2,
         RegisterRef<bool>* dummy3,
         RegisterRef<bool>* dummy4,
         RegisterRef<bool>* dummy5)
{
    if (context.get()) {
        op->value(false);
    } else {
        context.reset(new EICtx);
        op->value(true);
    }
}

void
CalcExtContextTest::setupExtendedTestInstructions()
{
    ExtendedInstructionTable* eit = InstructionFactory::getExtendedInstructionTable();
    ExtendedInstructionDef* inst;

    vector<StandardTypeDescriptorOrdinal>params;
    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst1", params,
             (ExtendedInstruction1Context<bool>*) NULL,
             ctxInst1);
    inst = eit->lookupBySignature("ctxInst1(bo)");
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst1"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 1);


    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst2", params,
             (ExtendedInstruction2Context<bool,bool>*) NULL,
             ctxInst2);
    inst = eit->lookupBySignature("ctxInst2(bo,bo)");
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst2"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 2);


    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst3", params,
             (ExtendedInstruction3Context<bool,bool,bool>*) NULL,
             ctxInst3);
    inst = eit->lookupBySignature("ctxInst3(bo,bo,bo)");
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst3"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 3);


    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst4", params,
             (ExtendedInstruction4Context<bool,bool,bool,bool>*) NULL,
             ctxInst4);
    inst = eit->lookupBySignature("ctxInst4(bo,bo,bo,bo)");
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst4"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 4);


    params.push_back(STANDARD_TYPE_BOOL);

    eit->add("ctxInst5", params,
             (ExtendedInstruction5Context<bool,bool,bool,bool,bool>*) NULL,
             ctxInst5);
    inst = eit->lookupBySignature("ctxInst5(bo,bo,bo,bo,bo)");
    BOOST_REQUIRE(inst);
    BOOST_CHECK_EQUAL(inst->getName(),string("ctxInst5"));
    BOOST_CHECK_EQUAL(inst->getParameterTypes().size(), 5);
    
}


void
CalcExtContextTest::testCalcExtContext()
{
    CalcInit::instance();
    // add in some extended instructions after init
    setupExtendedTestInstructions();

    ostringstream pg("");

    pg << "O bo,bo,bo,bo,bo,bo;" << endl;
    pg << "T;" << endl;
    pg << "CALL 'ctxInst1(O1);" << endl;
    pg << "CALL 'ctxInst2(O2,O0);" << endl;
    pg << "CALL 'ctxInst3(O3,O0,O0);" << endl;
    pg << "CALL 'ctxInst4(O4,O0,O0,O0);" << endl;
    pg << "CALL 'ctxInst5(O5,O0,O0,O0,O0);" << endl;

    BOOST_MESSAGE(pg.str());
    
    Calculator calc;
    
    try {
        calc.assemble(pg.str().c_str());
    }
    catch (FennelExcn& ex) {
        BOOST_MESSAGE("Assemble exception " << ex.getMessage());
        BOOST_MESSAGE(pg.str());
        BOOST_REQUIRE(0);
    }

    TupleDataWithBuffer outTuple(calc.getOutputRegisterDescriptor());
    TupleDataWithBuffer inTuple(calc.getInputRegisterDescriptor());

    calc.bind(&inTuple, &outTuple);
    calc.exec();

#if 0
    TuplePrinter tuplePrinter;
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), outTuple);
    cout << endl;
#endif

    int i;
    for (i=1; i <=5; i++) {
        BOOST_CHECK_EQUAL(*(reinterpret_cast<bool *>
                            (const_cast<PBuffer>((outTuple[i]).pData))),
                          true);
    }
    

    // call program again, should get different output
    calc.exec();

    for (i=1; i <=5; i++) {
        BOOST_CHECK_EQUAL(*(reinterpret_cast<bool *>
                            (const_cast<PBuffer>((outTuple[i]).pData))),
                          false);
    }
    
    // call program again, should get same output
    calc.exec();

    for (i=1; i <=5; i++) {
        BOOST_CHECK_EQUAL(*(reinterpret_cast<bool *>
                            (const_cast<PBuffer>((outTuple[i]).pData))),
                          false);
    }
    

#if 0
    tuplePrinter.print(cout, calc.getOutputRegisterDescriptor(), outTuple);
    cout << endl;
#endif

}

// be sure that all context objects were destroyed
void
CalcExtContextTest::testCalcExtContextPost()
{
    BOOST_CHECK_EQUAL(EICtx::mCount, 0);
}


FENNEL_UNIT_TEST_SUITE(CalcExtContextTest);

