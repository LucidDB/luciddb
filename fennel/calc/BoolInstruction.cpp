/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Tech
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
//
// Instruction->Bool
*/
#include "fennel/common/CommonPreamble.h"
#include "fennel/calc/BoolInstruction.h"

FENNEL_BEGIN_CPPFILE("$Id$");

const char *
BoolOr::longName() const
{
    return "BoolOr";
}
const char *
BoolOr::shortName() const
{
    return "OR";
}
void
BoolOr::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}


const char *
BoolAnd::longName() const
{
    return "BoolAnd";
}
const char *
BoolAnd::shortName() const
{
    return "AND";
}
void
BoolAnd::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}


const char * 
BoolNot::longName() const
{
    return "BoolNot";
}
const char * 
BoolNot::shortName() const
{
    return "NOT";
}
void
BoolNot::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}


const char * 
BoolMove::longName() const
{
    return "BoolMove";
}
const char * 
BoolMove::shortName() const
{
    return "MOVE";
}
void
BoolMove::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}


const char * 
BoolRef::longName() const
{
    return "BoolRef";
}
const char * 
BoolRef::shortName() const
{
    return "REF";
}
void
BoolRef::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}


const char *
BoolIs::longName() const
{
    return "BoolIs";
}
const char *
BoolIs::shortName() const
{
    return "IS";
}
void
BoolIs::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}


const char * 
BoolIsNot::longName() const 
{
    return "BoolIsNot";
}
const char * 
BoolIsNot::shortName() const 
{
    return "ISNOT";
}
void
BoolIsNot::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}


const char *
BoolEqual::longName() const
{
    return "BoolEqual";
}
const char *
BoolEqual::shortName() const
{
    return "EQ";
}
void
BoolEqual::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}


const char * 
BoolNotEqual::longName() const 
{
    return "BoolNotEqual";
}
const char * 
BoolNotEqual::shortName() const 
{
    return "NE";
}
void
BoolNotEqual::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}

const char * 
BoolGreater::longName() const 
{
    return "BoolGreater";
}
const char * 
BoolGreater::shortName() const 
{
    return "GT";
}
void
BoolGreater::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}

const char * 
BoolLess::longName() const 
{
    return "BoolLess";
}
const char * 
BoolLess::shortName() const 
{
    return "LT";
}
void
BoolLess::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}

const char * 
BoolIsNull::longName() const 
{
    return "BoolIsNull";
}
const char * 
BoolIsNull::shortName() const 
{
    return "ISNULL";
}
void
BoolIsNull::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}

const char * 
BoolIsNotNull::longName() const 
{
    return "BoolIsNotNull";
}
const char * 
BoolIsNotNull::shortName() const 
{
    return "ISNOTNULL";
}
void
BoolIsNotNull::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}

const char * 
BoolToNull::longName() const 
{
    return "BoolToNull";
}
const char * 
BoolToNull::shortName() const 
{
    return "TONULL";
}
void
BoolToNull::describe(string &out, bool values) const {
    describeHelper(out, values, longName(), shortName(), mResult, mOp1, mOp2);
}

FENNEL_END_CPPFILE("$Id$");

// End BoolInstruction.cpp
