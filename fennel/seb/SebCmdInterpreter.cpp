/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/seb/SebCmdInterpreter.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/db/Database.h"

#include <strstream>

#include "scaledb/incl/SdbStorageAPI.h"

FENNEL_BEGIN_CPPFILE("$Id$");

unsigned short SebCmdInterpreter::userId;
unsigned short SebCmdInterpreter::dbId;

void SebCmdInterpreter::visit(ProxyCmdOpenDatabase &cmd)
{
    CmdInterpreter::visit(cmd);
    ConfigMap configMap;
    SharedProxyDatabaseParam pParam = cmd.getParams();
    for (; pParam; ++pParam) {
        configMap.setStringParam(pParam->getName(), pParam->getValue());
    }
    std::string configFile =
        configMap.getStringParam("databaseDir") + "/scaledb.cnf";
    if (SDBGlobalInit(const_cast<char *>(configFile.c_str()))) {
        throw FennelExcn(std::string("Failed to initialize ScaleDB"));
    }
    // REVIEW jvs 12-Jul-2009:  What is the correct binding between
    // users, threads, etc?
    userId = SDBGetNewUserId();
    dbId = SDBOpenDatabase(userId, const_cast<char *>("test"));
}

void SebCmdInterpreter::visit(ProxyCmdCloseDatabase &cmd)
{
    CmdInterpreter::visit(cmd);
    SDBGlobalEnd();
}

void SebCmdInterpreter::visit(ProxyCmdBeginTxn &cmd)
{
    CmdInterpreter::visit(cmd);
    SDBStartTransaction(userId);
}

void SebCmdInterpreter::visit(ProxyCmdCommit &cmd)
{
    CmdInterpreter::visit(cmd);
    // TODO jvs 13-Jul-2009:  savepoints, 2-phase commit
    SDBCommit(userId);
}

void SebCmdInterpreter::visit(ProxyCmdRollback &cmd)
{
    CmdInterpreter::visit(cmd);
    SDBRollBack(userId);
}

void SebCmdInterpreter::visit(ProxyCmdCreateIndex &cmd)
{
    CmdInterpreter::visit(cmd);

    // TODO jvs 11-Jul-2009:  discriminate between clustered
    // and unclustered

    TxnHandle *pTxnHandle = getTxnHandle(cmd.getTxnHandle());

    TupleDescriptor tupleDesc;
    readTupleDescriptor(
        tupleDesc,
        *(cmd.getTupleDesc()),
        pTxnHandle->pDb->getTypeFactory());

    std::ostrstream ossTbl;
    ossTbl << "tbl" << resultHandle << std::ends;
    std::string tblName = ossTbl.str();

    unsigned short tableId =
        SDBCreateTable(userId, dbId, const_cast<char *>(tblName.c_str()));
    for (uint i = 0; i < tupleDesc.size(); ++i) {
        TupleAttributeDescriptor const &attrDesc = tupleDesc[i];
        std::ostrstream oss;
        oss << "field" << i << std::ends;
        std::string fieldName = oss.str();
        StandardTypeDescriptorOrdinal typeOrdinal =
            static_cast<StandardTypeDescriptorOrdinal>(
                attrDesc.pTypeDescriptor->getOrdinal());
        unsigned short typeCode;
        // REVIEW jvs 11-Jul-2009:  mapping for floating point,
        // UNICODE, CHAR vs VARCHAR
        switch (typeOrdinal) {
        case STANDARD_TYPE_INT_8:
        case STANDARD_TYPE_INT_16:
        case STANDARD_TYPE_INT_32:
        case STANDARD_TYPE_INT_64:
        case STANDARD_TYPE_REAL:
        case STANDARD_TYPE_DOUBLE:
            typeCode = ENGINE_TYPE_S_NUMBER;
            break;
        case STANDARD_TYPE_UINT_8:
        case STANDARD_TYPE_UINT_16:
        case STANDARD_TYPE_UINT_32:
        case STANDARD_TYPE_UINT_64:
        case STANDARD_TYPE_BOOL:
            typeCode = ENGINE_TYPE_U_NUMBER;
            break;
        case STANDARD_TYPE_CHAR:
        case STANDARD_TYPE_VARCHAR:
        case STANDARD_TYPE_UNICODE_CHAR:
        case STANDARD_TYPE_UNICODE_VARCHAR:
            typeCode = ENGINE_TYPE_STRING;
            break;
        case STANDARD_TYPE_BINARY:
        case STANDARD_TYPE_VARBINARY:
            typeCode = ENGINE_TYPE_BYTE_ARRAY;
            break;
        default:
            throw FennelExcn(std::string("Unsupported datatype"));
        }
        SDBCreateField(
            userId, dbId, tableId,
            const_cast<char *>(fieldName.c_str()),
            typeCode,
            attrDesc.cbStorage,
            0,
            NULL,
            false,
            0);
    }
    TupleProjection keyProj;
    CmdInterpreter::readTupleProjection(
        keyProj, cmd.getKeyProj());
    std::vector<std::string> keyFieldStrings;
    for (uint i = 0; i < keyProj.size(); ++i) {
        std::ostrstream oss;
        oss << "field" << keyProj[i] << std::ends;
        std::string fieldName = oss.str();
        keyFieldStrings.push_back(fieldName);
    }
    std::vector<char *> keyFields;
    for (uint i = 0; i < keyFieldStrings.size(); ++i) {
        keyFields.push_back(const_cast<char *>(keyFieldStrings[i].c_str()));
    }
    keyFields.push_back(NULL);

    std::ostrstream ossIdx;
    ossTbl << "idx" << resultHandle << std::ends;
    std::string idxName = ossIdx.str();
    unsigned short indexId = SDBCreateIndex(
        userId,
        dbId,
        tableId,
        const_cast<char *>(idxName.c_str()),
        &(keyFields[0]),
        NULL,
        true,
        false,
        NULL,
        0,
        0);
    SDBOpenAllDBFiles(userId, dbId);
    uint pageId = indexId;
    pageId <<= 16;
    pageId |= tableId;
    resultHandle = pageId;
}

FENNEL_END_CPPFILE("$Id$");

// End SebCmdInterpreter.cpp
