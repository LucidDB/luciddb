/*
// $Id$
// pg2luciddb is a PG emulator for LucidDB
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
// Portions Copyright (C) 2009 Alexander Mekhrishvili
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
*/

package org.luciddb.pg2luciddb;

/**
 * JDBC to PostgreSQL type mapping from file pg_type.h
 */

public class JDBCToPostgreSQLType 
{
    private static final int POSTGRESQL_TYPE_INVALID = 0;

    private static final Object types[][] = 
    {
            {"bool", new Integer(16)},
            // not sure about this !!!
            {"boolean", new Integer(16)},
            {"bytea", new Integer(17)},
            //{"char", new Integer(18)},

            {"char", new Integer(1043)},

            {"name", new Integer(19)},
            {"int8", new Integer(20)},
            {"int2", new Integer(21)},
            {"int2vector", new Integer(22)},
            {"int4", new Integer(23)},
            {"integer", new Integer(23)},
            {"smallint", new Integer(21)},
            {"tinyint", new Integer(21)},
            {"bigint", new Integer(20)},
            {"regproc", new Integer(24)},
            {"text", new Integer(25)},
            {"oid", new Integer(26)},
            {"tid", new Integer(27)},
            {"xid", new Integer(28)},
            {"cid", new Integer(29)},
            {"oidvector", new Integer(30)},
            {"pg_type", new Integer(71)},
            {"pg_attribute", new Integer(75)},
            {"pg_proc", new Integer(81)},
            {"pg_class", new Integer(83)},
            {"xml", new Integer(142)},
            //{"_xml", new Integer(143)},
            //{"smgr", new Integer(210)},
            {"point", new Integer(600)},
            {"lseg", new Integer(601)},
            {"path", new Integer(602)},
            {"box", new Integer(603)},
            {"polygon", new Integer(604)},
            {"line", new Integer(628)},
            //{"_line", new Integer(629)},
            {"cidr", new Integer(650)},
            //{"_cidr", new Integer(651)},
            {"real", new Integer(700)},
            {"float4", new Integer(700)},
            {"float", new Integer(701)},
            {"double", new Integer(701)},
            {"float8", new Integer(701)},
            {"abstime", new Integer(702)},
            {"reltime", new Integer(703)},
            {"tinterval", new Integer(704)},
            {"unknown", new Integer(705)},
            {"circle", new Integer(718)},
            //{"_circle", new Integer(719)},
            {"money", new Integer(790)},
            //{"_money", new Integer(791)},
            {"macaddr", new Integer(829)},
            {"inet", new Integer(869)},
            //{"_bool", new Integer(1000)},
            //{"_bytea", new Integer(1001)},
            //{"_char", new Integer(1002)},
            //{"_name", new Integer(1003)},
            {"_int2", new Integer(1005)},
            //{"_int2vector", new Integer(1006)},
            {"_int4", new Integer(1007)},
            //{"_regproc", new Integer(1008)},
            //{"_text", new Integer(1009)},
            //{"_tid", new Integer(1010)},
            //{"_xid", new Integer(1011)},
            //{"_cid", new Integer(1012)},
            //{"_oidvector", new Integer(1013)},
            //{"_bchar", new Integer(1014)},
            //{"_varchar", new Integer(1015)},
            //{"_int8", new Integer(1016)},
            //{"_point", new Integer(1017)},
            //{"_lseg", new Integer(1018)},
            //{"_path", new Integer(1019)},
            //{"_box", new Integer(1020)},
            {"_float4", new Integer(1021)},
            //{"_float8", new Integer(1022)},
            //{"_abstime", new Integer(1023)},
            //{"_reltime", new Integer(1024)},
            //{"_tinterval", new Integer(1025)},
            //{"_poligon", new Integer(1027)},
            //{"_oid", new Integer(1028)},
            {"aclitem", new Integer(1033)},
            //{"_aclitem", new Integer(1034)},
            //{"_macaddr", new Integer(1040)},
            //{"_inet", new Integer(1041)},
            {"bpchar", new Integer(1042)},
            {"varchar", new Integer(1043)},
            {"date", new Integer(1082)},
            {"time", new Integer(1083)},
            {"timestamp", new Integer(1114)},
            //{"_timestamp", new Integer(1115)},
            //{"_date", new Integer(1182)},
            //{"_time", new Integer(1183)},
            {"timestamptz", new Integer(1184)},
            //{"_timestamptz", new Integer(1185)},             
            {"interval", new Integer(1186)},
            //{"_interval", new Integer(1187)},
            //{"_numeric", new Integer(1231)},
            {"_cstring", new Integer(1263)},
            {"timetz", new Integer(1266)},
            {"bit", new Integer(1560)},
            //{"_bit", new Integer(1561)},
            {"varbit", new Integer(1562)},
            //{"_varbit", new Integer(1563)},
            {"numeric", new Integer(1700)},
            {"decimal", new Integer(1700)},
            {"refcursor", new Integer(1790)},
            //{"_refcursor", new Integer(2201)},
            {"regprocedure", new Integer(2202)},
            {"regoper", new Integer(2203)},
            {"regoperator", new Integer(2204)},
            {"regclass", new Integer(2205)},
            {"regtype", new Integer(2206)},
            //{"_regprocedure", new Integer(2207)},
            //{"_regoper", new Integer(2208)},
            //{"_regoperator", new Integer(2209)},
            //{"_regclass", new Integer(2210)},
            {"_regtype", new Integer(2211)},
            {"cstring", new Integer(2275)},
            //{"uuid", new Integer(2950)},
            //{"_uuid", new Integer(2951)},
            {"tsvector", new Integer(3614)},
            {"gtsvector", new Integer(3642)},
            {"tsquery", new Integer(3615)},
            {"regconfig", new Integer(3734)},
            {"regdictionary", new Integer(3769)},
            //{"_tsvector", new Integer(3643)},
            //{"_gtsvector", new Integer(3644)},
            //{"_tsquery", new Integer(3645)},
            //{"_regconfig", new Integer(3735)},
            //{"_regdictionary", new Integer(3770)},
            //{"txid_snapshot", new Integer(2970)},
            //{"_txid_snapshot", new Integer(2949)},
    };

    /**
     * Return the PostgreSQL data type corresponding to "type" parameter.
     */
    public static int getPostgreSQLType(String type) 
    {
        for (int i = 0; i < types.length; i++) 
        {
            if (type.equalsIgnoreCase((String) types[i][0])) {
                return (Integer) types[i][1];
            }
        }

        return POSTGRESQL_TYPE_INVALID;
    }
}
