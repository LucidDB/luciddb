/*
 // $Id$
 // Farrago is an extensible data management system.
 // Copyright (C) 2005-2009 The Eigenbase Project
 // Copyright (C) 2005-2009 SQLstream, Inc.
 // Copyright (C) 2005-2009 LucidEra, Inc.
 // Portions Copyright (C) 2004-2009 John V. Sichi
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
package net.sf.farrago.ddl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import net.sf.farrago.util.FarragoProperties;

import org.eigenbase.sql.SqlDialect;
import org.eigenbase.sql.util.SqlBuilder;
import org.eigenbase.util.Util;

/**
 * DdlSqlj contains the system-defined implementations for the standard SQLJ
 * system procedures such as INSTALL_JAR.
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DdlSqlj
{

    private static String INSTALL = "INSTALL";
    private static String REMOVE = "REMOVE";

    // ~ Methods
    // ----------------------------------------------------------------

    /**
     * @sql.2003 Part 13 Section 11.1
     */
    public static void install_jar(String url, String jar, int deploy)
        throws SQLException, IOException
    {
        url = url.trim();
        jar = jar.trim();

        if (deploy != 0) {
            // TODO jvs 18-Jan-2005
            // throw Util.needToImplement("deploy");
            if (url.toLowerCase().startsWith("class")) {
            } else {
                String myUrl = FarragoProperties.instance().expandProperties(
                    url);
                // Support deploy descriptor file FRG-387
                String[] sqls = getDeploySQLs(myUrl, INSTALL);

                if (sqls != null || sqls.length > 0) {
                    // logic: If there is any failure, stop processing
                    // statements
                    try {
                        for (String sql : sqls) {
                            System.out.println(sql);
                            executeSql(sql);
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                }
            }
            deploy = 0;
        }
        SqlBuilder sql = new SqlBuilder(SqlDialect.EIGENBASE);
        sql.append("CREATE JAR ");
        // REVIEW: We can't use sql.identifier(jar), because
        // the jar argument to install_jar is already quoted
        // if needed. But is there some sanitization we need
        // to do, or is no SQL injection attack possible?
        sql.append(jar);
        sql.append(" library ");
        sql.literal(url);
        sql.append(" options(");
        sql.append(deploy);
        sql.append(")");
        executeSql(sql.getSql());
    }

    /**
     * @sql.2003 Part 13 Section 11.2
     */
    public static void replace_jar(String url, String jar)
        throws SQLException
    {
        url = url.trim();
        jar = jar.trim();

        // TODO jvs 18-Jan-2005
        throw Util.needToImplement("replace_jar");
    }

    /**
     * @sql.2003 Part 13 Section 11.3
     */
    public static void remove_jar(String jar, int undeploy)
        throws SQLException, IOException
    {
        jar = jar.trim();

        if (undeploy != 0) {
            // TODO jvs 18-Jan-2005
            // throw Util.needToImplement("deploy");
            String url = getJarURL(jar);
            if (url != null) {
                String myUrl =
                    FarragoProperties.instance().expandProperties(url);
                String[] sqls = getDeploySQLs(myUrl, REMOVE);
                if (sqls != null || sqls.length > 0) {
                // logic:If there are any failures, ignore, and continue
                // executing statements
                    for (String sql : sqls) {
                        try {
                            System.out.println(sql);
                            executeSql(sql);
                        } catch (SQLException ex) {
                            ex.printStackTrace();
                        }
                    }
               }
            }
            undeploy = 0;
        }
        SqlBuilder sql = new SqlBuilder(SqlDialect.EIGENBASE);
        sql.append("DROP JAR ");
        // REVIEW: see comments in install_jar regarding possible
        // sanitization needed
        sql.append(jar);
        sql.append(" options (");
        sql.append(undeploy);
        sql.append(") RESTRICT");
        executeSql(sql.getSql());
    }

    /**
     * @sql.2003 Part 13 Section 11.4
     */
    public static void alter_java_path(String jar, String path)
        throws SQLException
    {
        jar = jar.trim();
        path = path.trim();
        throw Util.needToImplement("alter_java_path");
    }

    private static void executeSql(String sql)
        throws SQLException
    {
        Connection conn =
            DriverManager.getConnection(
                "jdbc:default:connection");
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);

        // NOTE jvs 19-Jan-2005: no need for cleanup; default connection
        // is cleaned up automatically.
    }

    private static String getJarURL(String jar)
        throws SQLException
    {
        String url = "";
        String catalog = "";
        String schema = "";
        String jarname = "";
        String[] ss = jar.split("\\.");
        int size = ss.length;
        switch (size) {
        case 1:
            jarname = ss[0].replace("\"", "").trim();
            break;
        case 2:
            schema = ss[0].replace("\"", "").trim();
            jarname = ss[1].replace("\"", "").trim();
            break;
        case 3:
            catalog = ss[0].replace("\"", "").trim();
            schema = ss[1].replace("\"", "").trim();
            jarname = ss[2].replace("\"", "").trim();
            break;
        default:
            throw new SQLException("Input[" + jar + "] is invaild");
        }
        ;
        Connection conn =
            DriverManager.getConnection(
                 "jdbc:default:connection");
        PreparedStatement ps = null;
        ResultSet rs = null;
        StringBuffer mysql = null;
        if (catalog.length() == 0) {
            mysql = new StringBuffer();
            mysql.append("select PARAM_VALUE from ");
            mysql.append("sys_boot.mgmt.session_parameters_view ");
            mysql.append("where PARAM_NAME ='catalogName'");
            ps = conn.prepareStatement(mysql.toString());
            rs = ps.executeQuery();
            while (rs.next()) {
                catalog = rs.getString(1);
            }
        }
        if (schema.length() == 0) {
            mysql = new StringBuffer();
            mysql.append("select PARAM_VALUE from ");
            mysql.append("sys_boot.mgmt.session_parameters_view ");
            mysql.append("where PARAM_NAME ='schemaName'");
            ps = conn.prepareStatement(mysql.toString());
            rs = ps.executeQuery();
            while (rs.next()) {
                schema = rs.getString(1);
            }
        }
        if (schema.length() == 0 || catalog.length() == 0) {
            throw new SQLException(
                "Error: Not found default catelog or schema.");
        }
        mysql = new StringBuffer();
        mysql.append("select \"url\" from ");
        mysql.append("sys_fem.sql2003.\"Jar\" as JT ");
        mysql.append("inner join ");
        mysql.append("sys_boot.MGMT.DBA_SCHEMAS_INTERNAL1 as ST ");
        mysql.append("on JT.\"namespace\" = ST.\"mofId\" ");
        mysql.append("where JT.\"name\"=? ");
        mysql.append("and ST.\"CATALOG_NAME\"=? and ST.\"SCHEMA_NAME\"=?");
        ps = conn.prepareStatement(mysql.toString());
        ps.setString(1, jarname);
        ps.setString(2, catalog);
        ps.setString(3, schema);

        rs = ps.executeQuery();

        while (rs.next()) {
            url = rs.getString(1);
        }
        return url;
    }

    /**
     * Refer to [jira]FRG-387
     * @param operation
     * @throws IOException
     */
    private static String[] getDeploySQLs(String jarUrl, String operation)
        throws IOException
    {
        String[] ret = null;
        System.out.println("jarUrl: " + jarUrl);
        URL url = new URL(jarUrl);
        JarFile jf = new JarFile(url.getFile());
        Manifest mf = jf.getManifest();
        if (mf != null) {
            Attributes ats = mf.getMainAttributes();
            String deplyFiles = ats.getValue("Name");
            String deployFlag = ats.getValue("SQLJDeploymentDescriptor").trim();
            System.out.println("deplyFiles:" + deplyFiles);
            System.out.println("deployFlag:" + deployFlag);
            if ("true".equalsIgnoreCase(deployFlag)) {
                JarEntry entry = jf.getJarEntry(deplyFiles);
                if (entry != null) {
                    InputStream in = jf.getInputStream(entry);
                    BufferedReader br = new BufferedReader(
                        new InputStreamReader(in));
                    String s = "";
                    StringBuffer sb = new StringBuffer();
                    boolean flag = false;
                    while ((s = br.readLine()) != null) {
                        if (s.contains("BEGIN " + operation)) {
                            flag = true;
                        } else if (s.contains("END " + operation) && flag) {
                            break;
                        } else if (flag) {
                            sb.append(s);
                        }
                    }
                    ret = sb.toString().split(";");
                }
            }
        } else {
            System.out.println("manfest file was not found.");
        }
        return ret;
    }
}

// End DdlSqlj.java
