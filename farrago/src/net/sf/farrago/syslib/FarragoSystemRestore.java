/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.syslib;

import java.io.*;
import java.nio.channels.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import net.sf.farrago.resource.*;

/**
 * FarragoSystemRestore implements restore of the Farrago catalog and Fennel
 * data from a previously created backup.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FarragoSystemRestore
{
    //~ Instance fields --------------------------------------------------------
    
    private String archiveDirectory;
    
    Long dbDatSize = null;
    FarragoBackupType backupType = null;
    Long lowerBoundCsn = null;
    Long upperBoundCsn = null;
    Boolean isCompressed = null;

    //~ Constructors -----------------------------------------------------------

    public FarragoSystemRestore(String archiveDirectory)
    {
        this.archiveDirectory = archiveDirectory;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Restores the database from a backup.
     */
    public void restoreDatabase()
        throws Exception
    {
        FarragoSession session = FarragoUdrRuntime.getSession();
        FarragoRepos repos = session.getRepos();
        FarragoReposTxnContext reposTxnContext = 
            new FarragoReposTxnContext(repos, true);
        FennelExecutionHandle execHandle = null;
        
        try {
            execHandle = new FennelExecutionHandle();
            FarragoUdrRuntime.setExecutionHandle(execHandle);

            // Put the repository in exclusive access mode and then make sure
            // this is the only active session.
            reposTxnContext.beginExclusiveAccess();
            FarragoDatabase db = ((FarragoDbSession) session).getDatabase();
            List<FarragoSession> activeSessions = db.getSessions();
            if (activeSessions.size() > 1) {
                throw FarragoResource.instance().NeedExclusiveAccess.ex();
            }
        
            // Validate input
            archiveDirectory =
                FarragoBackupRestoreUtil.validateArchiveDirectory(
                    archiveDirectory,
                    false);
            readPropertyFile();           
            FarragoBackupRestoreUtil.checkBackupFiles(
                archiveDirectory,
                isCompressed,
                false);
        
            // Get information on the backups that have completed
            List<FarragoCatalogUtil.BackupData> backupData;
            try {
                reposTxnContext.beginReadTxn();
                backupData = FarragoCatalogUtil.getCurrentBackupData(repos); 
                reposTxnContext.commit();
            } finally {
                reposTxnContext.rollback();
            }
            
            // If we're restoring an incremental or differential backup,
            // then a full backup needs to have been executed
            if (backupData.size() == 0 && backupType != FarragoBackupType.FULL)
            {
                throw FarragoResource.instance().NoFullBackup.ex();
            }
           
            FennelDbHandle fennelDbHandle = db.getFennelDbHandle();
            FarragoRepos systemRepos = db.getSystemRepos();
        
            // Restore the data pages first so we can also verify if the backup
            // file is the correct one.
            FemCmdRestoreFromBackup cmd =
                systemRepos.newFemCmdRestoreFromBackup();
            cmd.setDbHandle(fennelDbHandle.getFemDbHandle(systemRepos));
            String dataDumpName = "FennelDataDump.dat";
            if (isCompressed.booleanValue()) {
                dataDumpName += ".gz";
            }
            File dataFile = new File(archiveDirectory, dataDumpName);
            cmd.setBackupPathname(dataFile.getAbsolutePath());
            cmd.setFileSize(dbDatSize);
            if (isCompressed.booleanValue()) {
                cmd.setCompressionProgram("gzip");
            } else {
                cmd.setCompressionProgram("");
            }
            
            if (backupType != FarragoBackupType.FULL) {
                // Add 1 to account for the checkpoint at the end of the
                // prior restore
                lowerBoundCsn++;
            }
            cmd.setLowerBoundCsn(lowerBoundCsn);
            cmd.setUpperBoundCsn(upperBoundCsn);
            fennelDbHandle.executeCmd(cmd, execHandle);
        
            // Copy the catalog dump from the backup to the catalog directory
            // and then set the flag needed to request a database shutdown
            // and metamodel dump.  On the next database restart, the
            // repository will be reloaded from these dumps.
            copyCatalogDump();
            ((FarragoDbSession) session).setMetamodelDump(true);
            
        } finally {
            FarragoUdrRuntime.setExecutionHandle(null);
            if (execHandle != null) {
                execHandle.delete();
            }
            reposTxnContext.endExclusiveAccess();
        }
    }
    
    private void readPropertyFile()
        throws Exception
    {
        BufferedReader reader =
            new BufferedReader(new FileReader(
                new File(archiveDirectory, "backup.properties")));
        try {
            String property = reader.readLine();
            while (property != null) {
                String [] parts = property.split("=", 2);
                if (parts[0].equals("db.dat.size")) {
                    dbDatSize = new Long(parts[1]);
                } else if (parts[0].equals("backup.type")) {
                    backupType =
                        FarragoBackupRestoreUtil.getBackupType(parts[1]);
                } else if (parts[0].equals("compression.mode")) {
                    isCompressed = 
                        FarragoBackupRestoreUtil.isCompressed(parts[1]);
                } else if (parts[0].equals("lower.bound.csn")) {
                    lowerBoundCsn = new Long(parts[1]);
                } else if (parts[0].equals("upper.bound.csn")) {
                    upperBoundCsn = new Long(parts[1]);
                } else {
                    throw FarragoResource.instance().
                        InvalidBackupPropertySetting.ex(parts[0]);
                }
                property = reader.readLine();
            }
        } finally {
            reader.close();
        }
        
        checkMissingProperty("db.dat.size", dbDatSize);
        checkMissingProperty("backup.type", backupType);
        checkMissingProperty("lower.bound.csn", lowerBoundCsn);
        checkMissingProperty("upper.bound.csn", upperBoundCsn);
        checkMissingProperty("compression.mode", isCompressed);
    }
    
    private void checkMissingProperty(String name, Object val)
    {
        if (val == null) {
            throw FarragoResource.instance().MissingPropertySetting.ex(name);
        }
    }
    
    private void copyCatalogDump()
        throws Exception
    {
        FarragoModelLoader modelLoader = 
            FarragoManagementUDR.getModelLoader();
        String dumpName = "FarragoCatalogDump.xmi";
        if (isCompressed.booleanValue()) {
            dumpName += ".gz";
        }
        
        FileChannel dest = null;
        FileChannel src = null;
        try {
            File copiedDump =
                new File(
                    modelLoader.getFarragoProperties().getCatalogDir(),
                    dumpName);
            dest = new FileOutputStream(copiedDump).getChannel();
            src =
                new FileInputStream(
                    new File(archiveDirectory, dumpName)).getChannel();
            src.transferTo(0, src.size(), dest);
        } finally {
            if (dest != null) {
                dest.close();
            }
            if (src != null) {
                src.close();
            }
        }
    }
}

// End FarragoSystemRestore.java
