/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import java.util.*;
import java.util.logging.*;
import java.util.zip.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;

import org.eigenbase.trace.*;


/**
 * FarragoSystemBackup implements hot backup of the Farrago catalog and Fennel
 * data, provided Fennel supports versioning. Full, incremental, and
 * differential backups are supported, and the backup files can be optionally
 * compressed.
 *
 * @author Zelaine Fong
 * @version $Id$
 */

public class FarragoSystemBackup
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getSyslibTracer();

    //~ Instance fields --------------------------------------------------------

    private String archiveDirectory;
    private String backupType;
    private String compressionMode;
    private boolean checkSpace;
    private long padding;

    private FarragoReposTxnContext reposTxnContext;
    private FarragoRepos repos;
    private FarragoBackupType type;
    private FarragoRepos systemRepos;
    private FennelDbHandle fennelDbHandle;
    private boolean isCompressed;
    Long lowerBoundCsn = null;
    Long upperBoundCsn = null;
    Long dbSize = null;

    private EigenbaseTimingTracer timingTracer;

    //~ Constructors -----------------------------------------------------------

    public FarragoSystemBackup(
        String archiveDirectory,
        String backupType,
        String compressionMode,
        boolean checkSpace,
        long padding)
    {
        this.archiveDirectory = archiveDirectory;
        this.backupType = backupType;
        this.compressionMode = compressionMode;
        this.checkSpace = checkSpace;
        this.padding = padding;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Backs up the database.
     */
    public void backupDatabase()
        throws Exception
    {
        timingTracer = new EigenbaseTimingTracer(tracer, "backup: begin");

        // Validate the input parameters
        archiveDirectory =
            FarragoBackupRestoreUtil.validateArchiveDirectory(
                archiveDirectory,
                true);
        type = FarragoBackupRestoreUtil.getBackupType(backupType);
        isCompressed = FarragoBackupRestoreUtil.isCompressed(compressionMode);

        FarragoSession session = FarragoUdrRuntime.getSession();
        repos = session.getRepos();
        reposTxnContext = new FarragoReposTxnContext(repos, true);
        FarragoDatabase db = ((FarragoDbSession) session).getDatabase();

        // Can't backup if the current session has a label setting
        if (((FarragoDbSession) session).getSessionLabelCsn() != null) {
            throw FarragoResource.instance().ReadOnlySession.ex();
        }

        // Make sure there are no other backups in progress
        if (!db.setBackupFlag(true)) {
            throw FarragoResource.instance().BackupInProgress.ex();
        }

        boolean abandonBackup = false;
        FennelExecutionHandle execHandle = null;
        systemRepos = db.getSystemRepos();
        fennelDbHandle = db.getFennelDbHandle();
        try {
            execHandle = new FennelExecutionHandle();
            FarragoUdrRuntime.setExecutionHandle(execHandle);

            FarragoBackupRestoreUtil.checkBackupFiles(
                archiveDirectory,
                isCompressed,
                true);

            timingTracer.traceTime("backup: checkBackupFiles");

            // Execute the first part of the backup
            abandonBackup = true;
            initiateBackup(execHandle);

            // Backup data pages
            boolean backupSucceeded = false;
            abandonBackup = false;
            try {
                FemCmdCompleteBackup cmd2 =
                    systemRepos.newFemCmdCompleteBackup();
                cmd2.setDbHandle(fennelDbHandle.getFemDbHandle(systemRepos));
                cmd2.setLowerBoundCsn(lowerBoundCsn);
                cmd2.setUpperBoundCsn(upperBoundCsn);
                fennelDbHandle.executeCmd(cmd2, execHandle);
                backupSucceeded = true;
            } finally {
                db.cleanupBackupData(backupSucceeded, true);
            }

            timingTracer.traceTime("backup: femCmdCompleteBackup");

            writeBackupPropertyFile(
                archiveDirectory,
                backupType,
                compressionMode,
                dbSize.toString(),
                lowerBoundCsn.toString(),
                upperBoundCsn.toString());
        } finally {
            FarragoUdrRuntime.setExecutionHandle(null);
            if (execHandle != null) {
                execHandle.delete();
            }
            if (abandonBackup) {
                // Tell Fennel that the backup wasn't completed
                FemCmdAbandonBackup cmd = systemRepos.newFemCmdAbandonBackup();
                cmd.setDbHandle(fennelDbHandle.getFemDbHandle(systemRepos));
                fennelDbHandle.executeCmd(cmd);
            }
            db.setBackupFlag(false);
        }

        timingTracer.traceTime("backup: finished");
    }

    /**
     * Executes the initial portion of the backup, which includes backing up
     * Fennel metadata pages and the Farrago catalog. During this portion of the
     * backup, the catalog is locked.
     *
     * @param execHandle execution handle to associate with the Fennel command
     */
    private void initiateBackup(FennelExecutionHandle execHandle)
        throws Exception
    {
        try {
            // Acquire a write lock on the catalog, since we will be
            // updating the catalog below
            reposTxnContext.beginLockedTxn(false);

            List<FarragoCatalogUtil.BackupData> backupData =
                FarragoCatalogUtil.getCurrentBackupData(repos);

            // If this is not a full backup, make sure a backup has already
            // been done.
            if ((type != FarragoBackupType.FULL) && (backupData.size() == 0)) {
                throw FarragoResource.instance().NoFullBackup.ex();
            }

            String startTime = FarragoCatalogUtil.createTimestamp().toString();

            // Backup the database header and page allocation metadata pages.
            // The Fennel command that does this also retrieves the commit
            // sequence number associated with the start of the backup
            // and the size of db.dat.
            FemCmdInitiateBackup cmd1 = systemRepos.newFemCmdInitiateBackup();
            cmd1.setDbHandle(fennelDbHandle.getFemDbHandle(systemRepos));
            String dataDumpName = "FennelDataDump.dat";
            if (isCompressed) {
                dataDumpName += ".gz";
            }
            File dataFile = new File(archiveDirectory, dataDumpName);
            lowerBoundCsn = getLowerBoundCsn(type, backupData);
            cmd1.setBackupPathname(dataFile.getAbsolutePath());
            cmd1.setCheckSpaceRequirements(checkSpace);
            cmd1.setSpacePadding(padding);
            cmd1.setLowerBoundCsn(lowerBoundCsn);
            if (isCompressed) {
                cmd1.setCompressionProgram("gzip");
            } else {
                cmd1.setCompressionProgram("");
            }
            fennelDbHandle.executeCmd(cmd1, execHandle);
            upperBoundCsn = cmd1.getResultHandle().getLongHandle();
            dbSize = cmd1.getResultDataDeviceSize();

            // Add records to the catalog, indicating a pending backup.
            // The records will be converted to a completed backup or
            // deleted when the backup completes.  If a crash occurs
            // before the backup completes, then the records will be cleaned
            // up after the database is recovered.
            FarragoCatalogUtil.addPendingSystemBackup(
                repos,
                backupType,
                upperBoundCsn,
                startTime);

            timingTracer.traceTime("backup: addPendingSystemBackup");

            // Export the catalog
            File export =
                FarragoBackupRestoreUtil.getCatalogBackupFile(
                    archiveDirectory,
                    isCompressed);
            OutputStream exportStream = new FileOutputStream(export);
            try {
                if (isCompressed) {
                    exportStream = new GZIPOutputStream(exportStream);
                }

                repos.getEnkiMdrRepos().backupExtent(
                    FarragoReposUtil.FARRAGO_CATALOG_EXTENT,
                    exportStream);
            } finally {
                exportStream.close();
            }

            reposTxnContext.commit();
        } finally {
            reposTxnContext.rollback();

            // Unlock the catalog
            reposTxnContext.unlockAfterTxn();

            timingTracer.traceTime("backup: backupExtent");
        }
    }

    private void writeBackupPropertyFile(
        String archiveDirectory,
        String backupType,
        String compressionMode,
        String dbSize,
        String lowerBoundCsn,
        String upperBoundCsn)
        throws Exception
    {
        // Write out the backup properties
        BufferedWriter writer =
            new BufferedWriter(
                new FileWriter(
                    new File(archiveDirectory, "backup.properties")));

        try {
            // The type of backup
            writer.write("backup.type=");
            writer.write(backupType);
            writer.newLine();

            // Whether the backup is compressed
            writer.write("compression.mode=");
            writer.write(compressionMode);
            writer.newLine();

            // The size of db.dat
            writer.write("db.dat.size=");
            writer.write(dbSize);
            writer.newLine();

            // The commit sequence numbers
            writer.write("lower.bound.csn=");
            writer.write(lowerBoundCsn);
            writer.newLine();
            writer.write("upper.bound.csn=");
            writer.write(upperBoundCsn);
            writer.newLine();
        } finally {
            writer.close();
        }
    }

    /**
     * Determines the lower bound csn for the current backup based on the type
     * of the backup and previous backup data.
     *
     * @param backupType the type of the current backup
     * @param backupData information on backups completed
     *
     * @return the lower bound csn of the current backup
     */
    private long getLowerBoundCsn(
        FarragoBackupType backupType,
        List<FarragoCatalogUtil.BackupData> backupData)
    {
        long lowerBoundCsn;

        if (backupType == FarragoBackupType.FULL) {
            lowerBoundCsn = -1;
        } else if (backupType == FarragoBackupType.INCREMENTAL) {
            lowerBoundCsn = backupData.get(1).csn;
        } else {
            lowerBoundCsn = backupData.get(0).csn;
        }
        return lowerBoundCsn;
    }
}

// End FarragoSystemBackup.java
