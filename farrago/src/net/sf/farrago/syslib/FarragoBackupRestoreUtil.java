/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;


/**
 * FarragoBackupRestoreUtil contains utility methods used by backup and restore.
 *
 * @author Zelaine Fong
 * @version $Id$
 */

public abstract class FarragoBackupRestoreUtil
{
    //~ Static fields/initializers ---------------------------------------------

    private static final String CATALOG_BACKUP_FILENAME = "FarragoCatalogDump";

    //~ Methods ----------------------------------------------------------------

    /**
     * Validates the archive directory, expanding property names within the
     * name, as needed.
     *
     * @param directory the full pathname of the archive directory
     * @param isBackup true if this is a backup, as opposed to a restore
     *
     * @return the archive directory name with properties expanded
     */
    public static String validateArchiveDirectory(
        String directory,
        boolean isBackup)
    {
        directory = FarragoProperties.instance().expandProperties(directory);
        File fileDir = new File(directory);
        if (!fileDir.exists()) {
            throw FarragoResource.instance().InvalidDirectory.ex(directory);
        }
        if (isBackup && !fileDir.canWrite()) {
            throw FarragoResource.instance().BackupArchiveDirNotWritable.ex(
                directory);
        } else if (!isBackup && !fileDir.canRead()) {
            throw FarragoResource.instance().BackupArchiveDirNotReadable.ex(
                directory);
        }
        return directory;
    }

    /**
     * Translates a string representing a backup type (full, incremental, or
     * differential) into a symbolic value. Throws an exception if an invalid
     * type is passed in.
     *
     * @param backupType string value of the backup type
     *
     * @return the symbolic value of the backup type
     */
    public static FarragoBackupType getBackupType(String backupType)
    {
        if (backupType.equals("FULL")) {
            return FarragoBackupType.FULL;
        } else if (backupType.equals("INCREMENTAL")) {
            return FarragoBackupType.INCREMENTAL;
        } else if (backupType.equals("DIFFERENTIAL")) {
            return FarragoBackupType.DIFFERENTIAL;
        } else {
            throw FarragoResource.instance().InvalidBackupType.ex(backupType);
        }
    }

    /**
     * Verifies the existence or non-existence of files in the archive
     * directory.
     *
     * @param archiveDirectory the name of the archive directory
     * @param isCompressed whether the backup is compressed
     * @param isBackup true if the files are going to be used for a backup, as
     * opposed to a restore
     */
    public static void checkBackupFiles(
        String archiveDirectory,
        boolean isCompressed,
        boolean isBackup)
        throws Exception
    {
        checkBackupFile(archiveDirectory, "backup.properties", isBackup);
        checkBackupFile(
            archiveDirectory,
            isCompressed ? (CATALOG_BACKUP_FILENAME + ".gz")
            : CATALOG_BACKUP_FILENAME,
            isBackup);
        checkBackupFile(
            archiveDirectory,
            isCompressed ? "FennelDataDump.dat.gz" : "FennelDataDump.dat",
            isBackup);
    }

    private static void checkBackupFile(
        String archiveDirectory,
        String filename,
        boolean isBackup)
        throws Exception
    {
        File file = new File(archiveDirectory, filename);
        boolean exists = file.exists();
        if (isBackup) {
            if (exists) {
                throw FarragoResource.instance().BackupFileAlreadyExists.ex(
                    filename);
            }
        } else {
            if (!exists) {
                throw FarragoResource.instance().BackupFileDoesNotExist.ex(
                    filename);
            } else if (!file.canRead()) {
                throw FarragoResource.instance().BackupFileNotReadable.ex(
                    filename);
            }
        }
    }

    /**
     * Validates a string representing the compression mode of a backup. Throws
     * an exception for an invalid mode.
     *
     * @param compressionMode string value indicating the compression mode
     *
     * @return true if the string indicates compression
     */
    public static boolean isCompressed(String compressionMode)
    {
        if (compressionMode.equals("COMPRESSED")) {
            return true;
        } else if (compressionMode.equals("UNCOMPRESSED")) {
            return false;
        } else {
            throw FarragoResource.instance().InvalidCompressionMode.ex(
                compressionMode);
        }
    }

    /**
     * Returns the catalog backup file name relative to the given archive
     * directory.
     *
     * @param archiveDir archive directory
     * @param isCompressed whether compression is enabled
     */
    public static File getCatalogBackupFile(
        String archiveDir,
        boolean isCompressed)
    {
        String dumpName = CATALOG_BACKUP_FILENAME;
        if (isCompressed) {
            dumpName += ".gz";
        }
        return new File(archiveDir, dumpName);
    }
}

// End FarragoBackupRestoreUtil.java
