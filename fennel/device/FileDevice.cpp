/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
#include "fennel/device/FileDevice.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/common/SysCallExcn.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>
#include <sstream>

#ifdef __MINGW32__
#include <windows.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

FileDevice::FileDevice(
    std::string filenameInit,DeviceMode openMode,FileSize initialSize)
{
    filename = filenameInit;
    mode = openMode;

#ifdef __MINGW32__

    DWORD fdwCreate = mode.create ? CREATE_ALWAYS : OPEN_EXISTING;

    DWORD fdwFlags = FILE_FLAG_OVERLAPPED;

    DWORD fdwAccess = GENERIC_READ;
    if (!mode.readOnly) {
        fdwAccess |= GENERIC_WRITE;
    }
    if (mode.direct) {
        fdwFlags |= FILE_FLAG_NO_BUFFERING;
    }
    if (mode.sequential) {
        fdwFlags |= FILE_FLAG_SEQUENTIAL_SCAN;
    } else {
        fdwFlags |= FILE_FLAG_RANDOM_ACCESS;
    }
    if (mode.temporary) {
        fdwFlags |= FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE;
    } else {
        fdwFlags |= FILE_ATTRIBUTE_NORMAL;
    }

    // REVIEW:  I used FILE_SHARE_ so that recovery tests could reopen a
    // log file for read while it was still open for write by the original
    // txn.  Should probably fix the tests instead, in case allowing sharing
    // could hinder performance.

    handle = reinterpret_cast<int>(
        CreateFile(
            filename.c_str(),
            fdwAccess,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            NULL,
            fdwCreate,
            fdwFlags,
            NULL));

    if (!isOpen()) {
        std::ostringstream oss;
        oss << "Failed to open file " << filename;
        throw SysCallExcn(oss.str());
    }

    DWORD cbHigh = 0;
    DWORD cbLow = GetFileSize(HANDLE(handle),&cbHigh);
    if (cbLow == INVALID_FILE_SIZE) {
        std::ostringstream oss;
        oss << "Failed to get size for file " << filename;
        throw SysCallExcn(oss.str());
    }
    LARGE_INTEGER cbLarge;
    cbLarge.LowPart = cbLow;
    cbLarge.HighPart = cbHigh;
    cbFile = cbLarge.QuadPart;
    if (mode.create && initialSize > 0) {
        setSizeInBytes(initialSize);
    }

#else

    int access = O_LARGEFILE;
    int permission = S_IRUSR;
    if (mode.readOnly) {
        access |= O_RDONLY;
    } else {
        access |= O_RDWR;
        permission |= S_IWUSR;
    }
    if (mode.create) {
        access |= O_CREAT | O_TRUNC;
    }

    if (mode.direct) {
        // REVIEW jvs 4-Dec-2008: Comment below used to be true, but probably
        // only on 2.4 kernels.  2.6 kernels seem to be happy with
        // O_DIRECT+pwrite.
        // (http://lkml.indiana.edu/hypermail/linux/kernel/0511.2/1758.html).
        // So we can probably clean this up now.
        access |= O_SYNC;
        // NOTE:  We don't actually set O_DIRECT here, because on Linux
        // that results in EINVAL errors from pwrite.  Instead,
        // O_DIRECT is set from AioLinuxScheduler, because it is required
        // for libaio.
    }

    handle = ::open(filename.c_str(), access, permission);
    if (!isOpen()) {
        std::ostringstream oss;
        oss << "Failed to open file " << filename;
        throw SysCallExcn(oss.str());
    }
    if (flock(handle, LOCK_SH | LOCK_NB) < 0) {
        throw SysCallExcn("File lock failed");
    }
    cbFile = ::lseek(handle,0,SEEK_END);

    // Preallocate the file if we're creating the file, and an initial size
    // is specified.
    if (mode.create && initialSize > 0) {
        int rc = posix_fallocate(handle, 0, initialSize);
        if (rc) {
            throw SysCallExcn("File allocation failed", rc);
        }
        cbFile = initialSize;
    }

#endif
}

FileDevice::~FileDevice()
{
    if (isOpen()) {
        close();
    }
}

void FileDevice::close()
{
    assert(isOpen());
#ifdef __MINGW32__
    CloseHandle(HANDLE(handle));
#else
    ::close(handle);
    if (mode.temporary) {
        ::unlink(filename.c_str());
    }
#endif
    handle = -1;
}

void FileDevice::flush()
{
    if (mode.readOnly) {
        return;
    }
#ifdef __MINGW32__
    if (!FlushFileBuffers(HANDLE(handle))) {
        throw SysCallExcn("Flush failed");
    }
#else
    if (::fdatasync(handle)) {
        throw SysCallExcn("Flush failed");
    }
#endif
}

void FileDevice::setSizeInBytes(FileSize cbFileNew)
{
#ifdef __MINGW32__
    LARGE_INTEGER cbLarge;
    cbLarge.QuadPart = cbFileNew;
    if (!SetFilePointerEx(HANDLE(handle),cbLarge,NULL,FILE_BEGIN)) {
        throw SysCallExcn("Resize file failed:  SetFilePointer");
    }
    if (!SetEndOfFile(HANDLE(handle))) {
        throw SysCallExcn("Resize file failed:  SetEndOfFile");
    }
#else
    if (::ftruncate(handle,cbFileNew)) {
        throw SysCallExcn("Resize file failed");
    }
#endif
    cbFile = cbFileNew;
}

void FileDevice::transfer(RandomAccessRequest const &request)
{
    FileSize cbActual;
    assert(request.bindingList.size() == 1);
#ifdef __MINGW32__
    LARGE_INTEGER largeInt;
    RandomAccessRequestBinding &binding = request.bindingList.front();
    largeInt.QuadPart = request.cbOffset;
    binding.Offset = largeInt.LowPart;
    binding.OffsetHigh = largeInt.HighPart;

    DWORD dwActual = 0;
    BOOL bCompleted;
    if (request.type == RandomAccessRequest::READ) {
        bCompleted = ReadFile(
            HANDLE(handle),
            request.bindingList.front().getBuffer(),
            request.cbTransfer,
            &dwActual,
            &binding);
    } else {
        bCompleted = WriteFile(
            HANDLE(handle),
            request.bindingList.front().getBuffer(),
            request.cbTransfer,
            &dwActual,
            &binding);
    }
    if (!bCompleted) {
        if (GetLastError() == ERROR_IO_PENDING) {
            if (!GetOverlappedResult(
                    HANDLE(handle),
                    &binding,
                    &dwActual,
                    TRUE))
            {
                dwActual = 0;
            }
        } else {
            dwActual = 0;
        }
    }
    cbActual = dwActual;
#elif defined(__CYGWIN__)
    StrictMutexGuard guard(mutex);
    ::lseek(handle, request.cbOffset, SEEK_SET);
    if (request.type == RandomAccessRequest::READ) {
        cbActual = ::read(
            handle,
            request.bindingList.front().getBuffer(),
            request.cbTransfer);
    } else {
        cbActual = ::write(
            handle,
            request.bindingList.front().getBuffer(),
            request.cbTransfer);
    }
    guard.unlock();
#else
    if (request.type == RandomAccessRequest::READ) {
        cbActual = ::pread(
            handle,
            request.bindingList.front().getBuffer(),
            request.cbTransfer,
            request.cbOffset);
    } else {
        cbActual = ::pwrite(
            handle,
            request.bindingList.front().getBuffer(),
            request.cbTransfer,
            request.cbOffset);
    }
#endif
    request.bindingList.front().notifyTransferCompletion(
        cbActual == request.cbTransfer);
}

FENNEL_END_CPPFILE("$Id$");

// End FileDevice.cpp
