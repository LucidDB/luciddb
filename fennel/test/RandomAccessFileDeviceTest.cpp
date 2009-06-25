/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
#include "fennel/common/FileSystem.h"
#include "fennel/test/TestBase.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/cache/VMAllocator.h"
#include <memory>
#include <fstream>
#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>

using namespace fennel;

class RandomAccessFileDeviceTest : virtual public TestBase
{
    static const uint ZERO_SIZE;
    static const uint HALF_SIZE;
    static const uint FULL_SIZE;

    DeviceAccessSchedulerParams schedParams;
    SharedRandomAccessDevice pRandomAccessDevice;
    DeviceMode baseMode;

    void openDevice(DeviceMode openMode, std::string devName)
    {
        if (openMode.create) {
            FileSystem::remove(devName.c_str());
        }
        pRandomAccessDevice.reset(
            new RandomAccessFileDevice(devName, openMode));
    }

    void closeDevice()
    {
        pRandomAccessDevice.reset();
    }

    void testDeviceCreation()
    {
        const char *devName = "test.dat";
        DeviceMode openMode = baseMode;
        openMode.create = 1;
        openDevice(openMode, devName);
        closeDevice();
        if (openMode.temporary) {
            if (FileSystem::doesFileExist(devName)) {
                std::cerr << "temporary test.dat not deleted" << std::endl;
            }
        } else {
            openMode.create = 0;
            openDevice(openMode, devName);
            closeDevice();
        }
    }

    void testGrow()
    {
        const char *devName = "grow.dat";
        DeviceMode openMode = baseMode;
        openMode.create = 1;
        openDevice(openMode, devName);
        BOOST_CHECK_EQUAL(ZERO_SIZE, pRandomAccessDevice->getSizeInBytes());
        pRandomAccessDevice->setSizeInBytes(FULL_SIZE);
        BOOST_CHECK_EQUAL(FULL_SIZE, pRandomAccessDevice->getSizeInBytes());
        closeDevice();
        if (openMode.temporary) {
            return;
        }
        openMode.create = 0;
        openDevice(openMode, devName);
        BOOST_CHECK_EQUAL(FULL_SIZE, pRandomAccessDevice->getSizeInBytes());
        closeDevice();
    }

    void testShrink()
    {
        const char *devName = "shrink.dat";
        DeviceMode openMode = baseMode;
        openMode.create = 1;
        openDevice(openMode, devName);
        BOOST_CHECK_EQUAL(ZERO_SIZE, pRandomAccessDevice->getSizeInBytes());
        pRandomAccessDevice->setSizeInBytes(FULL_SIZE);
        BOOST_CHECK_EQUAL(FULL_SIZE, pRandomAccessDevice->getSizeInBytes());
        closeDevice();
        if (openMode.temporary) {
            return;
        }
        openMode.create = 0;
        openDevice(openMode, devName);
        BOOST_CHECK_EQUAL(FULL_SIZE, pRandomAccessDevice->getSizeInBytes());
        pRandomAccessDevice->setSizeInBytes(HALF_SIZE);
        closeDevice();
        openDevice(openMode, devName);
        BOOST_CHECK_EQUAL(HALF_SIZE, pRandomAccessDevice->getSizeInBytes());
        closeDevice();
    }

    void testLargeFile()
    {
        // Create a 5G file in order to test beyond 32-bit unsigned.
        FileSize cbOffset = 0x40000000; // 1G
        cbOffset *= 5;
        testAsyncIO(cbOffset);
    }

    class Listener
    {
        StrictMutex mutex;
        LocalCondition cond;

    public:
        int nTarget;
        int nSuccess;
        int nCompleted;

        explicit Listener(int nTargetInit)
        {
            nTarget = nTargetInit;
            nSuccess = 0;
            nCompleted = 0;
        }
        virtual ~Listener()
        {
        }

        StrictMutex &getMutex()
        {
            return mutex;
        }

        void notifyTransferCompletion(bool b)
        {
            StrictMutexGuard mutexGuard(mutex);
            if (b) {
                nSuccess++;
            }
            nCompleted++;
            if (nCompleted == nTarget) {
                cond.notify_all();
            }
        }

        void waitForAll()
        {
            StrictMutexGuard mutexGuard(mutex);
            while (nCompleted < nTarget) {
                cond.wait(mutexGuard);
            }
        }
    };

    class Binding : public RandomAccessRequestBinding
    {
        Listener &listener;
        uint cb;
        PBuffer pBuffer;
    public:
        explicit Binding(
            Listener &listenerInit,uint cbInit,PBuffer pBufferInit)
            : listener(listenerInit), cb(cbInit), pBuffer(pBufferInit)
        {
        }

        virtual ~Binding()
        {
        }

        virtual PBuffer getBuffer() const
        {
            return pBuffer;
        }

        virtual uint getBufferSize() const
        {
            return cb;
        }

        virtual void notifyTransferCompletion(bool bSuccess)
        {
            listener.notifyTransferCompletion(bSuccess);
        }
    };

    void testAsyncIO()
    {
        testAsyncIO(0);
    }

    void testRetryAsyncIO()
    {
        testAsyncIO(0, 5000);
    }

    void testAsyncIO(FileSize cbOffset, int n = 5)
    {
        uint cbSector = HALF_SIZE;
        VMAllocator allocator(cbSector*n);
        void *pBuf = allocator.allocate();
        void *pBuf2 = allocator.allocate();
        BOOST_REQUIRE(pBuf != NULL);
        try {
            testAsyncIOImpl(
                n, cbSector,
                reinterpret_cast<PBuffer>(pBuf),
                reinterpret_cast<PBuffer>(pBuf2),
                cbOffset);
        } catch (...) {
            allocator.deallocate(pBuf);
            allocator.deallocate(pBuf2);
            throw;
        }
        allocator.deallocate(pBuf);
        allocator.deallocate(pBuf2);
    }

    void testAsyncIOImpl(
        int n, uint cbSector,
        PBuffer pBuf, PBuffer pBuf2, FileSize cbOffset = 0)
    {
        DeviceAccessScheduler *pScheduler =
            DeviceAccessScheduler::newScheduler(schedParams);

        const char *devName = "async.dat";
        DeviceMode openMode = baseMode;
        openMode.create = 1;
        openDevice(openMode, devName);
        FileSize cbFile = cbOffset;
        cbFile += n*cbSector;
        pRandomAccessDevice->setSizeInBytes(cbFile);

        // close and re-open to get the actual file size
        if (!openMode.temporary) {
            closeDevice();
            openMode.create = 0;
            openDevice(openMode, devName);
            FileSize cbFileActual = pRandomAccessDevice->getSizeInBytes();
            BOOST_CHECK_EQUAL(cbFile, cbFileActual);
        }

        pScheduler->registerDevice(pRandomAccessDevice);
        std::string s = "Four score and seven years ago.";
        char const *writeBuf = s.c_str();
        uint cb = s.size();

        Listener writeListener(n);
        RandomAccessRequest writeRequest;
        writeRequest.pDevice = pRandomAccessDevice.get();
        writeRequest.cbOffset = cbOffset;
        writeRequest.cbTransfer = n * cbSector;
        writeRequest.type = RandomAccessRequest::WRITE;
        memcpy(pBuf, writeBuf, cb);
        for (int i = 0; i < n; i++) {
            Binding *pBinding = new Binding(
                writeListener, cbSector, PBuffer(pBuf));
            writeRequest.bindingList.push_back(*pBinding);
        }

        // LER-7110: take a redundant mutex on the listener around the request
        // to confirm that attempts to notify the listener don't deadlock in
        // the case where the async I/O queue is full
        StrictMutexGuard mutexGuard(writeListener.getMutex());
        pScheduler->schedule(writeRequest);
        mutexGuard.unlock();

        writeListener.waitForAll();
        BOOST_CHECK_EQUAL(n, writeListener.nSuccess);
        pRandomAccessDevice->flush();

        if (!openMode.temporary) {
            pScheduler->unregisterDevice(pRandomAccessDevice);
            closeDevice();
            openMode.create = 0;
            openDevice(openMode, devName);
            pScheduler->registerDevice(pRandomAccessDevice);
        }

        Listener readListener(n + 1);
        RandomAccessRequest readRequest;
        readRequest.pDevice = pRandomAccessDevice.get();
        readRequest.cbOffset = cbOffset;
        readRequest.cbTransfer = n*cbSector;
        readRequest.type = RandomAccessRequest::READ;
        for (int i = 0; i < n; i++) {
            Binding *pBinding = new Binding(
                readListener, cbSector,
                pBuf + i * cbSector);
            readRequest.bindingList.push_back(*pBinding);
        }

        // Test a simultaneous read on the same device which intersects
        // with the reads above; this simulates something like an online
        // backup reading into private buffers, aliasing the cache.
        RandomAccessRequest readRequest2;
        readRequest2.pDevice = pRandomAccessDevice.get();
        readRequest2.cbOffset = cbOffset;
        readRequest2.cbTransfer = cbSector;
        readRequest2.type = RandomAccessRequest::READ;
        Binding *pBinding = new Binding(
            readListener, cbSector, pBuf2);
        readRequest2.bindingList.push_back(*pBinding);

        pScheduler->schedule(readRequest);
        pScheduler->schedule(readRequest2);
        readListener.waitForAll();
        BOOST_CHECK_EQUAL(n + 1, readListener.nSuccess);
        for (int i = 0; i < n; i++) {
            std::string s2(reinterpret_cast<char *>(pBuf + i*cbSector),cb);
            BOOST_CHECK_EQUAL(s, s2);
        }
        std::string s3(reinterpret_cast<char *>(pBuf2), cb);
        BOOST_CHECK_EQUAL(s, s3);

        pScheduler->unregisterDevice(pRandomAccessDevice);
        closeDevice();

        pScheduler->stop();
        delete pScheduler;
    }

public:
    explicit RandomAccessFileDeviceTest()
    {
        schedParams.readConfig(configMap);
        FENNEL_UNIT_TEST_CASE(
            RandomAccessFileDeviceTest, testPermanentNoDirect);
        FENNEL_UNIT_TEST_CASE(RandomAccessFileDeviceTest, testTemporary);
        FENNEL_UNIT_TEST_CASE(RandomAccessFileDeviceTest, testPermanentDirect);
        FENNEL_UNIT_TEST_CASE(RandomAccessFileDeviceTest, testRetryAsyncIO);

        // NOTE jvs 11-Feb-2006:  This is optional since it creates
        // a 5G file.  On operating systems with sparse-file support, it
        // doesn't actually take up that much disk space.
        FENNEL_EXTRA_UNIT_TEST_CASE(
            RandomAccessFileDeviceTest, testLargeFile);
    }

    void testPermanentNoDirect()
    {
        baseMode = DeviceMode::load;
        runModeTests();
    }

    void testTemporary()
    {
        baseMode = DeviceMode::load;
        baseMode.temporary = true;
        runModeTests();
    }

    void testPermanentDirect()
    {
        baseMode = DeviceMode::load;
        baseMode.direct = true;
        runModeTests();
    }

    void runModeTests()
    {
        testDeviceCreation();
        testGrow();
        testShrink();
        testAsyncIO();
    }

    virtual void testCaseTearDown()
    {
        closeDevice();
    }
};

// NOTE:  HALF_SIZE and FULL_SIZE have to be multiples of the disk sector size
// for direct I/O to work on Windows.
const uint RandomAccessFileDeviceTest::ZERO_SIZE = 0;
const uint RandomAccessFileDeviceTest::HALF_SIZE = 4096;
const uint RandomAccessFileDeviceTest::FULL_SIZE = 2*HALF_SIZE;

FENNEL_UNIT_TEST_SUITE(RandomAccessFileDeviceTest);

// End RandomAccessFileDeviceTest.cpp
