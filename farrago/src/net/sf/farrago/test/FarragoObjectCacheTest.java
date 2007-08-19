/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
package net.sf.farrago.test;

import net.sf.farrago.util.*;

import junit.framework.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import org.eigenbase.util.*;

/**
 * <code>FarragoObjectCacheTest</code> is a unit test for {@link
 * FarragoObjectCache}.
 *
 *<p>
 *
 * Premise: inner class RentalCarAgency maintains a fleet of rental cars and
 * assigns them to customers as requested.  The agency is lucky enough to be
 * able to manufacture new cars on demand!  (It uses a private factory to do
 * this.)  However, it has a fixed number of tires it can outfit them with (due
 * to a terrible rubber tree blight).  So, it sometimes has to destroy old cars
 * to salvage their tires.  (For real usage patterns, read "object memory
 * usage" for "tires".)
 *
 *<p>
 *
 * TODO:
 *
 *<ul>
 *<li>test setMaxBytes
 *<li>test explicit discard
 *<li>test that discardAll forces new creations subsequently
 *<li>test with non-uniform numbers of tires
 *<li>test other victimization policies once they exist
 *</ul>
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoObjectCacheTest extends TestCase
{
    /**
     * Default cache size limit.
     */
    static final int MAX_TIRES = 1000;

    /**
     * Mileage limit after which a car should be discarded.
     */
    static final int MAX_MILEAGE = 10000;

    RentalCarAgency agency;

    AtomicInteger nCarsCreated;

    AtomicInteger nCarsDestroyed;

    /**
     * Creates a new FarragoObjectCacheTest object.
     */
    public FarragoObjectCacheTest(String testName)
        throws Exception
    {
        super(testName);

        nCarsCreated = new AtomicInteger();
        nCarsDestroyed = new AtomicInteger();
    }

    public void tearDown()
    {
        if (agency != null) {
            agency.decommissionEntireFleet();
            assertEquals(0, agency.getTiresInFleet());
            agency.shutDown();
            agency = null;
        }

        // First law of something-or-other.
        assertEquals(nCarsCreated.get(), nCarsDestroyed.get());
    }

    /**
     * Tests a scenario where two cars are rented at the same time.
     * A single thread acts as customer for both cars.
     */
    public void testOneThreadOverlappingExclusive()
    {
        // Start a new agency which prohibits car sharing.
        agency = new RentalCarAgency(true, MAX_TIRES);
        assertEquals(0, agency.getTiresInFleet());
        assertEquals(0, nCarsCreated.get());
        assertEquals(0, nCarsDestroyed.get());

        // Rent out a car.
        String description = "Economy 4-door";
        RentalCarAgreement a1 = agency.rentCar(description);
        long tiresAfterRent1 = agency.getTiresInFleet();
        assertTrue(tiresAfterRent1 > 0);
        RentalCar car1 = a1.getCar();
        assertEquals(description, car1.getDescription());
        assertEquals(0, car1.getMileage());
        assertEquals(1, nCarsCreated.get());

        // Rent out a second car of the same description; cars
        // can't be shared, so we should get back a different one.
        RentalCarAgreement a2 = agency.rentCar(description);
        assertNotSame(a1, a2);
        RentalCar car2 = a2.getCar();
        assertNotSame(car1, car2);
        assertEquals(0, car2.getMileage());
        assertEquals(description, car2.getDescription());
        assertEquals(2, nCarsCreated.get());

        // Since the first car can't be shared, second rental should
        // have increased the fleet size (measured in tires).
        long tiresAfterRent2 = agency.getTiresInFleet();
        assertTrue(tiresAfterRent2 > tiresAfterRent1);

        // Rubber hits the road.
        car1.drive(100);
        car2.drive(200);
        car1.drive(200);
        assertEquals(300, car1.getMileage());
        assertEquals(200, car2.getMileage());

        // Return the first car.
        agency.returnCar(a1);

        // Returning the car does not destroy it (it just puts
        // it back idle into the fleet), so the number of tires in
        // the fleet should remain at the highwater mark.
        long tiresAfterReturn1 = agency.getTiresInFleet();
        assertEquals(tiresAfterRent2, tiresAfterReturn1);
        assertEquals(0, nCarsDestroyed.get());

        // Return the second car.
        agency.returnCar(a2);
        assertEquals(0, nCarsDestroyed.get());

        // Let tearDown take care of cleanup verification.
    }

    /**
     * Tests a scenario where a single car is rented, returned, and then rented
     * and returned again.
     */
    public void testOneThreadSequential()
    {
        runOneThreadSequential("Sport 2-door", 300, false);
    }

    /**
     * Tests a scenario where a car is rented, driven a long way, and then
     * returned, so that a subsequent rental request forces creation
     * of a new car (discarding the old one).
     */
    public void testOneThreadSequentialStale()
    {
        runOneThreadSequential("Sport 2-door", 10*MAX_MILEAGE, true);
    }

    /**
     * Tests a scenario where a car is rented, smoked in, and then
     * returned, so that a subsequent rental request forces creation
     * of a new car (discarding the old one).
     */
    public void testOneThreadSequentialNonReusable()
    {
        runOneThreadSequential("Sport 2-door (Smoking)", 300, true);
    }

    private void runOneThreadSequential(
        String description,
        int milesToDrive,
        boolean expectNew)
    {
        // Start a new agency which prohibits car sharing.
        agency = new RentalCarAgency(true, MAX_TIRES);
        assertEquals(0, agency.getTiresInFleet());
        assertEquals(0, nCarsCreated.get());
        assertEquals(0, nCarsDestroyed.get());

        // Rent out a car.
        RentalCarAgreement a1 = agency.rentCar(description);
        long tiresAfterRent1 = agency.getTiresInFleet();
        assertTrue(tiresAfterRent1 > 0);
        RentalCar car1 = a1.getCar();
        assertEquals(description, car1.getDescription());
        assertEquals(0, car1.getMileage());
        assertEquals(1, nCarsCreated.get());

        // Rubber hits the road.
        car1.drive(milesToDrive);
        assertEquals(milesToDrive, car1.getMileage());

        // Return the first car.
        agency.returnCar(a1);

        // Rent out a second car of the same description; whether
        // or not we get back the same one depends on how far the
        // first drive was
        RentalCarAgreement a2 = agency.rentCar(description);
        assertNotSame(a1, a2);
        RentalCar car2 = a2.getCar();
        if (expectNew) {
            assertNotSame(car1, car2);
            assertEquals(0, car2.getMileage());
            assertEquals(2, nCarsCreated.get());
            assertEquals(1, nCarsDestroyed.get());
        } else {
            assertSame(car1, car2);
            assertEquals(300, car2.getMileage());
            assertEquals(1, nCarsCreated.get());
        }
        assertEquals(description, car2.getDescription());

        // Regardless of reuse, second rental should not have increased the
        // fleet size (measured in tires).
        long tiresAfterRent2 = agency.getTiresInFleet();
        assertEquals(tiresAfterRent1, tiresAfterRent2);

        // Rubber hits the road again.
        car2.drive(200);
        if (expectNew) {
            assertEquals(200, car2.getMileage());
        } else {
            assertEquals(500, car2.getMileage());
        }

        // Return the car again.
        agency.returnCar(a2);
        if (expectNew) {
            assertEquals(1, nCarsDestroyed.get());
        } else {
            assertEquals(0, nCarsDestroyed.get());
        }

        // Let tearDown take care of cleanup verification.
    }

    /**
     * Tests a scenario where the same car is rented by two customers
     * at the same time.  A single thread acts as both customers.
     */
    public void testOneThreadShared()
    {
        // Start a new agency which allows car sharing.
        agency = new RentalCarAgency(false, MAX_TIRES);
        assertEquals(0, agency.getTiresInFleet());
        assertEquals(0, nCarsCreated.get());
        assertEquals(0, nCarsDestroyed.get());

        // Rent out a car.
        String description = "Minivan 6-door";
        RentalCarAgreement a1 = agency.rentCar(description);
        long tiresAfterRent1 = agency.getTiresInFleet();
        assertTrue(tiresAfterRent1 > 0);
        RentalCar car1 = a1.getCar();
        assertEquals(description, car1.getDescription());
        assertEquals(0, car1.getMileage());
        assertEquals(1, nCarsCreated.get());

        // Rent out a second car of the same description; cars
        // can be shared, so we should get back the same one.
        RentalCarAgreement a2 = agency.rentCar(description);
        assertNotSame(a1, a2);
        RentalCar car2 = a2.getCar();
        assertSame(car1, car2);
        assertEquals(0, car2.getMileage());
        assertEquals(description, car2.getDescription());
        assertEquals(1, nCarsCreated.get());

        // Since the first car was shared, second rental should not
        // have increased the fleet size (measured in tires).
        long tiresAfterRent2 = agency.getTiresInFleet();
        assertEquals(tiresAfterRent1, tiresAfterRent2);

        // Rubber hits the road.
        car1.drive(100);
        car2.drive(200);
        car1.drive(200);
        assertEquals(500, car1.getMileage());

        // Return the car (first customer).  This does not actually
        // return it to the fleet since the second customer hasn't
        // finished with it yet.
        agency.returnCar(a1);
        assertEquals(0, nCarsDestroyed.get());

        // The car is still rented out to the second customer.
        long tiresAfterReturn1 = agency.getTiresInFleet();
        assertEquals(tiresAfterRent2, tiresAfterReturn1);

        // Return the car (second customer).  This actually
        // returns it to the fleet (but does not destroy it).
        agency.returnCar(a2);
        long tiresAfterReturn2 = agency.getTiresInFleet();
        assertEquals(tiresAfterRent2, tiresAfterReturn2);
        assertEquals(0, nCarsDestroyed.get());

        // Let tearDown take care of cleanup verification.
    }

    /**
     * Tests a scenario where new cars are constantly requested
     * and then returned, causing old cars to be recycled for
     * their tires.
     */
    public void testOneThreadVictimization()
    {
        // Start a new agency which prohibits car sharing.
        agency = new RentalCarAgency(true, MAX_TIRES);
        long tiresPrev = agency.getTiresInFleet();
        assertEquals(0, tiresPrev);

        // Repeatedly request cars with different descriptions, forcing
        // the factory to keep churning out new ones.  Eventually,
        // this should lead to old ones being destroyed in order to recycle
        // their tires.  Since each car has more than one tire, if we repeat
        // for MAX_TIRES iterations, we should definitely force recycling
        // to take place.
        for (int i = 0; i < MAX_TIRES; ++i) {
            // Dynamically construct the description based on the iteration.
            String description = "Model-T" + i;
            RentalCarAgreement a1 = agency.rentCar(description);
            long tiresCurrent = agency.getTiresInFleet();

            // Should never exceed the limit.
            assertTrue(tiresCurrent <= MAX_TIRES);

            // Should never reuse previous car, since description is always
            // new.
            RentalCar car1 = a1.getCar();
            assertEquals(0, car1.getMileage());
            if (i < 10) {
                // For first ten cars, verify that tire count in fleet
                // keeps increasing, since we shouldn't have hit
                // tire recycling limit yet.
                assertTrue(tiresCurrent > tiresPrev);
            } else if (i > MAX_TIRES - 10) {
                // For last ten cars, verify that tire count in fleet
                // has hit steady-state, since we've hit the recycling limit,
                // and the number of tires per car is fixed.
                assertEquals(tiresPrev, tiresCurrent);
            }

            // Take it for a spin.
            car1.drive(100);

            // Return it.
            agency.returnCar(a1);

            // Remember tire count for next iteration.
            tiresPrev = tiresCurrent;
        }

        // Should end up at limit (no slack).
        assertEquals(MAX_TIRES, tiresPrev);

        // Let tearDown take care of cleanup verification.
    }


    /**
     * Tests a scenario where an exception is thrown during
     * initialization.
     */
    public void testOneThreadFailedInitialization()
    {
        // Start a new agency which prohibits car sharing.
        agency = new RentalCarAgency(true, MAX_TIRES);

        // Attempt to rent out a car.
        String description = "Lemon-yellow Caddy";
        try {
            RentalCarAgreement a1 = agency.rentCar(description);
            fail("Expected a lemon but got success instead");
        } catch (LemonException ex) {
            // Expected case.
        } catch (Throwable t) {
            fail("Expected a lemon but got something else instead");
        }
    }

    /**
     * Tests a multi-threaded scenario with objects pinned exclusively.
     */
    public void testMultipleThreadsExclusive()
    {
        runMultipleThreads(true);
    }

    /**
     * Tests a multi-threaded scenario with objects pinned as shared.
     */
    public void testMultipleThreadsShared()
    {
        runMultipleThreads(false);
    }

    private void runMultipleThreads(boolean exclusive)
    {
        // Start a new agency with specified sharing mode.  Use a low limit for
        // number of tires in order to test cache victimization.  7 car
        // descriptions times 4 tires is 28 tires, so set limit to 25.
        agency = new RentalCarAgency(exclusive, 25);

        List<String> carDescriptions = Arrays.asList(
            new String []
            {
                "Economy 4-door",
                "Economy 4-door (Smoking)",
                "Compact 2-door",
                "Compact 4-door",
                "Sporty Lemon 2-door",
                "Minivan 8-door (w/Escape Pod)",
                "SUV"
            });
        List<CustomerThread> threads = new ArrayList<CustomerThread>();
        int nThreads = 4;

        try {
            // Kick off threads.
            for (int i = 0; i < nThreads; ++i) {
                CustomerThread thread = new CustomerThread(carDescriptions);
                thread.start();
                threads.add(thread);
            }
            // Run for three seconds.  The transactions are very quick,
            // so this is long enough to flush out most problems, including
            // hitting the max mileage.
            Thread.currentThread().sleep(3000);
        } catch (InterruptedException ex) {
            throw Util.newInternal(ex);
        } finally {
            // Reap threads.
            for (CustomerThread thread : threads) {
                thread.quit();
                try {
                    thread.join();
                } catch (InterruptedException ex) {
                    throw Util.newInternal(ex);
                }
            }
        }

        // Check thread status (do this after all threads have been
        // reaped above to avoid letting some linger).
        for (CustomerThread thread : threads) {
            thread.assertSuccessful();
        }

        // No matter what, should never exceed limit on tires.
        long tiresCurrent = agency.getTiresInFleet();
        assertTrue(tiresCurrent <= MAX_TIRES);
    }

    /**
     * RentalCar exemplifies a reusable object.
     */
    private class RentalCar implements FarragoAllocation
    {
        private String description;
        private int mileage;
        private boolean isDriving;

        /**
         * Creates a new car, with an initial mileage of 0 coming
         * out of the factory.
         *
         * @param description bland description of this car,
         * e.g. "SUV 12-Door"
         */
        RentalCar(String description)
        {
            this.description = description;
            nCarsCreated.incrementAndGet();
            if (hasEscapePod()) {
                // NOTE jvs 15-Jun-2007:  This mimics the pattern
                // in SQL statement preparation where we do reentrant
                // cache pins; incorrect nested synchronization in
                // FarragoObjectCache can lead to deadlocks.
                RentalCarAgreement rca = agency.rentCar("Mini");
                agency.returnCar(rca);
            }
        }

        /**
         * @return description of this car
         */
        public String getDescription()
        {
            return description;
        }

        /**
         * Drives this car, increasing the mileage.
         *
         * @param miles number of miles to drive
         */
        public void drive(int miles)
        {
            if (agency.isExclusive()) {
                // Only one person can drive a car at a time.  No backseat
                // drivers!  This block is intentionally NOT synchronized; in
                // multi-threaded tests, we want to catch incorrect sharing.
                assertFalse(isDriving);
                isDriving = true;
                mileage += miles;
                // Yield here to increase the chance that another thread
                // will see the isDriving state.
                Thread.yield();
                isDriving = false;
            } else {
                // Hold the wheel for me, please.
                synchronized(this) {
                    mileage += miles;
                }
            }
        }

        /**
         * @return current mileage for this car
         */
        public int getMileage()
        {
            return mileage;
        }

        /**
         * @return whether this car is old enough that it ought
         * to be discarded
         */
        public boolean isOld()
        {
            return mileage > MAX_MILEAGE;
        }

        /**
         * @return whether this car is a "smoking" vehicle, meaning it can
         * never be reused since that lingering smell is so nasty
         */
        public boolean isSmokingVehicle()
        {
            return description.indexOf("Smoking") > -1;
        }

        /**
         * @return whether this vehicle comes with an "escape pod"
         * (another rental car inside of it!)
         */
        public boolean hasEscapePod()
        {
            return description.indexOf("Escape Pod") > -1;
        }

        /**
         * Tests this car's quality after fabrication, throwing
         * a {@link LemonException} if unacceptable.
         */
        public void assureQuality()
        {
            if (description.indexOf("Lemon") > -1) {
                closeAllocation();
                throw new LemonException();
            }
        }

        /**
         * @return whether this car is absolutely too old to be in service as a
         * rental car any more
         */
        public boolean isTooOld()
        {
            return mileage > 2*MAX_MILEAGE;
        }

        // implement ClosableAllocation
        public void closeAllocation()
        {
            nCarsDestroyed.incrementAndGet();
        }
    }

    private class LemonException extends RuntimeException
    {
        LemonException()
        {
            super("Hoopty");
        }
    }

    /**
     * RentalCarAgreement encapsulates the assignment of a car to a customer
     * for a duration.
     */
    private class RentalCarAgreement
    {
        RentalCar car;

        FarragoObjectCache.Entry pinnedEntry;

        /**
         * Creates a new agreement.
         *
         * @param pinnedEntry reference to pinned cache entry, which
         * implements the checkout of a car from a fleet
         */
        RentalCarAgreement(FarragoObjectCache.Entry pinnedEntry)
        {
            this.pinnedEntry = pinnedEntry;
            car = (RentalCar) pinnedEntry.getValue();
        }

        /**
         * @return car assigned to customer by this agreement
         */
        public RentalCar getCar()
        {
            return car;
        }
    }

    /**
     * RentalCarAgency exemplifies a cache of reusable
     * objects.
     */
    private class RentalCarAgency
        implements FarragoObjectCache.CachedObjectFactory
    {
        private FarragoCompoundAllocation owner;
        private FarragoObjectCache fleet;
        private boolean exclusiveRentals;

        /**
         * Creates a new agency.
         *
         * @param exclusiveRentals if true, cars are rented out exclusively;
         * if false, cars can be shared by customers as long as the
         * description of the car is the same (more like hailing a cab
         * and sharing the ride)
         *
         * @param maxTires maximum number of tires which can exist
         * in fleet at any one time
         */
        RentalCarAgency(boolean exclusiveRentals, long maxTires)
        {
            this.exclusiveRentals = exclusiveRentals;
            owner = new FarragoCompoundAllocation();
            fleet =
                new FarragoObjectCache(
                    owner,
                    maxTires,
                    new FarragoLruVictimPolicy());
            assertEquals(maxTires, fleet.getBytesMax());
        }

        /**
         * Rents out a car of a given description.
         *
         * @param description of car to rent
         *
         * @return rental agreement
         */
        RentalCarAgreement rentCar(String description)
        {
            // If the fleet already has a car available matching the criteria,
            // rent that; otherwise, have to make a new one (in that case,
            // FarragoObjectCache will call back to our initializeEntry
            // implementation).
            FarragoObjectCache.Entry entry =
                fleet.pin(description, this, exclusiveRentals);
            RentalCarAgreement rca = new RentalCarAgreement(entry);
            return rca;
        }

        /**
         * Records that a customer has returned a car, terminating the
         * agreement.
         *
         * @param agreement agreement to be terminated
         */
        void returnCar(RentalCarAgreement agreement)
        {
            fleet.unpin(agreement.pinnedEntry);
        }

        // implement FarragoObjectCache.CachedObjectFactory
        public void initializeEntry(
            Object key,
            FarragoObjectCache.UninitializedEntry entry)
        {
            assert (key instanceof String);
            String description = (String) key;
            RentalCar car = new RentalCar(description);
            car.assureQuality();
            // for now, all cars have four tires; "smoking" cars
            // can never be shared or even reused
            entry.initialize(car, 4, !car.isSmokingVehicle());
        }

        // implement FarragoObjectCache.CachedObjectFactory
        public boolean isStale(Object value)
        {
            // After enough miles, sell it to a used-car dealer.
            return ((RentalCar) value).isOld();
        }

        /**
         * @return current number of tires on all cars in fleet
         * (regardless of whether those cars are currently rented out)
         */
        public long getTiresInFleet()
        {
            return fleet.getBytesCached();
        }

        /**
         * Decommissions all cars in the fleet; subsequent requests will
         * require creation of new cars.  May only be called
         * when no cars are currently rented out.
         */
        public void decommissionEntireFleet()
        {
            fleet.discardAll();
        }

        /**
         * @return whether this agency rents cars exclusively
         */
        public boolean isExclusive()
        {
            return exclusiveRentals;
        }

        /**
         * Shuts down this agency and verifies that its business affairs
         * were cleanly terminated.  May only be called when
         * no cars are currently rented out.
         */
        public void shutDown()
        {
            owner.closeAllocation();
            assertEquals(0, getTiresInFleet());
            assertFalse(owner.hasAllocations());
            fleet = null;
        }
    }

    /**
     * CustomerThread exemplifies a thread which uses objects from the
     * cache.
     */
    private class CustomerThread extends Thread
    {
        private boolean success;

        private boolean quit;

        private boolean sawSmokingVehicle;

        private List<String> carDescriptions;

        /**
         * Creates a new customer thread.
         *
         * @param carDescriptions list of car descriptions the customer
         * should rent, one after another
         */
        public CustomerThread(List<String> carDescriptions)
        {
            this.carDescriptions = carDescriptions;
        }

        /**
         * Tells the customer to buzz off.
         */
        public void quit()
        {
            quit = true;
        }

        public void assertSuccessful()
        {
            assertTrue(success);
        }

        // implement Runnable
        public void run()
        {
            int n = carDescriptions.size();
            int i = 0;
            int nIterations = 0;
            for (;;) {
                if (quit && (nIterations > 0)) {
                    assertTrue(sawSmokingVehicle);
                    success = true;
                    return;
                }
                String description = carDescriptions.get(i);
                RentalCarAgreement rca;
                try {
                    rca = agency.rentCar(description);
                } catch (LemonException ex) {
                    // Tolerate failure.
                    rca = null;
                }
                if (rca != null) {
                    useCar(rca, description);
                }
                ++i;
                if (i == n) {
                    ++nIterations;
                    i = 0;
                }
            }
        }

        private void useCar(
            RentalCarAgreement rca,
            String description)
        {
            try {
                RentalCar car = rca.getCar();
                // Make sure we got the car we asked for.
                assertEquals(description, car.getDescription());
                // And make sure we didn't get some old junker.  Note
                // that there's some tolerance here between
                // isOld and isTooOld, since in the shared
                // case, one customer may rent the car just at the
                // same time as another one pushes it over the limit.
                assertFalse(car.isTooOld());
                if (car.isSmokingVehicle()) {
                    // Smoking vehicles are never supposed to be
                    // shared or reused, so the mileage should
                    // always be zero when we get it.
                    assertEquals(0, car.getMileage());
                    sawSmokingVehicle = true;
                }
                // Now, take it for a test-drive.
                car.drive(100);
            } finally {
                // No matter what happens, don't leave it dangling.
                agency.returnCar(rca);
            }
        }
    }
}

// End FarragoObjectCacheTest.java
