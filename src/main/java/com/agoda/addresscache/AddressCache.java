package com.agoda.addresscache;

import java.net.InetAddress;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*
 * The AddressCache has a max age for the elements it's storing, an add method 
 * for adding elements, a remove method for removing, a peek method which 
 * returns the most recently added element, and a take method which removes 
 * and returns the most recently added element.
 */
public class AddressCache {

	// Increase with respect to volume of requests - For circumstances where we have millions of insertions, a larger number will suffice
	private static final int THREAD_POOL_FOR_EXPIRIES = 4;

	// Wiser choice than LinkedBlockingQueue since we can insert/remove on both ends
	private final ConcurrentLinkedDeque<InetAddress> internalAddressCache;

	private final ScheduledExecutorService expiryScheduler;

	private final TimeUnit unit;
	private final long maxAge;

	private final CyclicBarrier cyclicBarrier;

	public AddressCache(long maxAge, TimeUnit unit) {
		this.internalAddressCache = new ConcurrentLinkedDeque<InetAddress>();
		this.expiryScheduler = Executors.newScheduledThreadPool(THREAD_POOL_FOR_EXPIRIES);
		this.unit = unit;
		this.maxAge = maxAge;
		this.cyclicBarrier = new CyclicBarrier(50000);
	}

	/**
	 * add() method must store unique elements only (existing elements must be ignored). This will return true if the element was successfully added.
	 * 
	 * @param address
	 * @return
	 */
	public boolean add(InetAddress address) {
		return (!getInternalAddressCache().contains(address)) ? addAddress(address) : false;
	}

	/**
	 * remove() method will return true if the address was successfully removed
	 * 
	 * @param address
	 * @return
	 */
	public boolean remove(InetAddress address) {
		return getInternalAddressCache().remove(address);
	}

	/**
	 * The peek() method will return the most recently added element, null if no element exists.
	 * 
	 * @return
	 */
	public InetAddress peek() {
		return getInternalAddressCache().peekLast();
	}

	/**
	 * take() method retrieves and removes the most recently added element from the cache and waits if necessary until an element becomes available.
	 * 
	 * @return
	 * @throws InterruptedException
	 * @throws BrokenBarrierException
	 */

	// We could have gone with a simple while() loop which waits indefinitely, however, it's better to use java.util.concurrent's robust classes crafted for these scenarios
	public InetAddress take() throws InterruptedException {
		try {
			if (getInternalAddressCache().peekLast() == null)
				cyclicBarrier.await();
		} catch (BrokenBarrierException e) { // Populate this if debugging is required
		}

		return getInternalAddressCache().pollLast();
	}

	/*
	 * Call this after using cache - Need to clear cache - Good people clean after themselves
	 */
	public void clear() {
		cyclicBarrier.reset();
		getExpiryScheduler().shutdownNow();
		getInternalAddressCache().clear();
	}

	/**
	 * Inserts an element in the cache and initializes its expiry scheduler
	 * 
	 * @param address
	 *            The address to add
	 * @return Boolean depending on successful addition
	 */
	private boolean addAddress(InetAddress address) {
		if (getInternalAddressCache().add(address)) { // A Fail Case here, though unlikely - Assumption JVM's thread-pool scheduling doesn't break
			getExpiryScheduler().schedule(removeElementAfterDelay(address), getMaxAge(), getUnit());
			if (cyclicBarrier.getNumberWaiting() > 0) {
				cyclicBarrier.reset();
			}
			return true;
		}

		return false;
	}

	/*
	 * Another way of approaching the problem would be to extend the Object class and incorporate expiry functions within those custom objects However, that would mean overriding the default compareTo methods etc. An inconvenient overhead. We lose core functionality unnecessarily A simple runnable is lightweight for an InetAddress storing cache
	 * 
	 */
	private Runnable removeElementAfterDelay(final InetAddress address) {
		return new Runnable() {
			public void run() {
				getInternalAddressCache().remove(address);
			}
		};
	}

	public ConcurrentLinkedDeque<InetAddress> getInternalAddressCache() {
		return internalAddressCache;
	}

	private ScheduledExecutorService getExpiryScheduler() {
		return expiryScheduler;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public long getMaxAge() {
		return maxAge;
	}
}