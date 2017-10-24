package com.agoda.addresscache;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class AddressCacheTest {

	private static final long SECONDS_INTERVAL = 1000;

	private static final String IP_ADDRESS_1 = "8.8.8.8";
	private static final String IP_ADDRESS_2 = "4.4.4.4";
	private static final String IP_ADDRESS_3 = "127.0.0.1"; // No place like home

	/*
	 * This should time-out
	 */
	@Test(timeout = 25000)
	public void testNullAndEmpty() throws InterruptedException, UnknownHostException {
		AddressCache addressCache = new AddressCache(20, TimeUnit.SECONDS);
		Assert.assertEquals(addressCache.peek(), null);
		Assert.assertEquals(addressCache.take(), null); // Waits indefinitely
		Assert.assertNotEquals(addressCache.take(), false);
		Assert.assertNotEquals(addressCache.take(), InetAddress.getLocalHost());
		addressCache.clear();
	}

	@Test
	public void testBasic() throws InterruptedException, UnknownHostException {
		AddressCache addressCache = new AddressCache(60, TimeUnit.SECONDS);
		Assert.assertEquals(addressCache.add(InetAddress.getLocalHost()), true);
		Assert.assertEquals(addressCache.peek(), InetAddress.getLocalHost());
		Assert.assertEquals(addressCache.take(), InetAddress.getLocalHost());
		addressCache.clear();
	}

	@Test
	public void testExpiry() throws InterruptedException, UnknownHostException {
		AddressCache addressCache = new AddressCache(10, TimeUnit.SECONDS);
		Assert.assertEquals(addressCache.add(InetAddress.getLocalHost()), true);
		Thread.sleep(12 * SECONDS_INTERVAL);
		Assert.assertEquals(addressCache.peek(), null);
		addressCache.clear();
	}

	@Test
	public void testSequence() throws InterruptedException, UnknownHostException {
		AddressCache addressCache = new AddressCache(10, TimeUnit.SECONDS);
		Assert.assertEquals(addressCache.add(InetAddress.getLocalHost()), true);
		Assert.assertEquals(addressCache.add(InetAddress.getByName(IP_ADDRESS_1)), true);
		Assert.assertEquals(addressCache.add(InetAddress.getByName(IP_ADDRESS_2)), true);
		Assert.assertEquals(addressCache.add(InetAddress.getByName(IP_ADDRESS_3)), true);
		Assert.assertEquals(addressCache.take(), InetAddress.getByName(IP_ADDRESS_3));
		Assert.assertEquals(addressCache.take(), InetAddress.getByName(IP_ADDRESS_2));
		Assert.assertEquals(addressCache.take(), InetAddress.getByName(IP_ADDRESS_1));
		Assert.assertEquals(addressCache.take(), InetAddress.getLocalHost());
		Assert.assertEquals(addressCache.peek(), null);
		addressCache.clear();
	}

	@Test
	public void testSequenceWithExpiry() throws InterruptedException, UnknownHostException {
		AddressCache addressCache = new AddressCache(10, TimeUnit.SECONDS);
		Assert.assertEquals(addressCache.add(InetAddress.getLocalHost()), true);
		Thread.sleep(12 * SECONDS_INTERVAL);
		Assert.assertEquals(addressCache.add(InetAddress.getByName(IP_ADDRESS_1)), true);
		Assert.assertEquals(addressCache.add(InetAddress.getByName(IP_ADDRESS_2)), true);
		Assert.assertEquals(addressCache.take(), InetAddress.getByName(IP_ADDRESS_2));
		Assert.assertEquals(addressCache.take(), InetAddress.getByName(IP_ADDRESS_1));
		Assert.assertEquals(addressCache.peek(), null);
		Thread.sleep(10 * SECONDS_INTERVAL);
		Assert.assertEquals(addressCache.peek(), null);
		addressCache.clear();
	}

	@Test
	public void testMultipleDuplicateInsertions() throws InterruptedException, UnknownHostException {
		AddressCache addressCache = new AddressCache(50, TimeUnit.SECONDS);
		Assert.assertEquals(addressCache.add(InetAddress.getLocalHost()), true);
		Assert.assertEquals(addressCache.add(InetAddress.getLocalHost()), false);
		Assert.assertEquals(addressCache.peek(), InetAddress.getLocalHost());
		Assert.assertEquals(addressCache.take(), InetAddress.getLocalHost());
		Assert.assertEquals(addressCache.peek(), null);
		addressCache.clear();
	}

	@Test(expected = UnknownHostException.class)
	public void testInsertionException() throws InterruptedException, UnknownHostException {
		AddressCache addressCache = new AddressCache(50, TimeUnit.SECONDS);
		Assert.assertEquals(addressCache.add(InetAddress.getLocalHost()), true);
		Assert.assertEquals(addressCache.add(InetAddress.getByName("Testing.invalid")), false);
		Assert.assertEquals(addressCache.take(), InetAddress.getLocalHost());
		addressCache.clear();
	}

	// TestNG is better and comes with out-of-the-box solutions for asynchronous testing - tad short on time
	@Test
	public void testWaitingTake() throws InterruptedException, UnknownHostException {
		final AddressCache addressCache = new AddressCache(50, TimeUnit.SECONDS);

		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					Assert.assertEquals(addressCache.take(), InetAddress.getLocalHost());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}, 5000);

		// New Timer to execute
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					Assert.assertEquals(addressCache.add(InetAddress.getLocalHost()), true);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}, 10000);

		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				Assert.assertEquals(addressCache.peek(), null);
			}
		}, 20000);

		addressCache.clear();
	}

}
