package com.wordpress.algarecu.ds.hazelcast.fencedlock;

import com.hazelcast.config.Config;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.cp.session.CPSessionManagementService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This code sample demonstrates how monotonic fencing tokens
 * can be used for ordering lock holders and their operations
 * on external services.
 */
public class FencingOffStaleLockHolders {
	private static final Logger LOGGER = Logger.getLogger(FencingOffStaleLockHolders.class.getName());
	private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(2);

	/**
	 * Contains a "count" value and the fencing token
	 * which performed the last increment operation on the value.
	 */
	public static class FencedCount implements DataSerializable {

		private long fence;
		private int count;

		public FencedCount() {
		}

		FencedCount(long fence, int count) {
			this.fence = fence;
			this.count = count;
		}

		@Override
		public void writeData(ObjectDataOutput out) throws IOException {
			out.writeLong(fence);
			out.writeInt(count);
		}

		@Override
		public void readData(ObjectDataInput in) throws IOException {
			fence = in.readLong();
			count = in.readInt();
		}
	}

	/**
	 * Increments the current count value if the new fencing token
	 * in the function instance is greater than the existing one.
	 */
	public static class IncrementIfNotFencedOff implements IFunction<FencedCount, FencedCount>, DataSerializable {

		private long fence;

		public IncrementIfNotFencedOff() {
		}

		IncrementIfNotFencedOff(long fence) {
			this.fence = fence;
		}

		@Override
		public FencedCount apply(FencedCount current) {
			return fence > current.fence ? new FencedCount(fence, current.count + 1) : current;
		}

		@Override
		public void writeData(ObjectDataOutput out) throws IOException {
			out.writeLong(fence);
		}

		@Override
		public void readData(ObjectDataInput in) throws IOException {
			fence = in.readLong();
		}
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		LOGGER.setLevel(Level.INFO);
		LOGGER.severe("Severe: ");
		LOGGER.warning("Warning: ");
		LOGGER.info("Info :");
		LOGGER.finest("Really not important: ");

		Config config = new Config();
		CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
		cpSubsystemConfig.setCPMemberCount(4);
		HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
		HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
		HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(config);
		HazelcastInstance hz4 = Hazelcast.newHazelcastInstance(config);

		// Our IAtomicReference and FencedLock instances are placed into different CP groups.
		// We can consider them as independent services in practice...
		String atomicRefGroup = "atomicRefs";
		String lockGroup = "locks";

		IAtomicReference<FencedCount> atomicRef = hz1.getCPSubsystem().getAtomicReference("my-ref@" + atomicRefGroup);
		atomicRef.set(new FencedCount(FencedLock.INVALID_FENCE, 0));

		String lockName = "my-lock@" + lockGroup;
		FencedLock hz1Lock = hz1.getCPSubsystem().getLock(lockName);

		// Member One crashes. After some time, the lock 
		// will be auto-released due to missing CP session heartbeats
		hz1.getLifecycleService().terminate();

		CPSessionManagementService sessionManagementService = hz2.getCPSubsystem().getCPSessionManagementService();
		Collection sessions = sessionManagementService.getAllSessions(CPGroup.DEFAULT_GROUP_NAME).get();
		// There is only one active session and it belongs to the first instance
		assert sessions.size() == 1;
		CPSession session = (CPSession) sessions.iterator().next();
		
		// We know that the lock holding instance is crashed.
		// We are closing its session forcefully, hence releasing the lock...
		sessionManagementService.forceCloseSession(CPGroup.DEFAULT_GROUP_NAME, session.id()).get();

		FencedLock hz2Lock = hz2.getCPSubsystem().getLock(lockName);
		FencedLock hz3Lock = hz3.getCPSubsystem().getLock(lockName);
		FencedLock hz4Lock = hz4.getCPSubsystem().getLock(lockName);

		while (hz2Lock.isLocked() || hz2Lock.isLocked()) {
			Thread.sleep(TimeUnit.SECONDS.toMillis(1));
			System.out.println("Waiting for auto-release of the lock...");
		}

		long fence1 = hz1Lock.lockAndGetFence();
		// The first lock holder increments the count asynchronously.
		Future<Object> future1 = incrementAsync(atomicRef, fence1);
		hz1Lock.unlock();

		long fence2 = hz2Lock.lockAndGetFence();
		// The second lock holder increments the count asynchronously.
		Future<Object> future2 = incrementAsync(atomicRef, fence2);
		hz2Lock.unlock();

		long fence3 = hz3Lock.lockAndGetFence();
		// The second lock holder increments the count asynchronously.
		Future<Object> future3 = incrementAsync(atomicRef, fence3);
		hz3Lock.unlock();

		long fence4 = hz4Lock.lockAndGetFence();
		// The second lock holder increments the count asynchronously.
		Future<Object> future4 = incrementAsync(atomicRef, fence4);
		hz4Lock.unlock();

		future1.get();
		future2.get();
		future3.get();
		future4.get();

		// If the increment function of the second holder can be executed before the increment function
		// of the first holder, then the second function execution will not increment the count value.
		int finalValue = atomicRef.get().count;
		assert finalValue == 1 || finalValue == 2;

		// Sleep before
		Thread.sleep(6000);

		EXECUTOR.shutdown();
		hz1.getLifecycleService().terminate();
		hz2.getLifecycleService().terminate();
		hz3.getLifecycleService().terminate();
	}

	/**
	 * Applies the {@link IncrementIfNotFencedOff} function
	 * on the {@link IAtomicReference} instance after some random delay
	 */
	private static Future<Object> incrementAsync(final IAtomicReference<FencedCount> atomicRef, final long fence) {
		int randomDelayMs = new Random().nextInt(2000) + 2000;
		return EXECUTOR.schedule(new Callable<Object>() {
			@Override
			public Object call() {
				atomicRef.alter(new IncrementIfNotFencedOff(fence));
				return null;
			}
		}, randomDelayMs, TimeUnit.MILLISECONDS);
	}
}
