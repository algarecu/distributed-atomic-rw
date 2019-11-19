package com.wordpress.algarecu.ds.atomix;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.junit.Test;

import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.AtomixConfig;
import io.atomix.core.semaphore.AtomicSemaphore;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.primitive.Replication;
import io.atomix.primitive.partition.MemberGroupStrategy;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.storage.StorageLevel;


public class ReplicasBootAndClient {
	static String filename = "threadsFile.txt";


	static class LockingThread extends Thread {
		// attributes
		String threadName = "";
		int opsCount = 1;
		Atomix atomix;
		AtomicSemaphore distSemaphore;

		// Constructor
		LockingThread(String name, Atomix atomix){
			this.threadName = name;
			this.atomix = atomix;
			AtomicSemaphore semaphore = atomix.atomicSemaphoreBuilder("my-semaphore")
					.withProtocol(MultiRaftProtocol.builder()
							.withReadConsistency(ReadConsistency.LINEARIZABLE)
							.build())
					.build();
			this.distSemaphore = semaphore;
		}
		public void run() {
			try {
				System.out.println(threadName + " : acquiring the file lock...");
				System.out.println(threadName + " : available permits now: " + distSemaphore.availablePermits());

				distSemaphore.acquire();
				System.out.println(threadName + " : got the thread lock!");

				DateFormat dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
				Date today = Calendar.getInstance().getTime();
				String logDate = dateFormat.format(today);

				System.out.println(threadName + " : DOING OPERATION " + opsCount 
						+ ", available Semaphore permits : "
						+ distSemaphore.availablePermits());
				System.out.println(threadName + " : DOING WRITE TO FILE " + filename 
						+ ", with timestamp : "
						+ logDate);
				opsCount++;

				BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));
				writer.write(logDate);
				writer.newLine();
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				System.out.println(threadName + " : releasing lock...");
				distSemaphore.release();
			}
		}
	}

	static void WriteToFile(String filename) {
		// get the date
		DateFormat dateFormat = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
		Date today = Calendar.getInstance().getTime();
		String logDate = dateFormat.format(today);

		// get the writer
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(filename, true));
			writer.write(logDate);
			writer.newLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static Member member(String endpoint) {
		return Member.builder(endpoint)
				.withProperty("Type", "PERSISTENT")
				.withHost(endpoint)
				.build();
	}

	static Atomix create(Integer id, Member local, final List<Member> members) {
		File dataDir = new File("/tmp/data-local" + "-local-" + System.currentTimeMillis() / 1000);
		final Atomix atomix = Atomix.builder()
				.withMemberId("member0")
				.withHost("localhost").withPort(8000)
				.withMembershipProvider(BootstrapDiscoveryProvider.builder()
						.withNodes(
								Node.builder()
								.withId("member1")
								.withHost("localhost").withPort(8001)
								.build(),
								Node.builder()
								.withId("member2")
								.withHost("localhost").withPort(8002)
								.build(),
								Node.builder()
								.withId("member3")
								.withHost("localhost").withPort(8003)
								.build(),
								Node.builder()
								.withId("member4")
								.withHost("localhost").withPort(8003)
								.build())
						.build())
				.withManagementGroup(RaftPartitionGroup.builder("system")
						.withNumPartitions(1)
						.withDataDirectory(dataDir)
						.withMembers("member1", "member2", "member3", "member4")
						.build())
				.withPartitionGroups(
						// Must use Multi-primary protocols are designed for high scalability and availability
						// To use multi-primary primitives, the cluster must first be configured with a PrimaryBackupPartitionGroup
						PrimaryBackupPartitionGroup.builder("data")
						.withNumPartitions(1)
						.withMemberGroupStrategy(MemberGroupStrategy.HOST_AWARE)
						.build())
				.build();
		return atomix;
	}


	public static void main(String[] args) throws InterruptedException {
		// server count
		final int numberOfServers = 4;

		// Create members (nodes/hosts)
		final List<Member> members = new ArrayList<>();
		for (int i = 0; i < numberOfServers; i++) {
			members.add(member("localhost:8000" + i));
		}
		System.out.println("Created members");

		// create Atomix servers 
		List<Atomix> servers = new ArrayList<>();
		for (int i = 0; i < numberOfServers; i++) {
			// create but not start the servers
			Member localMember = members.get(i);
			Atomix atomix = create(i, localMember, members);
			servers.add(atomix);
		}
		// Start and wait for servers to fully start
		List<CompletableFuture<Void>> futures = new ArrayList<>();
		futures.addAll(servers.stream().map(a -> a.start()).collect(Collectors.toList()));
		try {
			CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(60, TimeUnit.SECONDS);
		} catch (ExecutionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TimeoutException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Created servers");

		// Apparently we need to create a semaphore for each server in Atomix (replicated)
		final List<DistributedSemaphore> locks = new ArrayList<>();
		for (int i = 0; i < numberOfServers; i++) {
			final DistributedSemaphore lock = servers.get(i).semaphoreBuilder("test-lock")
					.withProtocol(MultiPrimaryProtocol.builder("data")
							.withBackups(2)
							.withReplication(Replication.ASYNCHRONOUS)
							.build())
					.build();
			locks.add(lock);
		}

		// Now for every server (except #0, we'll handle that by hand), make them start obtaining the lock over and over
		for (int i = 1; i < numberOfServers; i++) {
			final int lockNumber = i;
			new Thread(() -> {
				DistributedSemaphore lock = locks.get(lockNumber);
				while (!Thread.currentThread().isInterrupted()) {
					try {
						lock.acquire();
						Thread.sleep(500);
						System.out.println("Got lock #" + lockNumber);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} finally {
						lock.release();
					}
				}
			}).start();
		}

		// Now get semaphore #0 and lock it. This works without issue.
		DistributedSemaphore a1Lock = locks.get(0);
		System.out.println("\nObtaining lock #0\n");
		a1Lock.acquire();
		System.out.println("\nLocked #0 obtained!\n");
		a1Lock.release();

		// Stop/shutdown server 2 and 3
		servers.get(2).stop().join();
		servers.get(3).stop().join();

		// Restart sever 2 (note, server 3 never returns)
		servers.set(2, create(2, members.get(2), members));
		servers.get(2).start().join();

		System.out.println("\nObtaining lock #0 again\n");
		// This lock will fail
		a1Lock.acquire();
		System.out.println("\nLocked #0 obtained again!\n");
		a1Lock.release();

		for (int i = 0; i < numberOfServers; i++) {
			servers.get(i).stop().join();
		}
	}
}

