package com.wordpress.algarecu.ds.atomix;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.core.lock.AtomicLock;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.primitive.Replication;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.primitive.partition.MemberGroupStrategy;


public class ReplicasBootAtomicLock {
	static final String filename = "threadsfile.txt";

	static Member member(String endpoint) {
		return Member.builder(endpoint)
				.withProperty("Type", "PERSISTENT")
				.withHost(endpoint)
				.build();
	}

	static void writeToFile() {
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

	static Atomix create() throws InterruptedException {
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
								.withHost("localhost").withPort(8004)
								.build())
						.build())
				.withManagementGroup(RaftPartitionGroup.builder("system")
						.withNumPartitions(4)
						.withMembers("member0", "member1", "member2", "member3")
						.withDataDirectory(dataDir)
						.build())
				.withPartitionGroups(
						// Multi-primary protocols are designed for HIGH SCALABILITY and availability
						// To use multi-primary primitives, the cluster must first be configured with a PrimaryBackupPartitionGroup
						PrimaryBackupPartitionGroup.builder("data")
						.withNumPartitions(1)
						.withMemberGroupStrategy(MemberGroupStrategy.HOST_AWARE)
						.build()
						)
				.build();
		return atomix;
	}

	// -------------- The Main -------------- //

	public static void main(String[] args) throws InterruptedException {
		final Atomix atomix = create();

		CompletableFuture<Void> completableFuture = atomix.start();
		completableFuture.join();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Build the Atomix");
		Thread.sleep(6000);
		
		// build the distributed semaphore
		List<DistributedSemaphore> locks = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			final DistributedSemaphore lock = atomix.semaphoreBuilder("my-semaphore")
					.withProtocol(MultiPrimaryProtocol.builder("data")
							.withBackups(2)
							.withReplication(Replication.ASYNCHRONOUS)
							.build())
					.build();
			locks.add(lock);
		}
		System.out.println("Built the distributed semaphore");

		atomix.getAtomicSemaphore("my-semaphore");
		AtomicLock myLock = atomix.atomicLockBuilder("my-semaphore")
				.withProtocol(MultiRaftProtocol.builder()
						.withReadConsistency(ReadConsistency.LINEARIZABLE)
						.build())
				.build();
		System.out.println("Get the atomic lock");

		Optional<io.atomix.utils.time.Version> result = myLock.tryLock(Duration.ofSeconds(10));
		try {
			if (result.isPresent()) {
				System.out.println("Acquire succeeded!");
				writeToFile();
			} else {
				System.out.println("Acquire failed!");
			}
		} finally {
			myLock.unlock();
			myLock.close();
		}
	}
}