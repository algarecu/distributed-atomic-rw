package com.wordpress.algarecu.ds;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.core.lock.DistributedLock;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;


public class ReplicasBoot {
	
	private static void buildLock(Atomix atomix, String filename) {
		// build the distributed lock for raft protocol and linearizable reads
		MultiRaftProtocol protocol = MultiRaftProtocol.builder()
				.withReadConsistency(ReadConsistency.LINEARIZABLE)
				.build();
		DistributedLock myLock = atomix.lockBuilder("my-lock")
				.withProtocol(protocol)
				.build();

		// get the distributed lock
		myLock.lock();
		System.out.println("MyThread" + " : acquiring the file lock...");
		try {
			WriteToFile(filename);
		}
		finally {
			myLock.unlock();
			myLock.close();
		}
	}

	private static void WriteToFile(String filename) {
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Member member(String endpoint) {
		return Member.builder(endpoint)
				.withProperty("Type", "PERSISTENT")
				.withHost(endpoint)
				.build();
	}

	public static void main(String[] args) throws InterruptedException {
		File dataDir = new File("/tmp/data-local" + "-local-" + System.currentTimeMillis() / 1000);
		// build the cluster
		Atomix atomix = Atomix.builder()
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
								.build())
						.build())
				.withManagementGroup(RaftPartitionGroup.builder("system")
						.withNumPartitions(1)
						.withMembers("member1", "member2", "member3")
						.withDataDirectory(dataDir)
						.build())
				.withPartitionGroups(PrimaryBackupPartitionGroup.builder("data")
						.withNumPartitions(3)
						.build())
				.build();

		// atomix.start().join();

		CompletableFuture<Void> completableFuture = atomix.start();
		completableFuture.join();

		// build the lock
		String filename = "threadsfile.txt";
		buildLock(atomix, filename);
	}
}

