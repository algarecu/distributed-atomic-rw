package com.wordpress.algarecu.ds.atomix;

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
	
	static final String filename = "threadsfile.txt";

	
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
		buildLock(atomix, filename);
	}
}
	
//	/* Code to bootstrap cluster replicas with old version of atomix in the pom.xml */
//	Storage storage = Storage.builder()
//			.withDirectory(new File("log"))
//			.withStorageLevel(StorageLevel.DISK)
//			.build();
//	AtomixReplica replica = AtomixReplica.builder(new Address("localhost", 8100))
//			.withStorage(storage)
//			.withTransport(new NettyTransport())
//			.build();
//    CompletableFuture<AtomixReplica> completableFuture = replica.bootstrap();
//    completableFuture.join();
//    Thread.sleep(6000);
//
//	List<Address> cluster = Arrays.asList(
//			new Address("localhost", 8101),
//			new Address("localhost", 8102)
//			);
//
//	AtomixReplica replica1 = AtomixReplica
//			.builder(cluster.get(0)).withStorage(storage)
//			.withStorage(storage)
//			.build();
//	replica1
//	  .join(new Address("localhost", 8100))
//	  .join();
//	WorkerThread WT1 = new WorkerThread(replica1, cluster);
//    WT1.run();
//
//	AtomixReplica replica2 = AtomixReplica
//			.builder(cluster.get(1)).withStorage(storage)
//			.withStorage(storage)
//			.build();
//	replica2
//	  .join(new Address("localhost", 8100),
//			new Address("localhost", 8101))
//	  .join();
//	WorkerThread WT2 = new WorkerThread(replica2, cluster);
//    WT2.run();
//	
//	/* Lock and Write */
//	DistributedLock lock = replica.getLock("my-map")
//			.join();
//	lock.lock()
//	.thenRun(() -> System.out.println("Acquired a lock"));
//	
//	replica.getMap("my-map")
//	.thenCompose(m -> m.put("K", "Value1"))
//	.thenRun(() -> System.out.println("Value put into Distributed Map"))
//	.join();
//	
//	logger.debug("Built cluster with number of members: " + cluster.size());
//}
//private static class WorkerThread extends Thread {
//    private AtomixReplica replica;
//    private List<Address> cluster;
//
//    WorkerThread(AtomixReplica replica, List<Address> cluster) {
//        this.replica = replica;
//        this.cluster = cluster;
//    }
//
//    public void run() {
//        replica.join(cluster)
//          .join();
//    }
//}
