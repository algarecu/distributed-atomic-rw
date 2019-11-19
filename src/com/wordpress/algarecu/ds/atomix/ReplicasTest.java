package com.wordpress.algarecu.ds.atomix;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import io.atomix.cluster.Member;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.lock.DistributedLock;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;

public class ReplicasTest {

	@Test
	public void testIt() throws InterruptedException, ExecutionException, TimeoutException {
		// server count
		final int numberOfServers = 4;
		// Create members (nodes/hosts)
		final List<Member> members = new ArrayList<>();
		for (int i = 0; i < numberOfServers; i++) {
			members.add(member("localhost:8000" + i));
		}

		// Create Atomix servers (but not start them)
		List<Atomix> servers = new ArrayList<>();
		for (int i = 0; i < numberOfServers; i++) {
			Member localMember = members.get(i);
			Atomix atomix = create(i, localMember, members);
			servers.add(atomix);
		}

		// Create a lock for each server
		final List<DistributedLock> locks = new ArrayList<>();
		for (int i = 0; i < numberOfServers; i++) {
			final DistributedLock lock = servers.get(i).lockBuilder("test-lock")
					.withProtocol(MultiRaftProtocol.builder()
							.withMaxRetries(5)
							.build())
					.build();
			locks.add(lock);
		}

		// Now for every server (except #0, we'll handle that by hand), make them start obtaining the lock over and over
		for (int i = 1; i < numberOfServers; i++) {
			final int lockNumber = i;
			new Thread(() -> {
				DistributedLock lock = locks.get(lockNumber);
				while (!Thread.currentThread().isInterrupted()) {
					try {
						lock.lock();
						Thread.sleep(500);
						System.out.println("Got lucky #" + lockNumber);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} finally {
						lock.unlock();
					}
				}
			}).start();
		}

		// Now get lock #0 (from server #0) and lock it. This works without issue.
		DistributedLock a1Lock = locks.get(0);
		System.out.println("\nObtaining lock #0\n");
		a1Lock.lock();
		System.out.println("\nLocked #0 obtained!\n");
		a1Lock.unlock();

		// Stop/shutdown server 2 and 3
		servers.get(2).stop().join();
		servers.get(3).stop().join();

		// Restart sever 2 (note, server 3 never returns)
		servers.set(2, create(2, members.get(2), members));
		servers.get(2).start().join();

		System.out.println("\nObtaining lock #0 again\n");
		// This lock will fail
		a1Lock.lock();
		System.out.println("\nLocked #0 obtained again!\n");
		a1Lock.unlock();

		for (int i = 0; i < numberOfServers; i++) {
			servers.get(i).stop().join();
		}
	}

	private static Member member(String endpoint) {
		// TODO Auto-generated method stub
		return Member.builder(endpoint)
				.withProperty("type", "PERSISTENT")
				.build();
	}

	private static Atomix create(Integer id, Member localMember, List<Member> members) {
		// TODO Auto-generated method stub
		File dataDir = new File("/tmp/data-" + id + "-" + System.currentTimeMillis() / 1000);
		Member[] membersArr = members.stream().toArray(Member[]::new);

		final Atomix atomix = Atomix.builder()
				.withMemberId("member1")
				.withHost("localhost").withPort(8000)
				.withMembershipProvider(BootstrapDiscoveryProvider.builder()
						.withNodes(membersArr)
						.build())
				.withManagementGroup(RaftPartitionGroup.builder("system")
						.withNumPartitions(1)
						.withMembers(membersArr)
						.withDataDirectory(dataDir)
						.build())
				.withPartitionGroups(PrimaryBackupPartitionGroup.builder("data")
						.withNumPartitions(1)
						.build())
				.build();
		return atomix;
	}
}
