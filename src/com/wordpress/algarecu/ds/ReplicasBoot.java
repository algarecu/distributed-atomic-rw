package com.wordpress.algarecu.ds;

import java.io.File;
import io.atomix.cluster.Member;
import io.atomix.cluster.Node;
import io.atomix.core.Atomix;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;


public class ReplicasBoot {
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
				.withMemberId("member1")
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
						.withNumPartitions(3)
						.withMembers("member1", "member2", "member3")
						.withDataDirectory(dataDir)
						.build())
				.withPartitionGroups(PrimaryBackupPartitionGroup.builder("data")
						.withNumPartitions(32)
						.build())
				.build();

		atomix.start().join();
	}
}

