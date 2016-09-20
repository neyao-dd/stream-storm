package cn.com.deepdata.esstorm;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.Resource;
import org.elasticsearch.hadoop.serialization.dto.NodeInfo;
import org.elasticsearch.hadoop.serialization.dto.ShardInfo;
import org.elasticsearch.hadoop.serialization.field.IndexExtractor;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.ObjectUtils;
import org.elasticsearch.hadoop.util.SettingsUtils;
import org.elasticsearch.hadoop.util.Version;

public class PartitionWriter implements Closeable {
	public final RestRepository repository;
	public final int number;
	public final int total;
	public final Settings settings;

	private boolean closed = false;

	PartitionWriter(Settings settings, int splitIndex, int splitsSize,
			RestRepository repository) {
		this.settings = settings;
		this.repository = repository;
		this.number = splitIndex;
		this.total = splitsSize;
	}

	public void close() {
		if (!closed) {
			closed = true;
			repository.close();
		}
	}

	public static PartitionWriter createWriter(Settings settings,
			int currentSplit, int totalSplits, Log log) {
		Version.logVersion();

		InitializationUtils.validateSettings(settings);
		InitializationUtils.discoverEsVersion(settings, log);
		InitializationUtils.discoverNodesIfNeeded(settings, log);
		InitializationUtils.filterNonClientNodesIfNeeded(settings, log);
		InitializationUtils.filterNonDataNodesIfNeeded(settings, log);

		List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);

		// check invalid splits (applicable when running in non-MR environments)
		// - in this case fall back to Random..
		int selectedNode = (currentSplit < 0) ? new Random().nextInt(nodes
				.size()) : currentSplit % nodes.size();

		// select the appropriate nodes first, to spread the load before-hand
		SettingsUtils.pinNode(settings, nodes.get(selectedNode));

		Resource resource = new Resource(settings, false);

		log.info(String.format("Writing to [%s]", resource));

		// single index vs multi indices
		IndexExtractor iformat = ObjectUtils.instantiate(
				settings.getMappingIndexExtractorClassName(), settings);
		iformat.compile(resource.toString());

		RestRepository repository = (iformat.hasPattern() ? initMultiIndices(
				settings, currentSplit, resource, log) : initSingleIndex(
				settings, currentSplit, resource, log));

		return new PartitionWriter(settings, currentSplit, totalSplits,
				repository);
	}

	private static RestRepository initSingleIndex(Settings settings,
			int currentInstance, Resource resource, Log log) {
		if (log.isDebugEnabled()) {
			log.debug(String.format("Resource [%s] resolves as a single index",
					resource));
		}

		RestRepository repository = new RestRepository(settings);
		// create the index if needed
		if (repository.touch()) {
			if (repository.waitForYellow()) {
				log.warn(String
						.format("Timed out waiting for index [%s] to reach yellow health",
								resource));
			}
		}

		if (settings.getNodesWANOnly()) {
			return randomNodeWrite(settings, currentInstance, resource, log);
		}

		// if client-nodes are used, simply use the underlying nodes
		if (settings.getNodesClientOnly()) {
			String clientNode = repository.getRestClient().getCurrentNode();
			if (log.isDebugEnabled()) {
				log.debug(String
						.format("Client-node routing detected; partition writer instance [%s] assigned to [%s]",
								currentInstance, clientNode));
			}

			return repository;
		}

		// no routing necessary; select the relevant target shard/node
		Map<ShardInfo, NodeInfo> targetShards = Collections.emptyMap();

		targetShards = repository.getWriteTargetPrimaryShards(settings
				.getNodesClientOnly());
		repository.close();

		Assert.isTrue(
				!targetShards.isEmpty(),
				String.format(
						"Cannot determine write shards for [%s]; likely its format is incorrect (maybe it contains illegal characters?)",
						resource));

		List<ShardInfo> orderedShards = new ArrayList<ShardInfo>(
				targetShards.keySet());
		// make sure the order is strict
		Collections.sort(orderedShards);
		if (log.isTraceEnabled()) {
			log.trace(String
					.format("Partition writer instance [%s] discovered [%s] primary shards %s",
							currentInstance, orderedShards.size(),
							orderedShards));
		}

		// if there's no task info, just pick a random bucket
		if (currentInstance <= 0) {
			currentInstance = new Random().nextInt(targetShards.size()) + 1;
		}
		int bucket = currentInstance % targetShards.size();
		ShardInfo chosenShard = orderedShards.get(bucket);
		NodeInfo targetNode = targetShards.get(chosenShard);

		// pin settings
		SettingsUtils.pinNode(settings, targetNode.getPublishAddress());
		String node = SettingsUtils.getPinnedNode(settings);
		repository = new RestRepository(settings);

		if (log.isDebugEnabled()) {
			log.debug(String
					.format("Partition writer instance [%s] assigned to primary shard [%s] at address [%s]",
							currentInstance, chosenShard.getName(), node));
		}

		return repository;
	}

	private static RestRepository initMultiIndices(Settings settings,
			int currentInstance, Resource resource, Log log) {
		if (log.isDebugEnabled()) {
			log.debug(String.format(
					"Resource [%s] resolves as an index pattern", resource));
		}

		return randomNodeWrite(settings, currentInstance, resource, log);
	}

	private static RestRepository randomNodeWrite(Settings settings,
			int currentInstance, Resource resource, Log log) {
		// multi-index write - since we don't know before hand what index will
		// be used, pick a random node from the given list
		List<String> nodes = SettingsUtils.discoveredOrDeclaredNodes(settings);
		String node = nodes.get(new Random().nextInt(nodes.size()));
		// override the global settings to communicate directly with the target
		// node
		SettingsUtils.pinNode(settings, node);

		if (log.isDebugEnabled()) {
			log.debug(String.format(
					"Partition writer instance [%s] assigned to [%s]",
					currentInstance, node));
		}

		return new RestRepository(settings);
	}
}
