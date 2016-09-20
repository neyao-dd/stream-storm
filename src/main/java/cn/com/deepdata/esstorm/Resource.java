package cn.com.deepdata.esstorm;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.StringUtils;

public class Resource {
	private final String indexAndType;
	private final String type;
	private final String index;
	private final String bulk;
	private final String refresh;

	public Resource(Settings settings, boolean read) {
		String resource = (read ? settings.getResourceRead() : settings
				.getResourceWrite());

		String errorMessage = "invalid resource given; expecting [index]/[type] - received ";
		Assert.hasText(resource, errorMessage + resource);

		// add compatibility for now
		if (resource.contains("?") || resource.contains("&")) {
			if (StringUtils.hasText(settings.getQuery())) {
				throw new EsHadoopIllegalArgumentException(
						String.format(
								"Cannot specify a query in the target index AND through %s",
								ConfigurationOptions.ES_QUERY));
			}

			// extract query
			int index = resource.indexOf("?");
			if (index > 0) {
				String query = resource.substring(index);

				// clean resource
				resource = resource.substring(0, index);
				index = resource.lastIndexOf("/");
				resource = (index > 0 ? resource.substring(0, index) : resource);

				settings.setProperty(ConfigurationOptions.ES_RESOURCE, resource);
				settings.setQuery(query);
			}
		}

		String res = StringUtils.sanitizeResource(resource);

		int slash = res.indexOf("/");
		if (slash < 0) {
			index = res;
			type = StringUtils.EMPTY;
		} else {
			index = res.substring(0, slash);
			type = res.substring(slash + 1);

			Assert.hasText(type, "No type found; expecting [index]/[type]");
		}
		Assert.hasText(index, "No index found; expecting [index]/[type]");
		Assert.isTrue(
				!StringUtils.hasWhitespace(index)
						&& !StringUtils.hasWhitespace(type),
				"Index and type should not contain whitespaces");

		indexAndType = index + "/" + type;

		// check bulk
		bulk = (indexAndType.contains("{") ? "/_bulk" : indexAndType + "/_bulk");
		refresh = (index.contains("{") ? "/_refresh" : index + "/_refresh");
	}

	String bulk() {
		return bulk;
	}

	String mapping() {
		return indexAndType + "/_mapping";
	}

	String aliases() {
		return index + "/_aliases";
	}

	String indexAndType() {
		return indexAndType;
	}

	public String type() {
		return type;
	}

	public String index() {
		return index;
	}

	@Override
	public String toString() {
		return indexAndType;
	}

	public String refresh() {
		return refresh;
	}
}
