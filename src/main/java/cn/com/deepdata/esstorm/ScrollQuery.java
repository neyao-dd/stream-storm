package cn.com.deepdata.esstorm;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.rest.stats.Stats;
import org.elasticsearch.hadoop.rest.stats.StatsAware;
import org.elasticsearch.hadoop.serialization.ScrollReader;
import org.elasticsearch.hadoop.serialization.ScrollReader.Scroll;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.StringUtils;

public class ScrollQuery implements Iterator<Object>, Closeable, StatsAware {
	private RestRepository repository;
	private String scrollId;
	private List<Object[]> batch = Collections.emptyList();
	private boolean finished = false;

	private int batchIndex = 0;
	private long read = 0;
	// how many docs to read - in most cases, all the docs that match
	private long size;

	private final ScrollReader reader;

	private final Stats stats = new Stats();

	private boolean closed = false;
	private boolean initialized = false;
	private String query;
	private BytesArray body;

	ScrollQuery(RestRepository client, String query, BytesArray body,
			long size, ScrollReader reader) {
		this.repository = client;
		this.size = size;
		this.reader = reader;
		this.query = query;
		this.body = body;
	}

	public void close() {
		if (!closed) {
			closed = true;
			finished = true;
			batch = Collections.emptyList();
			// typically the scroll is closed after it is consumed so this will
			// trigger a 404
			// however we're closing it either way
			if (StringUtils.hasText(scrollId)) {
				repository.getRestClient().deleteScroll(scrollId);
			}
		}
	}

	public boolean hasNext() {
		if (finished) {
			return false;
		}

		if (!initialized) {
			initialized = true;

			try {
				Scroll scroll = repository.scroll(query, body, reader);
				// size is passed as a limit (since we can't pass it directly
				// into the request) - if it's not specified (<1) just scroll
				// the whole index
				size = (size < 1 ? scroll.getTotalHits() : size);
				scrollId = scroll.getScrollId();
				batch = scroll.getHits();
			} catch (IOException ex) {
				throw new EsHadoopIllegalStateException(String.format(
						"Cannot create scroll for query [%s/%s]", query, body),
						ex);
			}
			// no longer needed
			body = null;
			query = null;
		}

		if (batch.isEmpty() || batchIndex >= batch.size()) {
			if (read >= size) {
				finished = true;
				return false;
			}

			try {
				Scroll scroll = repository.scroll(scrollId, reader);
				scrollId = scroll.getScrollId();
				batch = scroll.getHits();
			} catch (IOException ex) {
				throw new EsHadoopIllegalStateException(
						"Cannot retrieve scroll [" + scrollId + "]", ex);
			}
			read += batch.size();
			stats.docsReceived += batch.size();

			if (batch.isEmpty()) {
				finished = true;
				return false;
			}
			// reset index
			batchIndex = 0;
		}

		return true;
	}

	public long getSize() {
		return size;
	}

	public long getRead() {
		return read;
	}

	public Object[] next() {
		if (!hasNext()) {
			throw new NoSuchElementException("No more documents available");
		}
		return batch.get(batchIndex++);
	}

	public void remove() {
		throw new UnsupportedOperationException("read-only operator");
	}

	public Stats stats() {
		// there's no need to do aggregation
		return new Stats(stats);
	}

	public RestRepository repository() {
		return repository;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ScrollQuery [scrollId=").append(scrollId).append("]");
		return builder.toString();
	}
}
