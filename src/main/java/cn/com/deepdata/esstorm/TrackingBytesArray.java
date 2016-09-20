package cn.com.deepdata.esstorm;

import java.io.IOException;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.hadoop.util.ByteSequence;
import org.elasticsearch.hadoop.util.BytesArray;
import org.elasticsearch.hadoop.util.BytesRef;
import org.elasticsearch.hadoop.util.StringUtils;

public class TrackingBytesArray implements ByteSequence {
	private static class Entry {
		final int offset;
		final int length;
		final int initialPosition;

		Entry(int offset, int length, int initialPosition) {
			this.offset = offset;
			this.length = length;
			this.initialPosition = initialPosition;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + length;
			result = prime * result + offset;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Entry other = (Entry) obj;
			if (length != other.length)
				return false;
			if (offset != other.offset)
				return false;
			return true;
		}
	}

	private final BytesArray data;
	private int maxEntries = 0;
	private int size = 0;
	private List<Entry> entries = new LinkedList<TrackingBytesArray.Entry>();

	public TrackingBytesArray(BytesArray data) {
		this.data = data;
	}

	public void copyFrom(BytesArray from) {
		addEntry(from.length());
		from.copyTo(data);
	}

	public void copyFrom(BytesRef from) {
		addEntry(from.length());
		from.copyTo(data);
	}

	public int length() {
		return size;
	}

	public int entries() {
		return entries.size();
	}

	public BitSet leftoversPosition() {
		BitSet bitSet = new BitSet(maxEntries);
		for (Entry entry : entries) {
			bitSet.set(entry.initialPosition);
		}

		return bitSet;
	}

	private void addEntry(int length) {
		// implied offset - data.size
		entries.add(new Entry(data.length(), length, entries.size()));
		size += length;
		maxEntries = size;
	}

	public void remove(int index) {
		Entry entry = entries.remove(index);
		size -= entry.length;
	}

	public int length(int index) {
		return entries.get(index).length;
	}

	public int initialPosition(int index) {
		return entries.get(index).initialPosition;
	}

	public void writeTo(OutputStream out) throws IOException {
		if (size == 0) {
			return;
		}

		for (Entry entry : entries) {
			out.write(data.bytes(), entry.offset, entry.length);
		}
		out.flush();
	}

	public void reset() {
		size = 0;
		maxEntries = 0;
		entries.clear();
		data.reset();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder((int) length());
		for (Entry entry : entries) {
			sb.append(new String(data.bytes(), entry.offset, entry.length,
					StringUtils.UTF_8));
		}
		return sb.toString();
	}
}
