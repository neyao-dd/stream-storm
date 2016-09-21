package cn.com.deepdata.esstorm;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.hadoop.rest.HttpStatus;

public class BulkResponse {
	static BulkResponse ok(int totalWrites) {
		return new BulkResponse(totalWrites);
	}

	private final int httpStatus;
	private final int totalWrites;
	private final BitSet leftovers;
	private final List<String> errorExamples;
	private Map<Integer, Map<String, String>> esIdMapping;
	private Map<Integer, String> unrecoverableError;

	/**
	 * Creates a bulk response denoting that everything is OK
	 * 
	 * @param totalWrites
	 */
	private BulkResponse(int totalWrites) {
		this(HttpStatus.OK, totalWrites, new BitSet(), Collections
				.<String> emptyList());
	}

	public BulkResponse(int httpStatus, int totalWrites, BitSet leftovers,
			List<String> errorExamples) {
		this.httpStatus = httpStatus;
		this.totalWrites = totalWrites;
		this.leftovers = leftovers;
		this.errorExamples = errorExamples;
	}

	public int getHttpStatus() {
		return httpStatus;
	}

	public int getTotalWrites() {
		return totalWrites;
	}

	public BitSet getLeftovers() {
		return leftovers;
	}

	public List<String> getErrorExamples() {
		return errorExamples;
	}

	public Map<Integer, Map<String, String>> getEsIdMapping() {
		return esIdMapping;
	}

	public void setEsIdMapping(Map<Integer, Map<String, String>> esIdMapping) {
		this.esIdMapping = esIdMapping;
	}

	public Map<Integer, String> getUnrecoverableError() {
		return unrecoverableError;
	}

	public void setUnrecoverableError(Map<Integer, String> unrecoverableError) {
		this.unrecoverableError = unrecoverableError;
	}
}
