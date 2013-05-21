package org.opennms.netmgt.storage.sample.cassandra.internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.opennms.netmgt.api.sample.Metric;
import org.opennms.netmgt.api.sample.Resource;
import org.opennms.netmgt.api.sample.Results;
import org.opennms.netmgt.api.sample.Results.Row;
import org.opennms.netmgt.api.sample.Sample;
import org.opennms.netmgt.api.sample.SampleProcessor;
import org.opennms.netmgt.api.sample.Timestamp;

import com.datastax.driver.core.ResultSet;


public class CassandraAdapter extends SampleProcessor {
	private Resource m_resource;
	private Metric[] m_metrics;
	private Iterator<com.datastax.driver.core.Row> m_resultIterator;
	private com.datastax.driver.core.Row m_peeked;

	public CassandraAdapter(Resource resource, Metric[] metrics, ResultSet resultSet) {
		m_resource = resource;
		m_metrics = metrics;
		m_resultIterator = resultSet.iterator();
		if (m_resultIterator.hasNext()) {
			m_peeked = m_resultIterator.next();
		}
	}

	@Override
	public boolean hasNext() {
		return m_peeked != null;
	}
	
	private Timestamp collectedAt(com.datastax.driver.core.Row row) {
		Date collectedAt = row.getDate(CassandraStorage.F_COLLECTED_AT);
		return new Timestamp(collectedAt);
	}
	
	private String metricName(com.datastax.driver.core.Row row) {
		return m_peeked.getString(CassandraStorage.F_METRIC);
	}
	
	private double metricValue(com.datastax.driver.core.Row row) {
		return m_peeked.isNull(CassandraStorage.F_VALUE) ? Double.NaN : m_peeked.getDouble(CassandraStorage.F_VALUE);
	}

	@Override
	public Collection<Metric> getMetrics() {
		return Arrays.asList(m_metrics);
	}

	@Override
	public Row next() {

		if (m_peeked == null) {
			throw new NoSuchElementException("Attempt to get a element when no further element exists");
		}

		Timestamp timestamp = collectedAt(m_peeked);
		Row resultRow = new Results.Row(timestamp);

		do {
			String metricName = metricName(m_peeked);
			double metricValue = metricValue(m_peeked); // return NaN for null values

			for (Metric metric : m_metrics) {
				if (metricName.equals(metric.getName())) {
					Sample m = new Sample(m_resource, metric, timestamp, metricValue);
					resultRow.addSample(m);
				}
			}

			m_peeked = m_resultIterator.hasNext() ? m_resultIterator.next() : null;

		} while(m_peeked != null && timestamp.equals(collectedAt(m_peeked)));

		// Fill in any samples we didn't find in the DB w/ NaN
		for (Metric metric : m_metrics) {
			if (!resultRow.containsSample(metric)) {
				resultRow.addSample(new Sample(m_resource, metric, timestamp, Double.NaN));
			}
		}

		return resultRow;
	}
}
