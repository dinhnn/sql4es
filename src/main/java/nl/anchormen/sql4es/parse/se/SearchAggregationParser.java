package nl.anchormen.sql4es.parse.se;

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.joda.time.DateTime;

import nl.anchormen.sql4es.ESResultSet;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Column.Operation;
import nl.anchormen.sql4es.model.Utils;

/**
 * Parses aggregation part of elasticsearch result. 
 * @author cversloot
 *
 */
public class SearchAggregationParser {

	/**
	 * Parses an ES aggregation into a set of ResultRows
	 * @param agg
	 * @return
	 * @throws SQLException
	 */
	public void parseAggregation(Aggregation agg, ESResultSet rs) throws SQLException{
		if(agg instanceof Terms) {
			dfsAggregations((Terms) agg, rs, rs.getNewRow());
		}else if(agg instanceof Histogram){
			dfsAggregations((Histogram) agg, rs, rs.getNewRow());
		}else if (agg instanceof InternalFilter){
			processFilterAgg((InternalFilter)agg, rs);
		}else throw new SQLException ("Unknown aggregation type "+agg.getClass().getName());
	}
	
	/**
	 * Parse an aggregation result based on one or more aggregated terms
	 * @param terms
	 * @param rs
	 * @param row
	 * @throws SQLException
	 */
	private void dfsAggregations(Terms terms, ESResultSet rs, List<Object> row) throws SQLException{
		List<Object> currentRow = Utils.clone(row);
		String columnName = terms.getName();
		if(!rs.getHeading().hasLabel(columnName)) throw new SQLException("Unable to identify column for aggregation named "+columnName);
		Column aggCol = rs.getHeading().getColumnByLabel(columnName);
		for(Terms.Bucket bucket : terms.getBuckets()){
			boolean metricAggs = false;
			List<Aggregation> aggs = bucket.getAggregations().asList();
			if(aggs.size() == 0){
				currentRow.set(aggCol.getIndex(), bucket.getKey());
				metricAggs = true;
			}else for(Aggregation agg : bucket.getAggregations().asList()){
				if(agg instanceof Terms){
					currentRow.set(aggCol.getIndex(), bucket.getKey());
					dfsAggregations((Terms)agg, rs, currentRow);
				}else{
					if(metricAggs == false){
						currentRow.set(aggCol.getIndex(), bucket.getKey());
						metricAggs = true;
					}
					String metricName = agg.getName();
					if(!rs.getHeading().hasLabel(metricName)) throw new SQLException("Unable to identify column for aggregation named "+metricName);
					Column metricCol = rs.getHeading().getColumnByLabel(metricName);
					currentRow.set(metricCol.getIndex(), agg.getProperty("value"));
				}
			}
			if(metricAggs){
				rs.add(currentRow);
				currentRow = Utils.clone(row);
			}
			currentRow = Utils.clone(row);
		}
	}
	private void dfsAggregations(Histogram histogram, ESResultSet rs, List<Object> row) throws SQLException{
		List<Object> currentRow = Utils.clone(row);
		String columnName = histogram.getName();
		if(!rs.getHeading().hasLabel(columnName)) throw new SQLException("Unable to identify column for aggregation named "+columnName);
		Column aggCol = rs.getHeading().getColumnByLabel(columnName);
		for(Histogram.Bucket bucket : histogram.getBuckets()){
			boolean metricAggs = false;
			List<Aggregation> aggs = bucket.getAggregations().asList();
			if(aggs.size() == 0){
				currentRow.set(aggCol.getIndex(), bucket.getKey());
				metricAggs = true;
			}else for(Aggregation agg : bucket.getAggregations().asList()){
				if(agg instanceof Terms){
					currentRow.set(aggCol.getIndex(), bucket.getKey());
					dfsAggregations((Terms)agg, rs, currentRow);
				}else{
					if(metricAggs == false){
						Object key = bucket.getKey();
						if(key instanceof DateTime){
							key = new Date(((DateTime)key).getMillis());
						}
						currentRow.set(aggCol.getIndex(),key);
						metricAggs = true;
					}
					String metricName = agg.getName();
					if(!rs.getHeading().hasLabel(metricName)) throw new SQLException("Unable to identify column for aggregation named "+metricName);
					Column metricCol = rs.getHeading().getColumnByLabel(metricName);
					Object value = agg.getProperty("value");					
					if(value instanceof Number){
						double d = ((Number)value).doubleValue();
						if(Double.isNaN(d)||Double.isInfinite(d)){
							value = null;
						}							
					}
					currentRow.set(metricCol.getIndex(),value);
				}
			}
			if(metricAggs){
				rs.add(currentRow);
				currentRow = Utils.clone(row);
			}
			currentRow = Utils.clone(row);
		}
	}
	/**
	 * Parse an aggregation performed without grouping.
	 * @param filter
	 * @param rs
	 * @throws SQLException
	 */
	private void processFilterAgg(InternalFilter filter, ESResultSet rs) throws SQLException{
		//String name = global.getName(); // we do not care about the global name for now
		List<Object> row = rs.getNewRow();
		Column count = null;
		for(Column c : rs.getHeading().columns())
			if(c.getOp() == Operation.COUNT) count = c;
		
		if(count != null){
			row.set(count.getIndex(), filter.getDocCount());
		}
		for(Aggregation agg : filter.getAggregations()){
			if(agg instanceof InternalNumericMetricsAggregation.SingleValue){
				InternalNumericMetricsAggregation.SingleValue numericAgg = 
						(InternalNumericMetricsAggregation.SingleValue)agg;
				String name =numericAgg.getName();
				Column column = rs.getHeading().getColumnByLabel(name);
				if(column == null){
					throw new SQLException("Unable to identify column for "+name);
				}
				row.set(column.getIndex(), numericAgg.value());
			}else throw new SQLException("Unable to parse aggregation of type "+agg.getClass());
		}
		rs.add(row);
	}
	
}
