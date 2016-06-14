package nl.anchormen.sql4es.parse.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;

import nl.anchormen.sql4es.QueryState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Column.Operation;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.Utils;

/**
 * A Presto {@link AstVisitor} implementation that parses GROUP BY clauses
 *
 * @author cversloot
 */
public class GroupParser extends SelectParser {

  public AggregationBuilder parse(List<GroupingElement> elements,final QueryState state) {
    List<Column> groups = new ArrayList<Column>();
    for (GroupingElement grouping : elements) {
      for (Set<Expression> expressions : grouping.enumerateGroupingSets()) {
        for (Expression e : expressions) {
          Column column = (Column) e.accept(this, state);
          if (e instanceof FunctionCall) {
            List<Expression> args = ((FunctionCall) e).getArguments();
            if (args.size() > 1) {
              List<Object> opArgs = new ArrayList<>();
              for (int i = 1; i < args.size(); i++) {                
                opArgs.add(args.get(i).accept(new AstVisitor() {
                  @Override
                  protected Object visitExpression(Expression node, Object context) {
                    if (node instanceof StringLiteral) {
                      return ((StringLiteral) node).getValue();
                    }
                    if (node instanceof LongLiteral) {
                      return ((LongLiteral) node).getValue();
                    }
                    state.addException("Unable to parse type " + node.getClass() + " in Select");
                    return null;
                  }
                }, state));
              }
              column.setOpArgs(opArgs);
            }
          }
          groups.add(column);
        }
      }
    }

    // to find case sensitive group by definitions which ES needs
    for (Column groupby : groups) {
      if (groupby.getOp() != Operation.NONE && groupby.getOp() != Operation.DATE_HISTOGRAM) {
        state.addException("Can not use function '" + groupby.getAggName() + "' as GROUP BY, please use an alias to group by a function");
        return null;
      }
    }
    Heading.fixColumnReferences(state.originalSql() + ";", "group by.+", "\\W", groups);

    for (Column g : groups) {
      Column s = state.getHeading().getColumnByLabel(g.getAggName());
      if (s == null) {
        state.addException("Group by '" + g.getColumn() + "' not defined in SELECT");
      } else {
        // add column from select to this group (when referenced through an alias)
        g.setColumn(s.getColumn());
      }
    }
    return buildAggregationQuery(groups, 0, state);
  }

  /**
   * Adds aggregations recursively
   * All metric columns are added to last aggregation
   *
   * @param aggs
   * @param index
   * @param state
   * @return
   */
  private AggregationBuilder buildAggregationQuery(List<Column> aggs, int index, QueryState state) {
    Column agg = aggs.get(index);
    AggregationBuilder result = null;
    Column column = aggs.get(index);
    switch (column.getOp()) {
      case NONE:
        TermsBuilder tb = AggregationBuilders.terms(agg.getAggName()).field(agg.getColumn());
        tb.size(state.getIntProp(Utils.PROP_FETCH_SIZE, 10000));
        result = tb;
        break;
      case DATE_HISTOGRAM:
        DateHistogramBuilder db = AggregationBuilders.dateHistogram(agg.getAggName()).field(agg.getColumn());
        long interval;
        Object arg = column.getOpArgs().get(0);
        if(arg instanceof Number){
        	interval = ((Number)arg).longValue();
        } else if(arg instanceof String){
        	String str = (String)arg;
        	char unit=0;
        	int end = str.length();
        	if(str.length()>0){
						unit = str.charAt(end-1);
						if(!Character.isDigit(unit)){
							end--;
						}
        	}
					try{
						interval = Integer.parseInt(str.substring(0,end));
					}catch(NumberFormatException e){
						interval = 24*60*6000;;
					}
					switch(unit){
					case 's':
						interval*=1000;
						break;
					case 'm':
						interval*=60000;						
						break;
					case 'h':
						interval*=60*60000;													
						break;
					case 'd':
						interval*=24*60*60000;
						break;
					case 'w':
						interval*=7*24*60*60000;
						break;
					case 'M':
						interval*=30*24*60*60000;
						break;
					case 'y':
						interval*=365*24*60*60000;
						break;
					}
        } else {
        	interval = 24*60*6000;;
        }
        db.interval(interval);
        result = db;
        break;
    }
    if (index < aggs.size() - 1) result.subAggregation(buildAggregationQuery(aggs, index + 1, state));
    else addMetrics(result, state.getHeading(), true);
    return result;
  }

  /**
   * Adds a Filtered Aggregation used to aggregate all results for a query without having a Group By
   */
  public FilterAggregationBuilder buildFilterAggregation(QueryBuilder query, Heading heading) {
    FilterAggregationBuilder filterAgg = AggregationBuilders.filter("filter").filter(query);
    addMetrics(filterAgg, heading, false);
    return filterAgg;
  }

  /**
   * Adds a set of 'leaf aggregations' to the provided parent metric (i.e. count, sum, max etc)
   *
   * @param parentAgg
   * @param heading
   * @param addCount
   */
  @SuppressWarnings("rawtypes")
  private void addMetrics(AggregationBuilder parentAgg, Heading heading, boolean addCount) {
    for (Column metric : heading.columns()) {
      if (metric.getOp() == Operation.AVG)
        parentAgg.subAggregation(AggregationBuilders.avg(metric.getAggName()).field(metric.getColumn()));
      else if (addCount && metric.getOp() == Operation.COUNT)
        parentAgg.subAggregation(AggregationBuilders.count(metric.getAggName()));
      else if (metric.getOp() == Operation.MAX)
        parentAgg.subAggregation(AggregationBuilders.max(metric.getAggName()).field(metric.getColumn()));
      else if (metric.getOp() == Operation.MIN)
        parentAgg.subAggregation(AggregationBuilders.min(metric.getAggName()).field(metric.getColumn()));
      else if (metric.getOp() == Operation.SUM)
        parentAgg.subAggregation(AggregationBuilders.sum(metric.getAggName()).field(metric.getColumn()));
    }
  }

  public AggregationBuilder addDistinctAggregation(QueryState state) {
    List<Column> distinct = new ArrayList<Column>();
    for (Column s : state.getHeading().columns()) {
      if (s.getOp() == Operation.NONE && s.getCalculation() == null) distinct.add(s);
    }
    return buildAggregationQuery(distinct, 0, state);
  }

  @Override
  protected Object visitLongLiteral(LongLiteral node, QueryState context) {
    return super.visitLongLiteral(node, context);
  }
}
