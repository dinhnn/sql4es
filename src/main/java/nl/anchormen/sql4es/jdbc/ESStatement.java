package nl.anchormen.sql4es.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Use;

import nl.anchormen.sql4es.ESPipelineResultSet;
import nl.anchormen.sql4es.ESQueryState;
import nl.anchormen.sql4es.ESResultSet;
import nl.anchormen.sql4es.ESUpdateState;
import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.Utils;
import nl.anchormen.sql4es.parse.sql.ParseResult;

public class ESStatement implements Statement {

	private static final SqlParser parser = new SqlParser();
	protected ESConnection connection;
	
	protected int queryTimeoutSec = 10;
	protected boolean poolable = true;
	protected boolean closeOnCompletion = false;
	protected ResultSet result;

	protected ESQueryState queryState;
	protected ESUpdateState updateState;
	
	public ESStatement(ESConnection connection) throws SQLException{
		this.connection = connection;
		this.queryState = new ESQueryState(connection.getClient(), this);
		updateState = new ESUpdateState(connection.getClient(), this);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		//System.out.println("QUERY: ["+sql+"]");
		if(connection.getSchema() == null) throw new SQLException("No active index set for this driver. Pleas specify an active index or alias by executing 'USE <index/alias>' first");
		sql = sql.replaceAll("\r", " ").replaceAll("\n", " ");
		com.facebook.presto.sql.tree.Statement statement = parser.createStatement(sql);
		if(statement instanceof Query){
			if(this.result != null) this.result.close();
			ParseResult parseResult = queryState.buildRequest(sql, ((Query)statement).getQueryBody(), connection.getSchema());
			this.result = queryState.execute();
			while((parseResult = parseResult.getParent())!=null){
        ESPipelineResultSet resultSet = new ESPipelineResultSet(parseResult.getHeading(),this.result);
				for(Column column:parseResult.getHeading().columns()) {
					if(column.getOp()==null) throw new SQLException("Unsupport");
					switch (column.getOp()){
						case MAX:
							Double max = null;
							while(this.result.next()){
								double num = this.result.getDouble(column.getColumn());
								max = max==null?num:Math.max(num,max);
							}
              resultSet.add(max);
							this.result.beforeFirst();
							break;
						case MIN:
							Double min = null;
							while(this.result.next()){
								double num = this.result.getDouble(column.getColumn());
								min = min==null?num:Math.max(num,min);
							}
              resultSet.add(min);
							this.result.beforeFirst();
							break;
						case AVG:
							double sum = 0;
							int count=0;
							while(this.result.next()){
								double num = this.result.getDouble(column.getColumn());
								sum+=num;
								count++;
							}
              resultSet.add(count==0?null:sum/count);
							this.result.beforeFirst();
							break;
            case GROWTH:
              double avgX = 0; double avgY = 0; double avgXY = 0; double avgXX = 0;
              int n = 0;

              String knowX = null;
              double newX=0;
              List<Object> opArgs = column.getOpArgs();
              if(opArgs!=null && opArgs.size()>1){
                knowX = opArgs.get(0).toString();
                newX = ((Number)opArgs.get(1)).doubleValue();
              }
              String knowY = column.getColumn();

              while(this.result.next()){
                double x;

                if(knowX==null){
                  x = n;
                } else {
                  Object ox = this.result.getObject(knowX);
                  if(ox instanceof Date)x = ((Date)ox).getTime();
                  else if(ox instanceof Number)x = ((Number)ox).doubleValue();
                  else x = n;
                }
                double y = Math.log(this.result.getDouble(knowY));
                avgX += x; avgY += y; avgXY += x*y; avgXX += x*x;
                n++;
              }
              if(knowX==null)newX = n;
              avgX /= n; avgY /= n; avgXY /= n; avgXX /= n;

              double beta = (avgXY - avgX*avgY) / (avgXX - avgX*avgX);
              double alpha = avgY - beta*avgX;
              resultSet.add(Math.exp( alpha + beta * newX));
              break;
					}
				}
				this.result = resultSet;
			}
			return this.result;
		}else if(statement instanceof Explain){
			String ex = queryState.explain(sql, (Explain)statement, connection.getSchema());
			if(this.result != null) this.result.close();
			Heading heading = new Heading();
			heading.add(new Column("Explanation"));
			ESResultSet rs = new ESResultSet(heading, 1, 1);
			List<Object> row = rs.getNewRow();
			row.set(0, ex);
			rs.add(row);
			this.result = rs;
			return result;
		}else throw new SQLException("Provided query is not a SELECT or EXPLAIN query");
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		//System.out.println("QUERY: ["+sql+"]");
		sql = sql.replaceAll("\r", " ").replaceAll("\n", " ").trim();
		// custom stuff to support UPDATE statements since Presto does not parse it
		if(sql.toLowerCase().startsWith("update")){
			return updateState.execute(sql);
		}
		
		com.facebook.presto.sql.tree.Statement statement = parser.createStatement(sql);
		if(statement instanceof Query) throw new SQLException("A regular query cannot be executed as an Update");
		if(statement instanceof Insert){
			//if(connection.getSchema() == null) throw new SQLException("No active index set for this driver. Pleas specify an active index or alias by executing 'USE <index/alias>' first");
			return updateState.execute(sql, (Insert)statement, connection.getSchema());
		}else if(statement instanceof Delete){
			if(connection.getSchema() == null) throw new SQLException("No active index set for this driver. Pleas specify an active index or alias by executing 'USE <index/alias>' first");
			return updateState.execute(sql, (Delete)statement, connection.getSchema());
		}else if(statement instanceof CreateTable){
			return updateState.execute(sql, (CreateTable)statement, connection.getSchema());
		}else if(statement instanceof CreateTableAsSelect){
			return updateState.execute(sql, (CreateTableAsSelect)statement, connection.getSchema());
		}else if(statement instanceof CreateView){
			return updateState.execute(sql, (CreateView)statement, connection.getSchema());
		}else if(statement instanceof Use){
			connection.setSchema( ((Use)statement).getSchema());
			connection.getTypeMap(); // updates the type mappings found in properties
			return 0;
		}else if(statement instanceof DropTable){
			return updateState.execute(sql, (DropTable)statement);
		}else if(statement instanceof DropView){
			return updateState.execute(sql, (DropView)statement);
		}throw new SQLFeatureNotSupportedException("Unable to parse provided update sql");
	}

	@Override
	public void close() throws SQLException {
		queryState.close();
		updateState.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getMaxRows() throws SQLException {
		return this.queryState.getMaxRows();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		this.queryState.setMaxRows(max);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return queryTimeoutSec;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		this.queryTimeoutSec = seconds;
	}

	@Override
	public void cancel() throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		//System.out.println("QUERY: ["+sql+"]");
		sql = sql.replaceAll("\r", " ").replaceAll("\n", " ");
		String sqlNorm = sql.trim().toLowerCase();
		if(sqlNorm.startsWith("select") || sqlNorm.startsWith("explain")) {
			this.result = this.executeQuery(sql);
			return result != null;
		}else if(sqlNorm.startsWith("insert") || sqlNorm.startsWith("delete")
				|| sqlNorm.startsWith("create") || sqlNorm.startsWith("use") ||
				sqlNorm.startsWith("drop")) {
			this.executeUpdate(sql);
			return false;
		}else throw new SQLException("Provided query type '"+sql.substring(0, sql.indexOf(' '))+"' is not supported");
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return this.result;
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return -1;
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		this.result = queryState.moreResutls(Utils.getBooleanProp(this.connection.getClientInfo(), Utils.PROP_RESULT_NESTED_LATERAL, true));
		return result != null;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		// Not able to set this one
	}

	@Override
	public int getFetchSize() throws SQLException {
		return Utils.getIntProp(getConnection().getClientInfo(), Utils.PROP_FETCH_SIZE, 10000);
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetType() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		sql = sql.trim().replaceAll("\r", " ").replaceAll("\n", " ");
		updateState.addToBulk(sql, this.getConnection().getSchema());
	}

	@Override
	public void clearBatch() throws SQLException {
		this.updateState.clearBulk();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		return this.updateState.executeBulk();
	}

	@Override
	public Connection getConnection() throws SQLException {
		return this.connection;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException {
		// TODO use current param
		ResultSet newResult = queryState.moreResutls(Utils.getBooleanProp(this.connection.getClientInfo(), Utils.PROP_RESULT_NESTED_LATERAL, true));
		if(newResult == null) return false;
		this.result = newResult;
		return true;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		return this.executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		return this.executeUpdate(sql);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		return this.executeUpdate(sql);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		return this.execute(sql);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		return this.execute(sql);
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		return this.execute(sql);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return connection.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		this.poolable = poolable;
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return poolable;
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		this.closeOnCompletion = true;
		
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return closeOnCompletion;
	}

}
