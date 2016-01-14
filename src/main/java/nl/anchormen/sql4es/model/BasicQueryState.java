package nl.anchormen.sql4es.model;

import java.sql.SQLException;
import java.util.Properties;

import nl.anchormen.sql4es.QueryState;

public class BasicQueryState implements QueryState{

	private Heading heading;
	private Properties props;
	private String sql;
	private SQLException exception = null;

	public BasicQueryState(String sql, Heading heading, Properties props){
		this.heading = heading;
		this.props = props;
		this.sql = sql;
	}
	
	@Override
	public String originalSql() {
		return sql;
	}

	@Override
	public Heading getHeading() {
		return heading;
	}

	@Override
	public void addException(String msg) {
		this.exception = new SQLException(msg);		
	}

	@Override
	public boolean hasException() {
		return this.exception != null;
	}

	@Override
	public SQLException getException() {
		return this.exception;
	}

	@Override
	public int getIntProp(String name, int def) {
		return Utils.getIntProp(props, name, def);
	}

	@Override
	public String getProperty(String name, String def) {
		if(!this.props.containsKey(name)) return def;
		try {
			return this.props.getProperty(name);
		} catch (Exception e) {
			return def;
		}
	}

}