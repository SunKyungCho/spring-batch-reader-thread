package me.study.springbatchreaderthread.job.step;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.AbstractSqlPagingQueryProvider;
import org.springframework.batch.item.database.support.Db2PagingQueryProvider;
import org.springframework.batch.item.database.support.DerbyPagingQueryProvider;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.batch.item.database.support.HsqlPagingQueryProvider;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.OraclePagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlServerPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlitePagingQueryProvider;
import org.springframework.batch.item.database.support.SybasePagingQueryProvider;
import org.springframework.batch.support.DatabaseType;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.util.Assert;

/**
 * This is a builder for the {@link CustomJdbcPagingItemReader}.  When configuring, either a
 * {@link PagingQueryProvider} or the SQL fragments should be provided.  If the SQL
 * fragments are provided, the metadata from the provided {@link DataSource} will be used
 * to create a PagingQueryProvider for you.  If both are provided, the PagingQueryProvider
 * will be used.
 *
 * @author Michael Minella
 * @author Glenn Renfro
 * @author Drummond Dawson
 * @since 4.0
 * @see CustomJdbcPagingItemReader
 */
public class CustomJdbcPagingItemReaderBuilder<T> {

	private DataSource dataSource;

	private int fetchSize = CustomJdbcPagingItemReader.VALUE_NOT_SET;

	private PagingQueryProvider queryProvider;

	private RowMapper<T> rowMapper;

	private Map<String, Object> parameterValues;

	private int pageSize = 10;

	private String groupClause;

	private String selectClause;

	private String fromClause;

	private String whereClause;

	private Map<String, Order> sortKeys;

	private boolean saveState = true;

	private String name;

	private int maxItemCount = Integer.MAX_VALUE;

	private int currentItemCount;

	/**
	 * Configure if the state of the {@link org.springframework.batch.item.ItemStreamSupport} should be persisted within the {@link
	 * org.springframework.batch.item.ExecutionContext} for restart purposes.
	 *
	 * @param saveState defaults to true
	 * @return The current instance of the builder.
	 */
	public CustomJdbcPagingItemReaderBuilder<T> saveState(boolean saveState) {
		this.saveState = saveState;

		return this;
	}

	/**
	 * The name used to calculate the key within the {@link org.springframework.batch.item.ExecutionContext}. Required if {@link #saveState(boolean)}
	 * is set to true.
	 *
	 * @param name name of the reader instance
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.ItemStreamSupport#setName(String)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> name(String name) {
		this.name = name;

		return this;
	}

	/**
	 * Configure the max number of items to be read.
	 *
	 * @param maxItemCount the max items to be read
	 * @return The current instance of the builder.
	 * @see org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader#setMaxItemCount(int)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> maxItemCount(int maxItemCount) {
		this.maxItemCount = maxItemCount;

		return this;
	}

	/**
	 * Index for the current item. Used on restarts to indicate where to start from.
	 *
	 * @param currentItemCount current index
	 * @return this instance for method chaining
	 * @see org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader#setCurrentItemCount(int)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> currentItemCount(int currentItemCount) {
		this.currentItemCount = currentItemCount;

		return this;
	}

	/**
	 * The {@link DataSource} to query against.  Required.
	 *
	 * @param dataSource the {@link DataSource}
	 * @return this instance for method chaining
	 * @see CustomJdbcPagingItemReader#setDataSource(DataSource)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> dataSource(DataSource dataSource) {
		this.dataSource = dataSource;

		return this;
	}

	/**
	 * A hint to the underlying RDBMS as to how many records to return with each fetch.
	 *
	 * @param fetchSize number of records
	 * @return this instance for method chaining
	 * @see CustomJdbcPagingItemReader#setFetchSize(int)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> fetchSize(int fetchSize) {
		this.fetchSize = fetchSize;

		return this;
	}

	/**
	 * The {@link RowMapper} used to map the query results to objects.  Required.
	 *
	 * @param rowMapper a {@link RowMapper} implementation
	 * @return this instance for method chaining
	 * @see CustomJdbcPagingItemReader#setRowMapper(RowMapper)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> rowMapper(RowMapper<T> rowMapper) {
		this.rowMapper = rowMapper;

		return this;
	}

	/**
	 * Creates a {@link BeanPropertyRowMapper} to be used as your {@link RowMapper}.
	 *
	 * @param mappedClass the class for the row mapper
	 * @return this instance for method chaining
	 * @see BeanPropertyRowMapper
	 */
	public CustomJdbcPagingItemReaderBuilder<T> beanRowMapper(Class<T> mappedClass) {
		this.rowMapper = new BeanPropertyRowMapper<>(mappedClass);

		return this;
	}

	/**
	 * A {@link Map} of values to set on the SQL's prepared statement.
	 *
	 * @param parameterValues Map of values
	 * @return this instance for method chaining
	 * @see CustomJdbcPagingItemReader#setParameterValues(Map)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> parameterValues(Map<String, Object> parameterValues) {
		this.parameterValues = parameterValues;

		return this;
	}

	/**
	 * The number of records to request per page/query.  Defaults to 10.  Must be greater than zero.
	 *
	 * @param pageSize number of items
	 * @return this instance for method chaining
	 * @see CustomJdbcPagingItemReader#setPageSize(int)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> pageSize(int pageSize) {
		this.pageSize = pageSize;

		return this;
	}

	public CustomJdbcPagingItemReaderBuilder<T> groupClause(String groupClause) {
		this.groupClause = groupClause;

		return this;
	}

	public CustomJdbcPagingItemReaderBuilder<T> selectClause(String selectClause) {
		this.selectClause = selectClause;

		return this;
	}

	public CustomJdbcPagingItemReaderBuilder<T> fromClause(String fromClause) {
		this.fromClause = fromClause;

		return this;
	}

	public CustomJdbcPagingItemReaderBuilder<T> whereClause(String whereClause) {
		this.whereClause = whereClause;

		return this;
	}

	public CustomJdbcPagingItemReaderBuilder<T> sortKeys(Map<String, Order> sortKeys) {
		this.sortKeys = sortKeys;

		return this;
	}

	/**
	 * A {@link PagingQueryProvider} to provide the queries required.  If provided, the SQL fragments configured via {@link #selectClause(String)},
	 * {@link #fromClause(String)}, {@link #whereClause(String)}, {@link #groupClause}, and {@link #sortKeys(Map)} are ignored.
	 *
	 * @param provider the db specific query provider
	 * @return this instance for method chaining
	 * @see CustomJdbcPagingItemReader#setQueryProvider(PagingQueryProvider)
	 */
	public CustomJdbcPagingItemReaderBuilder<T> queryProvider(PagingQueryProvider provider) {
		this.queryProvider = provider;

		return this;
	}

	/**
	 * Provides a completely built instance of the {@link CustomJdbcPagingItemReader}
	 *
	 * @return a {@link CustomJdbcPagingItemReader}
	 */
	public CustomJdbcPagingItemReader<T> build() {
		Assert.isTrue(this.pageSize > 0, "pageSize must be greater than zero");
		Assert.notNull(this.dataSource, "dataSource is required");

		if (this.saveState) {
			Assert.hasText(this.name,
						   "A name is required when saveState is set to true");
		}

		CustomJdbcPagingItemReader<T> reader = new CustomJdbcPagingItemReader<>();

		reader.setMaxItemCount(this.maxItemCount);
		reader.setCurrentItemCount(this.currentItemCount);
		reader.setName(this.name);
		reader.setSaveState(this.saveState);
		reader.setDataSource(this.dataSource);
		reader.setFetchSize(this.fetchSize);
		reader.setParameterValues(this.parameterValues);

		if (this.queryProvider == null) {
			Assert.hasLength(this.selectClause, "selectClause is required when not providing a PagingQueryProvider");
			Assert.hasLength(this.fromClause, "fromClause is required when not providing a PagingQueryProvider");
			Assert.notEmpty(this.sortKeys, "sortKeys are required when not providing a PagingQueryProvider");

			reader.setQueryProvider(determineQueryProvider(this.dataSource));
		} else {
			reader.setQueryProvider(this.queryProvider);
		}

		reader.setRowMapper(this.rowMapper);
		reader.setPageSize(this.pageSize);

		return reader;
	}

	private PagingQueryProvider determineQueryProvider(DataSource dataSource) {

		try {
			DatabaseType databaseType = DatabaseType.fromMetaData(dataSource);

			AbstractSqlPagingQueryProvider provider;

			switch (databaseType) {

				case DERBY:
					provider = new DerbyPagingQueryProvider();
					break;
				case DB2:
				case DB2VSE:
				case DB2ZOS:
				case DB2AS400:
					provider = new Db2PagingQueryProvider();
					break;
				case H2:
					provider = new H2PagingQueryProvider();
					break;
				case HSQL:
					provider = new HsqlPagingQueryProvider();
					break;
				case SQLSERVER:
					provider = new SqlServerPagingQueryProvider();
					break;
				case MYSQL:
					provider = new MySqlPagingQueryProvider();
					break;
				case ORACLE:
					provider = new OraclePagingQueryProvider();
					break;
				case POSTGRES:
					provider = new PostgresPagingQueryProvider();
					break;
				case SYBASE:
					provider = new SybasePagingQueryProvider();
					break;
				case SQLITE:
					provider = new SqlitePagingQueryProvider();
					break;
				default:
					throw new IllegalArgumentException("Unable to determine PagingQueryProvider type " +
														   "from database type: " + databaseType);
			}

			provider.setSelectClause(this.selectClause);
			provider.setFromClause(this.fromClause);
			provider.setWhereClause(this.whereClause);
			provider.setGroupClause(this.groupClause);
			provider.setSortKeys(this.sortKeys);

			return provider;
		} catch (MetaDataAccessException e) {
			throw new IllegalArgumentException("Unable to determine PagingQueryProvider type", e);
		}
	}
}
