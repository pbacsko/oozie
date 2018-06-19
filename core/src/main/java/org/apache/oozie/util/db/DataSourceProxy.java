package org.apache.oozie.util.db;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;

public class DataSourceProxy implements DataSource {
    
    private final BasicDataSourceWrapper wrappedDataSource;

    private final DataSource proxy;
 
    public DataSourceProxy() {
        wrappedDataSource = new BasicDataSourceWrapper();
        proxy = ProxyDataSourceBuilder.create(wrappedDataSource)
                  .listener(new DBProxyLogger())
                  .build();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return proxy.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        proxy.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        proxy.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return proxy.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return proxy.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return proxy.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return proxy.isWrapperFor(iface);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return proxy.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return proxy.getConnection(username, password);
    }
    
    public void setUsername(String username) {
        wrappedDataSource.setUsername(username);
    }

    public void setPassword(String password) {
        wrappedDataSource.setPassword(password);
    }

    public void setUrl(String url) {
        wrappedDataSource.setUrl(url);
    }

    public String getUrl() {
        return wrappedDataSource.getUrl();
    }

    public void setDriverClassName(String driverClassName) {
        wrappedDataSource.setDriverClassName(driverClassName);
    }
    
    public String getDriverClassName() {
        return wrappedDataSource.getDriverClassName();
    }

    public void setDriverClassLoader(
            ClassLoader driverClassLoader) {
        wrappedDataSource.setDriverClassLoader(driverClassLoader);
    }
    
    public ClassLoader getDriverClassLoader() {
        return wrappedDataSource.getDriverClassLoader();
    }
}
