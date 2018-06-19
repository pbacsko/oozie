package org.apache.oozie.util.db.wrapper;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class HSQLDriverWrapper extends org.hsqldb.jdbcDriver {

    public Connection connect(final String url,
            final Properties info) throws SQLException {
        return new HSQLConnectionWrapper(super.connect(url, info));
    }

}
