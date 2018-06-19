package org.apache.oozie.util.db;

import java.util.List;

import org.apache.oozie.util.XLog;

import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import net.ttddyy.dsproxy.listener.logging.AbstractQueryLoggingListener;

public class DBProxyLogger extends AbstractQueryLoggingListener {
    private static XLog LOG = XLog.getLog(DBProxyLogger.class);

    @Override
    public void afterQuery(ExecutionInfo execInfo, List<QueryInfo> queryInfoList) {
        boolean selectOnly = true;

        for (QueryInfo info : queryInfoList) {
            String query = info.getQuery().trim().toLowerCase();
            if (!query.startsWith("select")) {
                selectOnly = false;
            }
        }

        if (!selectOnly) {
            final String entry = getEntry(execInfo, queryInfoList);
            writeLog(entry);
        } else {
            LOG.info("Select-only query - not logging");
        }
    }

    @Override
    protected void writeLog(String message) {
        Exception e = new Exception("trace");
        // LOG.info(message, e);
        System.out.println(message);
        // e.printStackTrace(System.out);
    }
}
