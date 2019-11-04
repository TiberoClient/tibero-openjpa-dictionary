package com.tmax.tibero.openjpa;

import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

import org.apache.openjpa.jdbc.conf.JDBCConfiguration;
import org.apache.openjpa.jdbc.identifier.DBIdentifier;
import org.apache.openjpa.jdbc.identifier.DBIdentifierUtil;
import org.apache.openjpa.jdbc.kernel.JDBCFetchConfiguration;
import org.apache.openjpa.jdbc.kernel.JDBCStore;
import org.apache.openjpa.jdbc.kernel.exps.FilterValue;
import org.apache.openjpa.jdbc.schema.Column;
import org.apache.openjpa.jdbc.schema.ForeignKey;
import org.apache.openjpa.jdbc.schema.ForeignKey.FKMapKey;
import org.apache.openjpa.jdbc.schema.Index;
import org.apache.openjpa.jdbc.schema.PrimaryKey;
import org.apache.openjpa.jdbc.schema.Schema;
import org.apache.openjpa.jdbc.schema.Table;
import org.apache.openjpa.jdbc.sql.*;
import org.apache.openjpa.lib.jdbc.DelegatingDatabaseMetaData;
import org.apache.openjpa.lib.jdbc.DelegatingPreparedStatement;
import org.apache.openjpa.lib.log.Log;
import org.apache.openjpa.lib.util.J2DoPrivHelper;
import org.apache.openjpa.lib.util.Localizer;
import org.apache.openjpa.util.StoreException;
import org.apache.openjpa.util.UserException;

public class TiberoDictionary
        extends DBDictionary {
    public static final String SELECT_HINT = "openjpa.hint.TiberoSelectHint";
    public static final String VENDOR_TIBERO = "tibero";
    private static final Localizer _loc = Localizer.forPackage(TiberoDictionary.class);
    public boolean useTriggersForAutoAssign = false;
    public String autoAssignSequenceName = null;
    public boolean openjpa3GeneratedKeyNames = false;
    public String xmlTypeMarker = "XMLType(?)";
    private boolean _warnedCharColumn = false;
    private Class TbPreparedStatementClass = null;
    private int defaultBatchLimit = 100;

    public TiberoDictionary() {
        this.platform = "Tibero";
        this.validationSQL = "SELECT SYSDATE FROM DUAL";
        this.nextSequenceQuery = "SELECT {0}.NEXTVAL FROM DUAL";
        this.stringLengthFunction = "LENGTH({0})";
        this.joinSyntax = SYNTAX_DATABASE;
        this.maxTableNameLength = 30;
        this.maxColumnNameLength = 30;
        this.maxIndexNameLength = 30;
        this.maxConstraintNameLength = 30;
        this.maxEmbeddedBlobSize = 4000;
        this.maxEmbeddedClobSize = 4000;
        this.inClauseLimit = 1000;

        this.supportsDeferredConstraints = true;
        this.supportsLockingWithDistinctClause = false;
        this.supportsSelectStartIndex = true;
        this.supportsSelectEndIndex = true;

        this.systemSchemaSet.addAll(Arrays.asList(new String[]{
                "CTXSYS", "MDSYS", "SYS", "SYSTEM", "WKSYS", "WMSYS", "XDB"}));

        this.supportsXMLColumn = true;
        this.xmlTypeName = "XMLType";
        this.bigintTypeName = "NUMBER{0}";
        this.bitTypeName = "NUMBER{0}";
        this.decimalTypeName = "NUMBER{0}";
        this.doubleTypeName = "NUMBER{0}";
        this.integerTypeName = "NUMBER{0}";
        this.numericTypeName = "NUMBER{0}";
        this.smallintTypeName = "NUMBER{0}";
        this.tinyintTypeName = "NUMBER{0}";
        this.longVarcharTypeName = "LONG";
        this.binaryTypeName = "BLOB";
        this.varbinaryTypeName = "BLOB";
        this.longVarbinaryTypeName = "BLOB";
        this.timeTypeName = "DATE";
        this.timeWithZoneTypeName = "DATE";
        this.varcharTypeName = "VARCHAR2{0}";
        this.fixedSizeTypeNameSet.addAll(Arrays.asList(new String[]{
                "LONG RAW", "RAW", "LONG"}));

        this.reservedWordSet.addAll(Arrays.asList(new String[]{
                "ACCESS", "AUDIT", "CLUSTER", "COMMENT", "COMPRESS", "EXCLUSIVE",
                "FILE", "IDENTIFIED", "INCREMENT", "INDEX", "INITIAL", "LOCK",
                "LONG", "MAXEXTENTS", "MINUS", "MODE", "NOAUDIT", "NOCOMPRESS",
                "NOWAIT", "OFFLINE", "ONLINE", "PCTFREE", "ROW"}));

        this.invalidColumnWordSet.addAll(Arrays.asList(new String[]{
                "ACCESS", "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC",
                "AUDIT", "BETWEEN", "BY", "CHAR", "CHECK", "CLUSTER", "COLUMN",
                "COMMENT", "COMPRESS", "CONNECT", "CREATE", "CURRENT", "DATE",
                "DECIMAL", "DEFAULT", "DELETE", "DESC", "DISTINCT", "DROP", "ELSE",
                "END-EXEC", "EXCLUSIVE", "EXISTS", "FILE", "FLOAT", "FOR", "FROM",
                "GRANT", "GROUP", "HAVING", "IDENTIFIED", "IMMEDIATE", "IN",
                "INCREMENT", "INDEX", "INITIAL", "INSERT", "INTEGER", "INTERSECT",
                "INTO", "IS", "LEVEL", "LIKE", "LOCK", "LONG", "MAXEXTENTS",
                "MINUS", "MODE", "NOAUDIT", "NOCOMPRESS", "NOT", "NOWAIT", "NULL",
                "NUMBER", "OF", "OFFLINE", "ON", "ONLINE", "OPTION", "OR", "ORDER",
                "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "REVOKE", "ROW",
                "ROWS", "SELECT", "SESSION", "SET", "SIZE", "SMALLINT", "TABLE",
                "THEN", "TO", "UNION", "UNIQUE", "UPDATE", "USER", "VALUES",
                "VARCHAR", "VIEW", "WHENEVER", "WHERE", "WITH"}));

        this.substringFunctionName = "SUBSTR";
        super.setBatchLimit(this.defaultBatchLimit);
        this.selectWordSet.add("WITH");
        this.reportsSuccessNoInfoOnBatchUpdates = true;
        try {
            this.TbPreparedStatementClass = Class.forName("com.tmax.tibero.jdbc.TbPreparedStatement");
        } catch (ClassNotFoundException localClassNotFoundException) {
        }
    }


    public void endConfiguration() {
        super.endConfiguration();
        if (this.useTriggersForAutoAssign) {
            this.supportsAutoAssign = true;
        }
    }

    public void connectedConfiguration(Connection conn) throws SQLException {
        super.connectedConfiguration(conn);
        if (driverVendor == null) {
            DatabaseMetaData meta = conn.getMetaData();
            String url = (meta.getURL() == null) ? "" : meta.getURL();
            String driverName = meta.getDriverName();
            String metadataClassName;

            if (meta instanceof DelegatingDatabaseMetaData)
                metadataClassName =
                        ((DelegatingDatabaseMetaData) meta).getInnermostDelegate().getClass().getName();
            else
                metadataClassName = meta.getClass().getName();

            if (metadataClassName.startsWith("com.tmax.tibero.") ||
                    url.indexOf("jdbc:tibero:") != -1 ||
                    "Tibero JDBC driver".equals(driverName)) {
                driverVendor =
                        VENDOR_TIBERO + meta.getDriverMajorVersion() +
                                meta.getDriverMinorVersion();

                String productVersion = meta.getDatabaseProductVersion();

                if (Integer.parseInt(productVersion) <= 3)
                    supportsXMLColumn = false;
            } else {
                driverVendor = VENDOR_OTHER;
            }
        }
    }

    public boolean supportsLocking(Select sel) {
        if (!super.supportsLocking(sel)) {
            return false;
        }
        return !requiresSubselectForRange(sel.getStartIndex(),
                sel.getEndIndex(), sel.isDistinct(), sel.getOrdering());
    }

    protected SQLBuffer getSelects(Select sel, boolean distinctIdentifiers, boolean forUpdate) {
        if (!requiresSubselectForRange(sel.getStartIndex(), sel
                .getEndIndex(), sel.isDistinct(), sel.getOrdering())) {
            return super.getSelects(sel, distinctIdentifiers, forUpdate);
        }
        if ((sel.getFromSelect() != null) || (sel.getTableAliases().size() < 2)) {
            return super.getSelects(sel, distinctIdentifiers, forUpdate);
        }
        SQLBuffer selectSQL = new SQLBuffer(this);
        List<?> aliases;
        if (distinctIdentifiers) {
            aliases = sel.getIdentifierAliases();
        } else {
            aliases = sel.getSelectAliases();
        }

        Object alias;
        int i = 0;

        for (Iterator itr = aliases.iterator(); itr.hasNext(); i++) {
            alias = itr.next();
            String asString = null;
            if ((alias instanceof SQLBuffer)) {
                asString = ((SQLBuffer) alias).getSQL();
                selectSQL.appendParamOnly((SQLBuffer) alias);
            } else {
                asString = alias.toString();
            }
            selectSQL.append(asString);
            if (asString.indexOf(" AS ") == -1) {
                selectSQL.append(" AS c").append(String.valueOf(i));
            }
            if (itr.hasNext()) {
                selectSQL.append(", ");
            }
        }
        return selectSQL;
    }

    public boolean canOuterJoin(int syntax, ForeignKey fk) {
        if (!super.canOuterJoin(syntax, fk)) {
            return false;
        }
        if ((fk != null) && (syntax == SYNTAX_DATABASE)) {
            if (fk.getConstants().length > 0) {
                return false;
            }
            if (fk.getPrimaryKeyConstants().length > 0) {
                return false;
            }
        }
        return true;
    }

    public SQLBuffer toNativeJoin(Join join) {
        if (join.getType() != 1) {
            return toTraditionalJoin(join);
        }
        ForeignKey fk = join.getForeignKey();
        if (fk == null) {
            return null;
        }
        boolean inverse = join.isForeignKeyInversed();

        Column[] from = inverse ? fk.getPrimaryKeyColumns() : fk.getColumns();

        Column[] to = inverse ? fk.getColumns() : fk.getPrimaryKeyColumns();

        SQLBuffer buf = new SQLBuffer(this);
        int count = 0;
        for (int i = 0; i < from.length; count++) {
            if (count > 0) {
                buf.append(" AND ");
            }
            buf.append(join.getAlias1()).append(".").append(from[i]);
            buf.append(" = ");
            buf.append(join.getAlias2()).append(".").append(to[i]);
            buf.append("(+)");
            i++;
        }
        if (fk.getConstantColumns().length > 0) {
            throw new StoreException(_loc.get("tibero-constant", join.getTable1(), join.getTable2())).setFatal(true);
        }
        if (fk.getConstantPrimaryKeyColumns().length > 0) {
            throw new StoreException(_loc.get("tibero-constant", join.getTable1(), join.getTable2())).setFatal(true);
        }
        return buf;
    }

    protected SQLBuffer toSelect(SQLBuffer select, JDBCFetchConfiguration fetch,
                                 SQLBuffer tables, SQLBuffer where, SQLBuffer group,
                                 SQLBuffer having, SQLBuffer order, boolean distinct,
                                 boolean forUpdate, long start, long end, boolean subselect, Select sel) {
        return toSelect(select, fetch, tables, where, group, having, order, distinct, forUpdate, start, end, sel);
    }

    protected SQLBuffer toSelect(SQLBuffer select, JDBCFetchConfiguration fetch,
                                 SQLBuffer tables, SQLBuffer where, SQLBuffer group,
                                 SQLBuffer having, SQLBuffer order, boolean distinct,
                                 boolean forUpdate, long start, long end, Select sel) {
        if (!isUsingRange(start, end)) {
            return super.toSelect(select, fetch, tables, where, group, having, order, distinct, forUpdate, 0L, Long.MAX_VALUE, sel);
        }
        SQLBuffer buf = new SQLBuffer(this);
        if (!requiresSubselectForRange(start, end, distinct, order)) {
            if ((where != null) && (!where.isEmpty())) {
                buf.append(where).append(" AND ");
            }
            buf.append("ROWNUM <= ").appendValue(end);
            return super.toSelect(select, fetch, tables, buf, group, having, order, distinct, forUpdate, 0L, Long.MAX_VALUE, sel);
        }
        SQLBuffer newsel = super.toSelect(select, fetch, tables, where, group, having, order, distinct, forUpdate, 0L, Long.MAX_VALUE, sel);
        if (!isUsingOffset(start)) {
            buf.append(getSelectOperation(fetch) + " * FROM (");
            buf.append(newsel);
            buf.append(") WHERE ROWNUM <= ").appendValue(end);
            return buf;
        }
        buf.append(getSelectOperation(fetch)).append(" * FROM (SELECT r.*, ROWNUM RNUM FROM (");
        buf.append(newsel);
        buf.append(") r");
        if (isUsingLimit(end)) {
            buf.append(" WHERE ROWNUM <= ").appendValue(end);
        }
        buf.append(") WHERE RNUM > ").appendValue(start);
        return buf;
    }

    private boolean requiresSubselectForRange(long start, long end, boolean distinct, SQLBuffer order) {
        if (!isUsingRange(start, end)) {
            return false;
        }
        return (isUsingOffset(start)) || (distinct) || (isUsingOrderBy(order));
    }

    public String getSelectOperation(JDBCFetchConfiguration fetch) {
        Object hint = fetch == null ? null : fetch.getHint(SELECT_HINT);
        String select = "SELECT";
        if (hint != null) {
            select = select + " " + hint;
        }
        return select;
    }

    public void setString(PreparedStatement stmnt, int idx, String val,
                          Column col)
            throws SQLException {

        String typeName = (col == null) ? null : col.getTypeIdentifier().getName();
        if (typeName != null && (typeName.toLowerCase(Locale.ENGLISH).startsWith("nvarchar") ||
                typeName.toLowerCase(Locale.ENGLISH).startsWith("nchar") ||
                typeName.toLowerCase(Locale.ENGLISH).startsWith("nclob"))) {
            Statement inner = stmnt;
            if (inner instanceof DelegatingPreparedStatement)
                inner = ((DelegatingPreparedStatement) inner).
                        getInnermostDelegate();

            try {
                inner.getClass().getMethod("setNString",
                        new Class[]{int.class, String.class}).
                        invoke(inner,
                                new Object[]{
                                        new Integer(idx), val
                                });
            } catch (Exception e) {
                log.warn(e);
            }
        }


        if (col != null && col.getType() == Types.CHAR
                && val != null && val.length() != col.getSize()) {
            Statement inner = stmnt;
            if (inner instanceof DelegatingPreparedStatement)
                inner = ((DelegatingPreparedStatement) inner).
                        getInnermostDelegate();

            try {
                Method setFixedCharMethod = inner.getClass().getMethod("setFixedCHAR",
                        new Class[]{int.class, String.class});
                if (!setFixedCharMethod.isAccessible()) {
                    setFixedCharMethod.setAccessible(true);
                }

                setFixedCharMethod.invoke(inner, new Object[]{new Integer(idx), val});
                return;
            } catch (Exception e) {
                log.warn(e);
            }


            if (!_warnedCharColumn && log.isWarnEnabled()) {
                _warnedCharColumn = true;
                log.warn(_loc.get("unpadded-char-cols"));
            }
        }
        super.setString(stmnt, idx, val, col);
    }

    public Column[] getColumns(DatabaseMetaData meta, String catalog, String schemaName,
                               String tableName, String columnName, Connection conn) throws SQLException {
        return getColumns(meta,
                DBIdentifier.newCatalog(catalog),
                DBIdentifier.newSchema(schemaName),
                DBIdentifier.newTable(tableName),
                DBIdentifier.newColumn(columnName), conn);
    }

    public Column[] getColumns(DatabaseMetaData meta, DBIdentifier catalog,
                               DBIdentifier schemaName, DBIdentifier tableName, DBIdentifier columnName, Connection conn)
            throws SQLException {
        Column[] cols = super.getColumns(meta, catalog, schemaName, tableName,
                columnName, conn);

        for (int i = 0; cols != null && i < cols.length; i++) {
            String typeName = cols[i].getTypeIdentifier().getName();
            if (typeName == null)
                continue;
            if (typeName.toUpperCase(Locale.ENGLISH).startsWith("TIMESTAMP"))
                cols[i].setType(Types.TIMESTAMP);
            else if ("BLOB".equalsIgnoreCase(typeName))
                cols[i].setType(Types.BLOB);
            else if ("CLOB".equalsIgnoreCase(typeName)
                    || "NCLOB".equalsIgnoreCase(typeName))
                cols[i].setType(Types.CLOB);
            else if ("FLOAT".equalsIgnoreCase(typeName))
                cols[i].setType(Types.FLOAT);
            else if ("NVARCHAR".equalsIgnoreCase(typeName))
                cols[i].setType(Types.VARCHAR);
            else if ("NCHAR".equalsIgnoreCase(typeName))
                cols[i].setType(Types.CHAR);
            else if ("XMLTYPE".equalsIgnoreCase(typeName)) {
                cols[i].setXML(true);
            }
        }
        return cols;
    }

    public int getPreferredType(int type) {
        switch (type) {
            case Types.TIME_WITH_TIMEZONE:
                return Types.TIME;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Types.TIMESTAMP;
            default:
                return type;
        }
    }

    public PrimaryKey[] getPrimaryKeys(DatabaseMetaData meta,
                                       DBIdentifier catalog, DBIdentifier schemaName, DBIdentifier tableName, Connection conn)
            throws SQLException {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT t0.OWNER AS TABLE_SCHEM, ").
                append("t0.TABLE_NAME AS TABLE_NAME, ").
                append("t0.COLUMN_NAME AS COLUMN_NAME, ").
                append("t0.CONSTRAINT_NAME AS PK_NAME ").
                append("FROM ALL_CONS_COLUMNS t0, ALL_CONSTRAINTS t1 ").
                append("WHERE t0.OWNER = t1.OWNER ").
                append("AND t0.CONSTRAINT_NAME = t1.CONSTRAINT_NAME ").
                append("AND t1.CONSTRAINT_TYPE = 'P'");
        if (!DBIdentifier.isNull(schemaName))
            buf.append(" AND t0.OWNER = ?");
        if (!DBIdentifier.isNull(tableName))
            buf.append(" AND t0.TABLE_NAME = ?");

        PreparedStatement stmnt = conn.prepareStatement(buf.toString());
        ResultSet rs = null;
        try {
            int idx = 1;
            if (!DBIdentifier.isNull(schemaName)) {
                setString(stmnt, idx++, convertSchemaCase(schemaName), null);
            }
            if (!DBIdentifier.isNull(tableName)) {
                setString(stmnt, idx++, convertSchemaCase(tableName.getUnqualifiedName()), null);
            }
            setTimeouts(stmnt, conf, false);
            rs = stmnt.executeQuery();
            List<PrimaryKey> pkList = new ArrayList<>();
            while (rs != null && rs.next()) {
                pkList.add(newPrimaryKey(rs));
            }
            return pkList.toArray(new PrimaryKey[pkList.size()]);
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (Exception e) {
                }
            try {
                stmnt.close();
            } catch (Exception e) {
            }
        }
    }

    public Index[] getIndexInfo(DatabaseMetaData meta, String catalog,
                                String schemaName, String tableName, boolean unique, boolean approx,
                                Connection conn)
            throws SQLException {
        return getIndexInfo(meta,
                DBIdentifier.newCatalog(catalog),
                DBIdentifier.newSchema(schemaName),
                DBIdentifier.newTable(tableName), unique, approx, conn);
    }

    public Index[] getIndexInfo(DatabaseMetaData meta, DBIdentifier catalog,
                                DBIdentifier schemaName, DBIdentifier tableName, boolean unique, boolean approx,
                                Connection conn)
            throws SQLException {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT t0.INDEX_OWNER AS TABLE_SCHEM, ").
                append("t0.TABLE_NAME AS TABLE_NAME, ").
                append("DECODE(t1.UNIQUENESS, 'UNIQUE', 0, 'NONUNIQUE', 1) ").
                append("AS NON_UNIQUE, ").
                append("t0.INDEX_NAME AS INDEX_NAME, ").
                append("t0.COLUMN_NAME AS COLUMN_NAME ").
                append("FROM ALL_IND_COLUMNS t0, ALL_INDEXES t1 ").
                append("WHERE t0.INDEX_OWNER = t1.OWNER ").
                append("AND t0.INDEX_NAME = t1.INDEX_NAME");
        if (!DBIdentifier.isNull(schemaName))
            buf.append(" AND t0.TABLE_OWNER = ?");
        if (!DBIdentifier.isNull(tableName))
            buf.append(" AND t0.TABLE_NAME = ?");

        PreparedStatement stmnt = conn.prepareStatement(buf.toString());
        ResultSet rs = null;
        try {
            int idx = 1;
            if (!DBIdentifier.isNull(schemaName))
                setString(stmnt, idx++, convertSchemaCase(schemaName), null);
            if (!DBIdentifier.isNull(tableName))
                setString(stmnt, idx++, convertSchemaCase(tableName), null);

            setTimeouts(stmnt, conf, false);
            rs = stmnt.executeQuery();
            List idxList = new ArrayList();
            while (rs != null && rs.next())
                idxList.add(newIndex(rs));
            return (Index[]) idxList.toArray(new Index[idxList.size()]);
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (Exception e) {
                }
            try {
                stmnt.close();
            } catch (Exception e) {
            }
        }
    }

    public ForeignKey[] getImportedKeys(DatabaseMetaData meta, String catalog,
                                        String schemaName, String tableName, Connection conn, boolean partialKeys)
            throws SQLException {
        return getImportedKeys(meta,
                DBIdentifier.newCatalog(catalog),
                DBIdentifier.newSchema(schemaName),
                DBIdentifier.newTable(tableName), conn, partialKeys);
    }

    public ForeignKey[] getImportedKeys(DatabaseMetaData meta, DBIdentifier catalog,
                                        DBIdentifier schemaName, DBIdentifier tableName, Connection conn, boolean partialKeys)
            throws SQLException {
        StringBuilder delAction = new StringBuilder("DECODE(t1.DELETE_RULE").
                append(", 'NO ACTION', ").append(DatabaseMetaData.importedKeyNoAction).
                append(", 'RESTRICT', ").append(DatabaseMetaData.importedKeyRestrict).
                append(", 'CASCADE', ").append(DatabaseMetaData.importedKeyCascade).
                append(", 'SET NULL', ").append(DatabaseMetaData.importedKeySetNull).
                append(", 'SET DEFAULT', ").append(DatabaseMetaData.importedKeySetDefault).
                append(")");

        StringBuilder buf = new StringBuilder();
        buf.append("SELECT t2.OWNER AS PKTABLE_SCHEM, ").
                append("t2.TABLE_NAME AS PKTABLE_NAME, ").
                append("t2.COLUMN_NAME AS PKCOLUMN_NAME, ").
                append("t0.OWNER AS FKTABLE_SCHEM, ").
                append("t0.TABLE_NAME AS FKTABLE_NAME, ").
                append("t0.COLUMN_NAME AS FKCOLUMN_NAME, ").
                append("t0.POSITION AS KEY_SEQ, ").
                append(delAction).append(" AS DELETE_RULE, ").
                append("t0.CONSTRAINT_NAME AS FK_NAME, ").
                append("DECODE(t1.DEFERRED, 'DEFERRED', ").
                append(DatabaseMetaData.importedKeyInitiallyDeferred).
                append(", 'IMMEDIATE', ").
                append(DatabaseMetaData.importedKeyInitiallyImmediate).
                append(") AS DEFERRABILITY ").
                append("FROM ALL_CONS_COLUMNS t0, ALL_CONSTRAINTS t1, ").
                append("ALL_CONS_COLUMNS t2 ").
                append("WHERE t0.OWNER = t1.OWNER ").
                append("AND t0.CONSTRAINT_NAME = t1.CONSTRAINT_NAME ").
                append("AND t1.CONSTRAINT_TYPE = 'R' ").
                append("AND t1.R_OWNER = t2.OWNER ").
                append("AND t1.R_CONSTRAINT_NAME = t2.CONSTRAINT_NAME ").
                append("AND t0.POSITION = t2.POSITION");
        if (!DBIdentifier.isNull(schemaName))
            buf.append(" AND t0.OWNER = ?");
        if (!DBIdentifier.isNull(tableName))
            buf.append(" AND t0.TABLE_NAME = ?");
        buf.append(" ORDER BY t2.OWNER, t2.TABLE_NAME, t0.POSITION");

        PreparedStatement stmnt = conn.prepareStatement(buf.toString());
        ResultSet rs = null;
        try {
            int idx = 1;
            if (!DBIdentifier.isNull(schemaName))
                setString(stmnt, idx++, convertSchemaCase(schemaName), null);
            if (!DBIdentifier.isNull(tableName))
                setString(stmnt, idx++, convertSchemaCase(tableName), null);
            setTimeouts(stmnt, conf, false);
            rs = stmnt.executeQuery();
            List<ForeignKey> fkList = new ArrayList<>();
            Map<FKMapKey, ForeignKey> fkMap = new HashMap<>();

            while (rs != null && rs.next()) {
                ForeignKey nfk = newForeignKey(rs);
                if (!partialKeys) {
                    ForeignKey fk = combineForeignKey(fkMap, nfk);
                    if (fk != nfk) {
                        continue;
                    }
                }
                fkList.add(nfk);
            }
            return (ForeignKey[]) fkList.toArray
                    (new ForeignKey[fkList.size()]);
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (Exception e) {
                }
            try {
                stmnt.close();
            } catch (Exception e) {
            }
        }
    }

    public String[] getCreateTableSQL(Table table) {
        String[] create = super.getCreateTableSQL(table);
        if (!useTriggersForAutoAssign)
            return create;

        Column[] cols = table.getColumns();
        List seqs = null;
        String seq, trig;
        for (int i = 0; cols != null && i < cols.length; i++) {
            if (!cols[i].isAutoAssigned())
                continue;
            if (seqs == null)
                seqs = new ArrayList(4);

            seq = autoAssignSequenceName;
            if (seq == null) {
                if (openjpa3GeneratedKeyNames)
                    seq = getOpenJPA3GeneratedKeySequenceName(cols[i]);
                else
                    seq = getGeneratedKeySequenceName(cols[i]);
                seqs.add("CREATE SEQUENCE " + seq + " START WITH 1");
            }
            if (openjpa3GeneratedKeyNames)
                trig = getOpenJPA3GeneratedKeyTriggerName(cols[i]);
            else
                trig = getGeneratedKeyTriggerName(cols[i]);

            seqs.add("CREATE OR REPLACE TRIGGER " + trig
                    + " BEFORE INSERT ON " + toDBName(table.getIdentifier())
                    + " FOR EACH ROW BEGIN SELECT " + seq + ".nextval INTO "
                    + ":new." + toDBName(cols[i].getIdentifier()) + " FROM DUAL; "
                    + "END " + trig + ";");
        }
        if (seqs == null)
            return create;

        String[] sql = new String[create.length + seqs.size()];
        System.arraycopy(create, 0, sql, 0, create.length);
        for (int i = 0; i < seqs.size(); i++)
            sql[create.length + i] = (String) seqs.get(i);
        return sql;
    }

    public int getJDBCType(int metaTypeCode, boolean lob, int precis,
                           int scale, boolean xml) {
        return getJDBCType(metaTypeCode, lob || xml, precis, scale);
    }

    protected String getSequencesSQL(String schemaName, String sequenceName) {
        return getSequencesSQL(DBIdentifier.newSchema(schemaName), DBIdentifier.newSequence(sequenceName));
    }

    protected String getSequencesSQL(DBIdentifier schemaName, DBIdentifier sequenceName) {
        StringBuilder buf = new StringBuilder();
        buf.append("SELECT SEQUENCE_OWNER AS SEQUENCE_SCHEMA, ").
                append("SEQUENCE_NAME FROM ALL_SEQUENCES");
        if (!DBIdentifier.isNull(schemaName) || !DBIdentifier.isNull(sequenceName))
            buf.append(" WHERE ");
        if (!DBIdentifier.isNull(schemaName)) {
            buf.append("SEQUENCE_OWNER = ?");
            if (!DBIdentifier.isNull(sequenceName))
                buf.append(" AND ");
        }
        if (!DBIdentifier.isNull(sequenceName))
            buf.append("SEQUENCE_NAME = ?");
        return buf.toString();
    }

    public boolean isSystemSequence(String name, String schema,
                                    boolean targetSchema) {
        return isSystemSequence(DBIdentifier.newSequence(name),
                DBIdentifier.newSchema(schema), targetSchema);
    }

    public boolean isSystemSequence(DBIdentifier name, DBIdentifier schema,
                                    boolean targetSchema) {
        if (super.isSystemSequence(name, schema, targetSchema))
            return true;

        String strName = DBIdentifier.isNull(name) ? "" : name.getName();
        return (autoAssignSequenceName != null
                && strName.equalsIgnoreCase(autoAssignSequenceName))
                || (autoAssignSequenceName == null
                && strName.toUpperCase(Locale.ENGLISH).startsWith("ST_"));
    }

    public Object getGeneratedKey(Column col, Connection conn)
            throws SQLException {
        if (!useTriggersForAutoAssign)
            return 0L;

        String seq = autoAssignSequenceName;
        if (seq == null && openjpa3GeneratedKeyNames)
            seq = getOpenJPA3GeneratedKeySequenceName(col);
        else if (seq == null)
            seq = getGeneratedKeySequenceName(col);
        PreparedStatement stmnt = conn.prepareStatement("SELECT " + seq
                + ".currval FROM DUAL");
        ResultSet rs = null;
        try {
            setTimeouts(stmnt, conf, false);
            rs = stmnt.executeQuery();
            rs.next();
            return rs.getLong(1);
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException se) {
                }
            try {
                stmnt.close();
            } catch (SQLException se) {
            }
        }
    }

    protected String getGeneratedKeyTriggerName(Column col) {
        String seqName = getGeneratedKeySequenceName(col);
        return seqName.substring(0, seqName.length() - 3) + "TRG";
    }

    protected String getOpenJPA3GeneratedKeySequenceName(Column col) {
        Table table = col.getTable();
        DBIdentifier sName = DBIdentifier.preCombine(table.getIdentifier(), "SEQ");
        return toDBName(getNamingUtil().makeIdentifierValid(sName, table.getSchema().
                getSchemaGroup(), maxTableNameLength, true));
    }

    protected String getOpenJPA3GeneratedKeyTriggerName(Column col) {
        Table table = col.getTable();
        DBIdentifier sName = DBIdentifier.preCombine(table.getIdentifier(), "TRIG");
        return toDBName(getNamingUtil().makeIdentifierValid(sName, table.getSchema().
                getSchemaGroup(), maxTableNameLength, true));
    }

    public void appendXmlComparison(SQLBuffer buf, String op, FilterValue lhs,
                                    FilterValue rhs, boolean lhsxml, boolean rhsxml) {
        super.appendXmlComparison(buf, op, lhs, rhs, lhsxml, rhsxml);
        if (lhsxml && rhsxml)
            appendXmlComparison2(buf, op, lhs, rhs);
        else if (lhsxml)
            appendXmlComparison1(buf, op, lhs, rhs);
        else
            appendXmlComparison1(buf, op, rhs, lhs);
    }

    private void appendXmlComparison1(SQLBuffer buf, String op,
                                      FilterValue lhs, FilterValue rhs) {
        appendXmlExtractValue(buf, lhs);
        buf.append(" ").append(op).append(" ");
        rhs.appendTo(buf);
    }

    private void appendXmlComparison2(SQLBuffer buf, String op,
                                      FilterValue lhs, FilterValue rhs) {
        appendXmlExtractValue(buf, lhs);
        buf.append(" ").append(op).append(" ");
        appendXmlExtractValue(buf, rhs);
    }

    private void appendXmlExtractValue(SQLBuffer buf, FilterValue val) {
        buf.append("extractValue(").
                append(val.getColumnAlias(
                        val.getFieldMapping().getColumns()[0])).
                append(",'/*/");
        val.appendTo(buf);
        buf.append("')");
    }

    public int getBatchUpdateCount(PreparedStatement ps) throws SQLException {
        int updateSuccessCnt = 0;
        if (batchLimit != 0 && ps != null) {
            updateSuccessCnt = ps.getUpdateCount();
            if (log.isTraceEnabled())
                log.trace(_loc.get("batch-update-success-count",
                        updateSuccessCnt));
        }
        return updateSuccessCnt;
    }

    public boolean isImplicitJoin() {
        return joinSyntax == SYNTAX_DATABASE;
    }

    public String getMarkerForInsertUpdate(Column col, Object val) {
        if (col.isXML() && val != RowImpl.NULL) {
            return xmlTypeMarker;
        }
        return super.getMarkerForInsertUpdate(col, val);
    }


    public String getIsNullSQL(String colAlias, int colType) {
        switch (colType) {
            case Types.BLOB:
            case Types.CLOB:
                return String.format("length (%s) = 0", colAlias);
        }
        return super.getIsNullSQL(colAlias, colType);
    }

    public String getIsNotNullSQL(String colAlias, int colType) {
        switch (colType) {
            case Types.BLOB:
            case Types.CLOB:
                return String.format("length (%s) != 0 ", colAlias);
        }
        return super.getIsNotNullSQL(colAlias, colType);
    }

    public void indexOf(SQLBuffer buf, FilterValue str, FilterValue find,
                        FilterValue start) {
        buf.append("INSTR(");
        str.appendTo(buf);
        buf.append(", ");
        find.appendTo(buf);
        if (start != null) {
            buf.append(", ");
            start.appendTo(buf);
        }
        buf.append(")");
    }
}
