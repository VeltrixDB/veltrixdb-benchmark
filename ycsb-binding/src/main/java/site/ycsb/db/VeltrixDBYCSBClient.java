package site.ycsb.db;

import com.veltrixdb.client.VeltrixDBClient;
import com.veltrixdb.client.VeltrixDBException;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Logger;

/**
 * YCSB binding for VeltrixDB.
 *
 * <p>Maps YCSB's table/key/fields model onto VeltrixDB as follows:
 * <ul>
 *   <li>{@code table} → VeltrixDB namespace (NSPUT / NSGET / NSDEL / NSSCAN)</li>
 *   <li>{@code key}   → namespace key</li>
 *   <li>{@code fields} → serialised inline in the value bytes (compact binary)</li>
 * </ul>
 *
 * <h3>Binary field format stored as value</h3>
 * <pre>
 * [2B fieldCount LE]
 * per field:
 *   [2B nameLen  LE][4B valueLen LE][name UTF-8][value bytes]
 * </pre>
 *
 * <h3>YCSB properties</h3>
 * <pre>
 * veltrixdb.host            = 127.0.0.1   # server hostname / IP
 * veltrixdb.port            = 9000        # TCP port
 * veltrixdb.connect.timeout = 5000        # connect timeout ms
 * veltrixdb.socket.timeout  = 10000       # per-op read timeout ms
 * veltrixdb.username        =             # optional AUTH username
 * veltrixdb.password        =             # optional AUTH password
 * veltrixdb.update.mode     = merge       # merge | overwrite
 *                                         #   merge    — read-modify-write (correct YCSB semantics)
 *                                         #   overwrite — blindly write supplied fields only
 *                                         #               (faster; use fieldcount=1 in workload
 *                                         #                to avoid semantic difference)
 * </pre>
 *
 * <h3>Quick start</h3>
 * <pre>
 * # Load phase
 * bin/ycsb load veltrixdb -s -P workloads/workload_a \
 *   -p veltrixdb.host=127.0.0.1 -p veltrixdb.port=9000 \
 *   -threads 32
 *
 * # Run phase
 * bin/ycsb run veltrixdb -s -P workloads/workload_a \
 *   -p veltrixdb.host=127.0.0.1 -p veltrixdb.port=9000 \
 *   -threads 64
 * </pre>
 */
public class VeltrixDBYCSBClient extends DB {

    private static final Logger LOG = Logger.getLogger(VeltrixDBYCSBClient.class.getName());

    // ── Property keys ─────────────────────────────────────────────────────────

    public static final String HOST_PROP       = "veltrixdb.host";
    public static final String PORT_PROP       = "veltrixdb.port";
    public static final String CONNECT_TO_PROP = "veltrixdb.connect.timeout";
    public static final String SOCKET_TO_PROP  = "veltrixdb.socket.timeout";
    public static final String USERNAME_PROP   = "veltrixdb.username";
    public static final String PASSWORD_PROP   = "veltrixdb.password";
    public static final String UPDATE_MODE_PROP = "veltrixdb.update.mode";

    // ── Per-thread state ──────────────────────────────────────────────────────
    // YCSB creates one DB instance per worker thread and calls init() once.
    // A single VeltrixDBClient connection per instance is therefore safe.

    private VeltrixDBClient client;
    private boolean mergeOnUpdate;

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void init() throws DBException {
        Properties p = getProperties();

        String host    = p.getProperty(HOST_PROP,        "127.0.0.1");
        int    port    = intProp(p, PORT_PROP,            9000);
        int    connTo  = intProp(p, CONNECT_TO_PROP,      5_000);
        int    sockTo  = intProp(p, SOCKET_TO_PROP,       10_000);
        String user    = p.getProperty(USERNAME_PROP,     "").trim();
        String pass    = p.getProperty(PASSWORD_PROP,     "").trim();
        mergeOnUpdate  = "merge".equalsIgnoreCase(
                             p.getProperty(UPDATE_MODE_PROP, "merge").trim());

        try {
            client = new VeltrixDBClient(host, port, connTo, sockTo);
            if (!user.isEmpty()) {
                client.auth(user, pass);
            }
        } catch (IOException | VeltrixDBException e) {
            throw new DBException(
                    "VeltrixDB: cannot connect to " + host + ":" + port + " — " + e.getMessage(), e);
        }

        LOG.info(String.format(
                "VeltrixDB YCSB binding ready: %s:%d  update=%s",
                host, port, mergeOnUpdate ? "merge" : "overwrite"));
    }

    @Override
    public void cleanup() throws DBException {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    // ── YCSB DB operations ────────────────────────────────────────────────────

    /**
     * Read one record.
     * {@code table} → namespace, {@code key} → namespace key.
     * If {@code fields} is null/empty every field is returned.
     */
    @Override
    public Status read(String table, String key,
                       Set<String> fields,
                       Map<String, ByteIterator> result) {
        try {
            byte[] data = client.getNS(table, key);
            if (data == null) {
                return Status.NOT_FOUND;
            }
            deserialize(data, fields, result);
            return Status.OK;
        } catch (IOException | VeltrixDBException e) {
            LOG.warning("read(" + table + "/" + key + "): " + e.getMessage());
            return Status.ERROR;
        }
    }

    /**
     * Scan {@code recordcount} records starting at {@code startkey}.
     * Uses VeltrixDB NSSCAN (prefix-based namespace scan).
     * Results are returned in the order VeltrixDB delivers them.
     */
    @Override
    public Status scan(String table, String startkey, int recordcount,
                       Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
        try {
            VeltrixDBClient.NSEntry[] entries =
                    client.scanNamespace(table, startkey, recordcount);
            for (VeltrixDBClient.NSEntry e : entries) {
                HashMap<String, ByteIterator> row = new HashMap<>();
                if (e.value != null && e.value.length > 0) {
                    deserialize(e.value, fields, row);
                }
                result.add(row);
            }
            return Status.OK;
        } catch (IOException | VeltrixDBException e) {
            LOG.warning("scan(" + table + "/" + startkey + "): " + e.getMessage());
            return Status.ERROR;
        }
    }

    /**
     * Update one record.
     *
     * <p>In {@code merge} mode (default) the existing record is read first so that
     * fields not mentioned in {@code values} are preserved — this matches strict
     * YCSB semantics for workloads that do partial field updates (A, F).
     *
     * <p>In {@code overwrite} mode the record is blindly replaced with only the
     * supplied fields (faster; use {@code fieldcount=1} in workload to avoid any
     * semantic difference).
     */
    @Override
    public Status update(String table, String key,
                         Map<String, ByteIterator> values) {
        if (!mergeOnUpdate) {
            return insert(table, key, values);
        }

        // Merge mode: read → patch supplied fields → write back
        try {
            Map<String, byte[]> merged = new LinkedHashMap<>();

            byte[] existing = client.getNS(table, key);
            if (existing != null) {
                deserializeToBytes(existing, null, merged);
            }
            for (Map.Entry<String, ByteIterator> e : values.entrySet()) {
                merged.put(e.getKey(), e.getValue().toArray());
            }

            client.putNS(table, key, serializeBytes(merged), -1);
            return Status.OK;
        } catch (IOException | VeltrixDBException e) {
            LOG.warning("update(" + table + "/" + key + "): " + e.getMessage());
            return Status.ERROR;
        }
    }

    /**
     * Insert a new record (or overwrite an existing one).
     * All supplied fields are serialised and stored as a single value.
     */
    @Override
    public Status insert(String table, String key,
                         Map<String, ByteIterator> values) {
        try {
            client.putNS(table, key, serialize(values), -1);
            return Status.OK;
        } catch (IOException | VeltrixDBException e) {
            LOG.warning("insert(" + table + "/" + key + "): " + e.getMessage());
            return Status.ERROR;
        }
    }

    /**
     * Delete a record.
     */
    @Override
    public Status delete(String table, String key) {
        try {
            client.deleteNS(table, key);
            return Status.OK;
        } catch (IOException | VeltrixDBException e) {
            LOG.warning("delete(" + table + "/" + key + "): " + e.getMessage());
            return Status.ERROR;
        }
    }

    // ── Serialisation helpers ─────────────────────────────────────────────────
    //
    // Compact binary format:
    //
    //   [2B fieldCount LE]
    //   per field:
    //     [2B nameLen LE][4B valueLen LE][name bytes][value bytes]
    //
    // Chosen over JSON to avoid parsing overhead inside the hot path of a
    // benchmark loop (no escaping, no UTF-8 decoding of values).

    private static byte[] serialize(Map<String, ByteIterator> fields) {
        // Drain ByteIterators first — they are single-pass cursors.
        Map<String, byte[]> raw = new LinkedHashMap<>(fields.size() * 2);
        for (Map.Entry<String, ByteIterator> e : fields.entrySet()) {
            raw.put(e.getKey(), e.getValue().toArray());
        }
        return serializeBytes(raw);
    }

    private static byte[] serializeBytes(Map<String, byte[]> fields) {
        List<byte[]> nameList = new ArrayList<>(fields.size());
        List<byte[]> valList  = new ArrayList<>(fields.size());

        int totalBytes = 2; // fieldCount (2B)
        for (Map.Entry<String, byte[]> e : fields.entrySet()) {
            byte[] n = e.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] v = e.getValue();
            nameList.add(n);
            valList.add(v);
            totalBytes += 2 + 4 + n.length + v.length;
        }

        ByteBuffer buf = ByteBuffer.allocate(totalBytes).order(ByteOrder.LITTLE_ENDIAN);
        buf.putShort((short) fields.size());
        for (int i = 0; i < nameList.size(); i++) {
            buf.putShort((short) nameList.get(i).length);
            buf.putInt(valList.get(i).length);
            buf.put(nameList.get(i));
            buf.put(valList.get(i));
        }
        return buf.array();
    }

    private static void deserialize(byte[] data, Set<String> filter,
                                    Map<String, ByteIterator> result) {
        Map<String, byte[]> raw = new LinkedHashMap<>();
        deserializeToBytes(data, filter, raw);
        for (Map.Entry<String, byte[]> e : raw.entrySet()) {
            result.put(e.getKey(), new ByteArrayByteIterator(e.getValue()));
        }
    }

    private static void deserializeToBytes(byte[] data, Set<String> filter,
                                           Map<String, byte[]> result) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        if (buf.remaining() < 2) {
            return;
        }
        int count = buf.getShort() & 0xFFFF;
        for (int i = 0; i < count; i++) {
            if (buf.remaining() < 6) {
                break;
            }
            int nameLen = buf.getShort() & 0xFFFF;
            int valLen  = buf.getInt();
            if (buf.remaining() < nameLen + valLen) {
                break;
            }
            byte[] nameBuf = new byte[nameLen];
            buf.get(nameBuf);
            byte[] valBuf = new byte[Math.max(valLen, 0)];
            if (valLen > 0) {
                buf.get(valBuf);
            }
            String name = new String(nameBuf, StandardCharsets.UTF_8);
            if (filter == null || filter.isEmpty() || filter.contains(name)) {
                result.put(name, valBuf);
            }
        }
    }

    // ── Utility ───────────────────────────────────────────────────────────────

    private static int intProp(Properties p, String key, int defaultValue) {
        String v = p.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            LOG.warning("VeltrixDB: invalid int for property '" + key + "': " + v
                    + " — using default " + defaultValue);
            return defaultValue;
        }
    }
}
