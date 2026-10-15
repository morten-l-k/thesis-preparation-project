package thesis.metabench;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

/**
 * JVM-wide record of the metadata reads that go through {@link TimingFileIO}.
 *
 * <p>Manifest lists and manifests share one wall clock, which runs while at least one read of
 * either kind is blocked on S3. Iceberg reads manifests on several worker threads at once, so
 * summing per-read times would count overlapping waits more than once; the shared clock counts
 * them once. Its total is t_fetch. Table metadata (metadata.json) gets its own clock: with a
 * REST catalog the server reads it, so it should stay at zero in the jobs. Data files get their
 * own clock and counters too, so a job can compare manifest bytes with data bytes.
 *
 * <p>The Python job reads this through py4j: {@link #reset()} before the query and
 * {@link #snapshotJson()} after it.
 */
public final class FetchStats {

  public enum Kind {
    MANIFEST_LIST,
    MANIFEST,
    TABLE_METADATA,
    DATA
  }

  private static final Object LOCK = new Object();
  private static final WallClock MANIFESTS = new WallClock();
  private static final WallClock TABLE_METADATA = new WallClock();
  private static final WallClock DATA = new WallClock();
  private static final Map<Kind, Counters> COUNTERS = new EnumMap<>(Kind.class);

  static {
    for (Kind kind : Kind.values()) {
      COUNTERS.put(kind, new Counters());
    }
  }

  private FetchStats() {}

  /** Marks the start of a blocking call on a file of this kind; returns the start time. */
  static long begin(Kind kind) {
    long now = System.nanoTime();
    synchronized (LOCK) {
      clockFor(kind).enter(now);
    }
    return now;
  }

  /** Marks the end of a call started with {@link #begin}. */
  static void end(Kind kind, long start, long bytes) {
    long now = System.nanoTime();
    synchronized (LOCK) {
      clockFor(kind).exit(now);
      Counters counters = COUNTERS.get(kind);
      counters.calls += 1;
      counters.threadNanos += now - start;
      counters.bytes += bytes;
    }
  }

  /** Counts one opened stream, i.e. one file read. */
  static void fileOpened(Kind kind) {
    synchronized (LOCK) {
      COUNTERS.get(kind).files += 1;
    }
  }

  public static void reset() {
    synchronized (LOCK) {
      MANIFESTS.reset();
      TABLE_METADATA.reset();
      DATA.reset();
      COUNTERS.values().forEach(Counters::reset);
    }
  }

  /**
   * Everything recorded since the last {@link #reset()}, as JSON. {@code t_fetch_ms} is the
   * manifest list + manifest wall clock; {@code thread_ms} per kind is the plain sum of call
   * times, so it can exceed the wall clock when reads overlap.
   */
  public static String snapshotJson() {
    synchronized (LOCK) {
      StringBuilder json = new StringBuilder();
      json.append("{\"t_fetch_ms\":").append(ms(MANIFESTS.busyNanos));
      json.append(",\"table_metadata_ms\":").append(ms(TABLE_METADATA.busyNanos));
      json.append(",\"data_read_ms\":").append(ms(DATA.busyNanos));
      json.append(",\"kinds\":{");
      boolean first = true;
      for (Map.Entry<Kind, Counters> entry : COUNTERS.entrySet()) {
        Counters c = entry.getValue();
        json.append(first ? "" : ",");
        json.append('"').append(entry.getKey().name().toLowerCase(Locale.ROOT)).append("\":{");
        json.append("\"files\":").append(c.files);
        json.append(",\"calls\":").append(c.calls);
        json.append(",\"bytes\":").append(c.bytes);
        json.append(",\"thread_ms\":").append(ms(c.threadNanos));
        json.append('}');
        first = false;
      }
      return json.append("}}").toString();
    }
  }

  private static WallClock clockFor(Kind kind) {
    switch (kind) {
      case TABLE_METADATA:
        return TABLE_METADATA;
      case DATA:
        return DATA;
      default:
        return MANIFESTS;
    }
  }

  private static String ms(long nanos) {
    return String.format(Locale.ROOT, "%.3f", nanos / 1e6);
  }

  /** Time during which at least one call was in flight. Guarded by LOCK. */
  private static final class WallClock {
    private int inFlight;
    private long busySince;
    private long busyNanos;

    void enter(long now) {
      if (inFlight++ == 0) {
        busySince = now;
      }
    }

    void exit(long now) {
      if (--inFlight == 0) {
        busyNanos += now - busySince;
      }
    }

    void reset() {
      inFlight = 0;
      busyNanos = 0;
    }
  }

  /** Guarded by LOCK. */
  private static final class Counters {
    long files;
    long calls;
    long bytes;
    long threadNanos;

    void reset() {
      files = 0;
      calls = 0;
      bytes = 0;
      threadNanos = 0;
    }
  }
}
