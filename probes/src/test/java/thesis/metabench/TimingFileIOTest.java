package thesis.metabench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TimingFileIOTest {

  private static final int READ_MILLIS = 50;
  private static final int CHUNKS = 4;

  private TimingFileIO io;

  @BeforeEach
  void setUp() {
    FetchStats.reset();
    io = new TimingFileIO();
    io.initialize(Map.of(TimingFileIO.DELEGATE_IMPL, SlowFileIO.class.getName()));
  }

  @Test
  void classifiesIcebergMetadataFiles() {
    assertEquals(
        FetchStats.Kind.MANIFEST_LIST,
        TimingFileIO.classify("s3://b/t/metadata/snap-123-1-abc.avro"));
    assertEquals(FetchStats.Kind.MANIFEST, TimingFileIO.classify("s3://b/t/metadata/abc-m0.avro"));
    assertEquals(
        FetchStats.Kind.MANIFEST, TimingFileIO.classify("s3://b/t/metadata/optimized-m-abc.avro"));
    assertEquals(
        FetchStats.Kind.TABLE_METADATA,
        TimingFileIO.classify("s3://b/t/metadata/00001-abc.metadata.json"));
    assertEquals(
        FetchStats.Kind.DATA, TimingFileIO.classify("s3://b/t/data/d=2024-01-01/00000-0-abc.parquet"));
    assertNull(TimingFileIO.classify("s3://b/t/metadata/stats-123.puffin"));
  }

  @Test
  void otherFilesAreNotWrapped() {
    assertInstanceOf(SlowInputFile.class, io.newInputFile("s3://b/t/metadata/stats-123.puffin"));
  }

  @Test
  void dataReadsAreCountedApartFromTFetch() {
    readFully(io.newInputFile("s3://b/t/data/00000-0-abc.parquet"));

    String json = FetchStats.snapshotJson();
    assertTrue(json.contains("\"t_fetch_ms\":0.000"), json);
    assertTrue(number(json, "\"data_read_ms\":([0-9.]+)") >= CHUNKS * READ_MILLIS, json);
    assertTrue(json.contains("\"data\":{\"files\":1,"), json);
    assertTrue(json.contains("\"bytes\":" + CHUNKS * SlowFileIO.CHUNK), json);
  }

  @Test
  void overlappingReadsAreCountedOnceInTFetch() throws InterruptedException {
    Runnable readManifest = () -> readFully(io.newInputFile("s3://b/t/metadata/abc-m0.avro"));
    Thread first = new Thread(readManifest);
    Thread second = new Thread(readManifest);
    first.start();
    second.start();
    first.join();
    second.join();

    String json = FetchStats.snapshotJson();
    double fetchMs = number(json, "\"t_fetch_ms\":([0-9.]+)");
    double threadMs = number(json, "\"manifest\":\\{[^}]*\"thread_ms\":([0-9.]+)");
    double oneReaderMs = CHUNKS * READ_MILLIS;

    assertTrue(json.contains("\"manifest\":{\"files\":2,"), json);
    assertTrue(json.contains("\"bytes\":" + 2 * CHUNKS * SlowFileIO.CHUNK), json);
    // Both readers waited about oneReaderMs, side by side: summed thread time is twice that,
    // the wall clock only once.
    assertTrue(threadMs >= 2 * oneReaderMs, json);
    assertTrue(fetchMs >= oneReaderMs && fetchMs < 1.5 * oneReaderMs, json);
  }

  private static void readFully(InputFile file) {
    try (SeekableInputStream in = file.newStream()) {
      byte[] buffer = new byte[64];
      while (in.read(buffer, 0, buffer.length) >= 0) {
        // drain
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static double number(String json, String regex) {
    Matcher m = Pattern.compile(regex).matcher(json);
    assertTrue(m.find(), json);
    return Double.parseDouble(m.group(1));
  }

  /** Delegate whose files hand out CHUNKS chunks, each after a READ_MILLIS pause. */
  public static class SlowFileIO implements FileIO {
    static final int CHUNK = 8;

    public SlowFileIO() {}

    @Override
    public InputFile newInputFile(String path) {
      return new SlowInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }

  static class SlowInputFile implements InputFile {
    private final String path;

    SlowInputFile(String path) {
      this.path = path;
    }

    @Override
    public long getLength() {
      return (long) CHUNKS * SlowFileIO.CHUNK;
    }

    @Override
    public SeekableInputStream newStream() {
      return new SeekableInputStream() {
        private long pos;

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void seek(long newPos) {
          pos = newPos;
        }

        @Override
        public int read() {
          throw new UnsupportedOperationException();
        }

        @Override
        public int read(byte[] b, int off, int len) {
          if (pos >= getLength()) {
            return -1;
          }
          try {
            Thread.sleep(READ_MILLIS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          pos += SlowFileIO.CHUNK;
          return SlowFileIO.CHUNK;
        }
      };
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public boolean exists() {
      return true;
    }
  }
}
