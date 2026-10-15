package thesis.metabench;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * FileIO that hands all I/O to a delegate (S3FileIO by default) and records, in
 * {@link FetchStats}, how long reads of metadata and data files spend blocked on it and how many
 * bytes they return.
 *
 * <p>Only the blocking calls themselves are timed: opening a file, asking its length, and each
 * read/seek/skip/close on its stream. Iceberg decodes and filters manifest entries between those
 * calls, so that work is not counted. Anything that is neither table metadata nor a Parquet data
 * file (e.g. Puffin statistics) is returned untouched.
 *
 * <p>Configured on the Spark catalog with {@code io-impl}; the delegate is set with
 * {@code metabench.delegate-io-impl}.
 */
public class TimingFileIO implements FileIO {

  public static final String DELEGATE_IMPL = "metabench.delegate-io-impl";
  private static final String DEFAULT_DELEGATE_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";

  private FileIO delegate;

  public TimingFileIO() {}

  @Override
  public void initialize(Map<String, String> properties) {
    String impl = properties.getOrDefault(DELEGATE_IMPL, DEFAULT_DELEGATE_IMPL);
    this.delegate = CatalogUtil.loadFileIO(impl, properties, null);
  }

  @Override
  public InputFile newInputFile(String path) {
    return timed(delegate.newInputFile(path), path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return timed(delegate.newInputFile(path, length), path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
    }
  }

  private static InputFile timed(InputFile file, String path) {
    FetchStats.Kind kind = classify(path);
    return kind == null ? file : new TimedInputFile(file, kind);
  }

  /**
   * Sorts files by Iceberg's naming: manifest lists are {@code snap-*.avro}, manifests are any
   * other {@code .avro} file, table metadata ends in {@code .metadata.json}, and this table's data
   * files are Parquet.
   */
  static FetchStats.Kind classify(String path) {
    String name = path.substring(path.lastIndexOf('/') + 1);
    if (name.endsWith(".metadata.json")) {
      return FetchStats.Kind.TABLE_METADATA;
    }
    if (name.endsWith(".avro")) {
      return name.startsWith("snap-") ? FetchStats.Kind.MANIFEST_LIST : FetchStats.Kind.MANIFEST;
    }
    if (name.endsWith(".parquet")) {
      return FetchStats.Kind.DATA;
    }
    return null;
  }

  private static final class TimedInputFile implements InputFile {
    private final InputFile delegate;
    private final FetchStats.Kind kind;

    TimedInputFile(InputFile delegate, FetchStats.Kind kind) {
      this.delegate = delegate;
      this.kind = kind;
    }

    @Override
    public long getLength() {
      // A HEAD request unless the caller already passed the length in.
      long start = FetchStats.begin(kind);
      try {
        return delegate.getLength();
      } finally {
        FetchStats.end(kind, start, 0);
      }
    }

    @Override
    public SeekableInputStream newStream() {
      long start = FetchStats.begin(kind);
      SeekableInputStream stream;
      try {
        stream = delegate.newStream();
      } finally {
        FetchStats.end(kind, start, 0);
      }
      FetchStats.fileOpened(kind);
      return new TimedStream(stream, kind);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      long start = FetchStats.begin(kind);
      try {
        return delegate.exists();
      } finally {
        FetchStats.end(kind, start, 0);
      }
    }
  }

  private static final class TimedStream extends SeekableInputStream {
    private final SeekableInputStream delegate;
    private final FetchStats.Kind kind;

    TimedStream(SeekableInputStream delegate, FetchStats.Kind kind) {
      this.delegate = delegate;
      this.kind = kind;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      long start = FetchStats.begin(kind);
      try {
        delegate.seek(newPos);
      } finally {
        FetchStats.end(kind, start, 0);
      }
    }

    @Override
    public int read() throws IOException {
      long start = FetchStats.begin(kind);
      int value = -1;
      try {
        value = delegate.read();
        return value;
      } finally {
        FetchStats.end(kind, start, value >= 0 ? 1 : 0);
      }
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      long start = FetchStats.begin(kind);
      int count = 0;
      try {
        count = delegate.read(buffer, offset, length);
        return count;
      } finally {
        FetchStats.end(kind, start, Math.max(count, 0));
      }
    }

    @Override
    public long skip(long n) throws IOException {
      long start = FetchStats.begin(kind);
      try {
        return delegate.skip(n);
      } finally {
        FetchStats.end(kind, start, 0);
      }
    }

    @Override
    public int available() throws IOException {
      return delegate.available();
    }

    @Override
    public void close() throws IOException {
      long start = FetchStats.begin(kind);
      try {
        delegate.close();
      } finally {
        FetchStats.end(kind, start, 0);
      }
    }
  }
}
