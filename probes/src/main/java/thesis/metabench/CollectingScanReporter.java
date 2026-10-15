package thesis.metabench;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReportParser;

/**
 * Keeps Iceberg's ScanReports in memory so the Python job can collect them through py4j. Each
 * report's {@code total-planning-duration} is the time Iceberg spent reading and filtering the
 * manifest list and manifests for one scan, which is t_manifests.
 *
 * <p>Configured on the Spark catalog with {@code metrics-reporter-impl}.
 */
public class CollectingScanReporter implements MetricsReporter {

  private static final List<String> REPORTS = new ArrayList<>();

  @Override
  public void report(MetricsReport report) {
    if (report instanceof ScanReport) {
      String json = ScanReportParser.toJson((ScanReport) report);
      synchronized (REPORTS) {
        REPORTS.add(json);
      }
    }
  }

  /** Returns the reports collected since the last call as a JSON array, and forgets them. */
  public static String drainJson() {
    synchronized (REPORTS) {
      String json = "[" + String.join(",", REPORTS) + "]";
      REPORTS.clear();
      return json;
    }
  }
}
