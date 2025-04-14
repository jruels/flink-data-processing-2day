package example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.range;
import static org.apache.flink.table.api.Expressions.withColumns;

/**
 * Example for getting started with the Table & SQL API.
 *
 * <p>The example shows how to create, transform, and query a table. It should give a first
 * impression about the look-and-feel of the API without going too much into details. See the other
 * examples for using connectors or more complex operations.
 *
 * <p>In particular, the example shows how to
 *
 * <ul>
 *   <li>setup a {@link TableEnvironment},
 *   <li>use the environment for creating example tables, registering views, and executing SQL
 *       queries,
 *   <li>transform tables with filters and projections,
 *   <li>declare user-defined functions,
 *   <li>and print/collect results locally.
 * </ul>
 *
 * <p>The example executes two Flink jobs. The results are written to stdout.
 */
public final class GettingStartedExample {

    public static void main(String[] args) throws Exception {

        // setup the unified API
        // in this case: declare that the table programs should be executed in batch mode
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        final TableEnvironment env = TableEnvironment.create(settings);

        // create a table with example data without a connector required
        final Table rawGreetings =
                env.fromValues(
                        Row.of(
                                "Hello World"),
                        Row.of(
                                "Hi World"),
                        Row.of(
                                "Howdy World"));

        // handle ranges of columns easily
        final Table truncatedGreetings = rawGreetings.select(withColumns(range(1, 1)));

        // name columns
        final Table namedGreetings =
                truncatedGreetings.as("greeting");

        // register a view temporarily
        env.createTemporaryView("greetings", namedGreetings);

        // use SQL whenever you like
        // call execute() and print() to get insights
        env.sqlQuery("SELECT greeting from greetings")
                .execute()
                .print();

      }
}
