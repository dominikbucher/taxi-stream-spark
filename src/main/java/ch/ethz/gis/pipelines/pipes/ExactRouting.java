package ch.ethz.gis.pipelines.pipes;

/**
 * ExactRouting uses an external service to resolve the exact path a Taxi takes to serve a potential Client, and how
 * long this route would take.
 * <p>
 * There are several ideas how to implement this:
 * <ul>
 * <li>
 * Extract this from the pipeline by having an individual component (on the driver) sending off the requests (this
 * could be distributed as well, but since only sending and waiting for responses is not a very resource-intensive
 * task this will not be necessary yet). This component then waits for the responses and pipes them back into the
 * streaming part, where they are merged with the stateful ClientRequest stream (where the Taxi-ClientRequest match
 * is updated).
 * </li>
 * <li>
 * We leave this within the stream which simply prolongs the streaming part. This might be more risky, as Spark will
 * not like the delays too much.
 * </li>
 * </ul>
 */
public class ExactRouting {
}
