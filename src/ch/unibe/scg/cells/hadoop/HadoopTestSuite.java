package ch.unibe.scg.cells.hadoop;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** A suite with all hadoop-related tests. */
@RunWith(Suite.class)
@Suite.SuiteClasses({
   HadoopPipelineHDFSInputTest.class,
   HadoopPipelineTest.class,
   HadoopCounterTest.class,
   HBaseCellSinkTest.class
})
public class HadoopTestSuite {
	// nothing to do.
}
