package ch.unibe.scg.cells.hadoop;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import ch.unibe.scg.cells.CellsModuleTest;
import ch.unibe.scg.cells.InMemoryPipelineTest;
import ch.unibe.scg.cells.InMemoryShufflerTest;
import ch.unibe.scg.cells.InMemorySourceTest;
import ch.unibe.scg.cells.LocalCounterTest;

/** A suite with all cells tests. */
@RunWith(Suite.class)
@Suite.SuiteClasses({
   HadoopPipelineHDFSInputTest.class,
   HadoopPipelineTest.class,
   HadoopCounterTest.class,
   HBaseCellSinkTest.class,
   CellsModuleTest.class,
   InMemoryPipelineTest.class,
   InMemoryShufflerTest.class,
   InMemorySourceTest.class,
   LocalCounterTest.class,
})
public class HadoopTestSuite {
	// nothing to do.
}
