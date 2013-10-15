package ch.unibe.scg.cells;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Test;

/** Check {@link LocalCounter}. */
public final class LocalCounterTest {
	/**Checks that counter stays alive after being serialized */
	@Test
	public void testCounterSerializationIsLive() throws IOException {
		LocalCounter  cnt = new LocalCounter("cnt1");
		LocalCounter cntCopy = ShallowSerializingCopy.clone(cnt);
		cntCopy.increment(1L);

		assertThat(cntCopy.toString(), equalTo("cnt1: 1"));
		assertThat(cnt.toString(), equalTo("cnt1: 1"));
	}
}
