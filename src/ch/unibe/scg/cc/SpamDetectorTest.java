package ch.unibe.scg.cc;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import ch.unibe.scg.cc.SpamDetector.FeatureVector;

/** Test {@link SpamDetector} */
public class SpamDetectorTest {
	SpamDetector spamDetector;

	/** Setup */
	@Before
	public void setUp() {
		spamDetector = new SpamDetector();
	}

	/** Test {@link SpamDetector#isSpamByParameters(FeatureVector)} */
	@Test
	public void testIsSpamResult() {
		String doc1 = "How is { life? + 12 - bbb }";
		String doc2 = "How is Brian? {++; k - 2; ab";
		FeatureVector fv = spamDetector.extractFeatureVector(doc1, doc2);
		if (Math.abs(fv.vocabularySimilarity - 0.286) > 0.01) {
			Assert.fail("Vocabulary similarity should be 2/7, but was " + fv.vocabularySimilarity);
		}

		assertThat(spamDetector.isSpamByParameters(fv), is(true));
	}
}
