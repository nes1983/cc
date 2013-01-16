package ch.unibe.scg.cc.lines;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.TransformerUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import ch.unibe.jexample.Given;
import ch.unibe.jexample.JExample;

@RunWith(JExample.class)
public class ModifiableLinesTest {

	@Test
	public ModifiableLines testFactory() {
		ModifiableLinesFactory f = new ModifiableLinesFactory();
		StringBuilder sb = new StringBuilder("a\n" + "b\n" + "c\n" + "d\n" + "e\n" + "f\n");
		ModifiableLines r = f.make(sb);
		Collection lineBreakPositions = CollectionUtils.collect(r.lineBreaks,
				TransformerUtils.invokerTransformer("getPosition"));
		assertThat(lineBreakPositions, contains(0, 1, 3, 5, 7, 9, 11));
		Collection lineBreakWeights = CollectionUtils.collect(r.lineBreaks,
				TransformerUtils.invokerTransformer("getWeight"));
		assertThat(lineBreakWeights, contains(1, 1, 1, 1, 1, 1, 1));
		return r;
	}

	@Given("testFactory")
	public ModifiableLines replace(ModifiableLines lines) {

		lines.replace(3, 4, "waa");
		assertThat(lines.toString(), is("a\nbwaac\nd\ne\nf\n"));
		Collection lineBreakPositions = CollectionUtils.collect(lines.lineBreaks,
				TransformerUtils.invokerTransformer("getPosition"));
		assertThat(lineBreakPositions, contains(0, 1, 7, 9, 11, 13));
		Collection lineBreakWeights = CollectionUtils.collect(lines.lineBreaks,
				TransformerUtils.invokerTransformer("getWeight"));
		assertThat(lineBreakWeights, contains(1, 1, 2, 1, 1, 1));
		return lines;
	}
}
