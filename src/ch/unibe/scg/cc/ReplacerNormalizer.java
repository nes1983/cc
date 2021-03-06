package ch.unibe.scg.cc;

import javax.inject.Inject;

import ch.unibe.scg.cc.regex.Replace;

/** Normalize a string by applying a series of Replaces. */
public class ReplacerNormalizer implements Normalizer {
	final private static long serialVersionUID = 1L;

	final private Replace[] replacers;

	/** Visible for testing only. */
	@Inject
	public ReplacerNormalizer(Replace[] replaces) {
		this.replacers = replaces;
	}

	@Override
	public void normalize(StringBuilder fileContents) {
		for (Replace r : replacers) {
			r.replaceAll(fileContents);
		}
	}
}