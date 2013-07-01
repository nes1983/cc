package ch.unibe.scg.cc;

import javax.inject.Inject;

import ch.unibe.scg.cc.regex.Replace;

public class Normalizer implements PhaseFrontend {
	final Replace[] replacers;

	@Inject
	public Normalizer(Replace[] replaces) {
		this.replacers = replaces;
	}

	@Override
	public void normalize(StringBuilder fileContents) {
		for (Replace r : replacers) {
			r.replaceAll(fileContents);
		}
	}
}