package ch.unibe.scg.cc.mappers;

import junit.framework.Assert;

import org.junit.Test;

import ch.unibe.scg.cc.mappers.GitTablePopulator.GitTablePopulatorMapper;

public final class GitTablePopulatorTest {
	@Test
	public void testProjnameRegex() {
		GitTablePopulatorMapper gtpm = new GitTablePopulatorMapper(null, null, null, null, null, null, null, null,
				null);
		String fullPathString = "har://hdfs-haddock.unibe.ch/projects/testdata.har"
				+ "/apfel/.git/objects/pack/pack-b017c4f4e226868d8ccf4782b53dd56b5187738f.pack";
		String projName = gtpm.getProjName(fullPathString);
		Assert.assertEquals("apfel", projName);

		fullPathString = "har://hdfs-haddock.unibe.ch/projects/dataset.har/dataset/sensei/objects/pack/pack-a33a3daca1573e82c6fbbc95846a47be4690bbe4.pack";
		projName = gtpm.getProjName(fullPathString);
		Assert.assertEquals("sensei", projName);
	}
}