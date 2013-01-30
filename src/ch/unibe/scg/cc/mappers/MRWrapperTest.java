package ch.unibe.scg.cc.mappers;

import junit.framework.Assert;

import org.junit.Test;

public class MRWrapperTest {
	@Test
	public void testIsSubclassOf() {
		Assert.assertTrue(MRWrapper.isSubclassOf(Integer.class, Object.class));
		Assert.assertFalse(MRWrapper.isSubclassOf(Object.class, Integer.class));
	}
}
