# Style guide #

The style of this package is based on the 
[Oracle style guide](http://www.oracle.com/technetwork/java/javase/documentation/codeconvtoc-136057.html). 
Please follow it with the following modifications:

- Compiler warnings are *not* tolerable, although they cannot always be avoided. If it is unavoidable,
	 add the smallest possibly  scoped annotation that turns off the warning, together with a *good* 
	 explanation in a comment of why the warning needs  to be disabled.
- Unit tests require no documentation, although a link to the unit under test is encouraged.
- While API documentation of all methods, including private ones, is encouraged, 
	it's only required for public methods outside of unit tests.
- Beginning comments (at the start of a file) aren't necessary
- Line length is not 80, but 100 chars.
- Same-line comments don't need to be shifted right, although you can.
- Declarations should NOT be declared at the beginning of blocks, but as late as possible.
- The ordering inside a class should be:
	- Inst vars
	- Constructors
	- Static inner classes
	- Public methods 
	- Private methods
- There is nothing wrong with default visibility. Use where appropriate.
- Don't use ternary operator (?:)
- In accordance with Oracle, but in contrast to Eclipse behavior: No blank line after class declaration.
- Violations of style do occur. Only change code for style that you're an expert on that code.
 	Don't change other people's code, just because it violates style. 
 	If you are an expert, and you do wish to change formatting, please do it in a separate patch,
 	that only changes formatting and nothing else.

Most important:

 - Local style beats global. Follow the style of the code around you