package ch.unibe.scg.cells;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.inject.Scope;

/**
 * The scope of the lifetime of a mapper in a pipeline. That is, a pipeline of three
 * mappers will have three different scopes, one for each mapper stage.
 * Further, {@link Mapper}s are guaranteed to run inside the scope. This is true
 * even for the {@link Mapper#close} method, which is still in scope.
 */
@Target({ TYPE, METHOD })
@Retention(RUNTIME)
@Scope
@interface PipelineStageScoped { }
