package ch.unibe.scg.cells;

import com.google.inject.Module;

/**
 * A module that holds all that the table-specific parameters needed to obtain tables.
 * HBase tables, for example, need their table name, and column family set. An HBaseTableModule
 * will set table name and column family.
 * InMemoryTables need nothing at all, so for them, an empty TableModule will do fine.
 * Since in memory tables need nothing, an HBaseTableModule can be used for both
 * in memory tables, and HBase tables.

 * <p>
 * Other backends might need additional parameters. You can set all of them in one module.
 */
public interface TableModule extends Module {}