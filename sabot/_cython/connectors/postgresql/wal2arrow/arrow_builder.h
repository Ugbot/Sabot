/*
 * arrow_builder.h
 *
 * Arrow RecordBatch builder for PostgreSQL CDC.
 */

#ifndef ARROW_BUILDER_H
#define ARROW_BUILDER_H

#include "postgres.h"
#include "access/tupdesc.h"
#include "arrow_c_data_interface.h"

/* Arrow batch builder structure */
typedef struct ArrowBatchBuilder
{
	MemoryContext context;

	/* Schema information */
	char *schema_name;
	char *table_name;
	int num_columns;

	/* Arrow C Data structures */
	struct ArrowSchema schema;
	struct ArrowArray array;

	/* Row accumulation */
	int num_rows;
	int capacity;

	/* Column buffers */
	char **column_data;
	int *column_lengths;

} ArrowBatchBuilder;

/* Function declarations */
void arrow_schema_from_tupdesc(struct ArrowSchema *schema, TupleDesc tupdesc);
void arrow_schema_release(struct ArrowSchema *schema);
void arrow_array_from_builder(struct ArrowArray *array, ArrowBatchBuilder *builder);
void arrow_array_release(struct ArrowArray *array);

#endif /* ARROW_BUILDER_H */
