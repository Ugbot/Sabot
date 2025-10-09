/*
 * wal2arrow.c
 *
 * PostgreSQL logical decoding output plugin that converts WAL records to Arrow IPC format.
 *
 * Based on wal2json by Euler Taveira de Oliveira.
 * Uses Arrow C Data Interface for zero-copy integration with Sabot.
 */

#include "postgres.h"
#include "catalog/pg_type.h"
#include "replication/logical.h"
#include "replication/origin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "access/sysattr.h"

/* Arrow C Data Interface */
#include "arrow_c_data_interface.h"
#include "arrow_builder.h"

PG_MODULE_MAGIC;

/* Plugin callbacks */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

/* Logical decoding callbacks */
static void wal2arrow_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
							   bool is_init);
static void wal2arrow_shutdown(LogicalDecodingContext *ctx);
static void wal2arrow_begin_txn(LogicalDecodingContext *ctx,
								 ReorderBufferTXN *txn);
static void wal2arrow_commit_txn(LogicalDecodingContext *ctx,
								  ReorderBufferTXN *txn,
								  XLogRecPtr commit_lsn);
static void wal2arrow_change(LogicalDecodingContext *ctx,
							  ReorderBufferTXN *txn,
							  Relation relation,
							  ReorderBufferChange *change);

/* Plugin data structure */
typedef struct Wal2ArrowData
{
	MemoryContext context;
	bool include_transaction;
	int batch_size;

	ArrowBatchBuilder *builder;

} Wal2ArrowData;


/*
 * Module initialization
 */
void
_PG_init(void)
{
	/* Module load callback */
}

/*
 * Output plugin initialization callback
 */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = wal2arrow_startup;
	cb->begin_cb = wal2arrow_begin_txn;
	cb->change_cb = wal2arrow_change;
	cb->commit_cb = wal2arrow_commit_txn;
	cb->shutdown_cb = wal2arrow_shutdown;
}

/*
 * Startup callback
 */
static void
wal2arrow_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt,
				   bool is_init)
{
	Wal2ArrowData *data;

	data = palloc0(sizeof(Wal2ArrowData));
	data->context = AllocSetContextCreate(ctx->context,
										   "wal2arrow context",
										   ALLOCSET_DEFAULT_SIZES);
	data->include_transaction = true;
	data->batch_size = 1000;  /* Default batch size */
	data->builder = NULL;

	ctx->output_plugin_private = data;

	/* Use binary output mode for Arrow IPC streaming */
	opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;
}

/*
 * Shutdown callback
 */
static void
wal2arrow_shutdown(LogicalDecodingContext *ctx)
{
	Wal2ArrowData *data = ctx->output_plugin_private;

	/* Cleanup Arrow builder if exists */
	if (data->builder != NULL)
	{
		/* TODO: Release Arrow structures */
		pfree(data->builder);
	}

	/* Cleanup context */
	MemoryContextDelete(data->context);
}

/*
 * Transaction begin callback
 */
static void
wal2arrow_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	Wal2ArrowData *data = ctx->output_plugin_private;

	/* Initialize new batch builder for this transaction */
	if (data->builder == NULL)
	{
		data->builder = palloc0(sizeof(ArrowBatchBuilder));
		data->builder->context = AllocSetContextCreate(data->context,
														"arrow batch context",
														ALLOCSET_DEFAULT_SIZES);
		data->builder->capacity = data->batch_size;
		data->builder->num_rows = 0;
	}
}

/*
 * Change callback - main conversion logic
 */
static void
wal2arrow_change(LogicalDecodingContext *ctx,
				  ReorderBufferTXN *txn,
				  Relation relation,
				  ReorderBufferChange *change)
{
	Wal2ArrowData *data = ctx->output_plugin_private;
	ArrowBatchBuilder *builder = data->builder;
	Form_pg_class class_form;
	TupleDesc tupdesc;
	ReorderBufferTupleBuf *tuplebuf = NULL;
	HeapTuple tuple = NULL;

	class_form = RelationGetForm(relation);
	tupdesc = RelationGetDescr(relation);

	/* Initialize schema if this is first row */
	if (builder->schema_name == NULL)
	{
		builder->schema_name = pstrdup(get_namespace_name(class_form->relnamespace));
		builder->table_name = pstrdup(NameStr(class_form->relname));
		builder->num_columns = tupdesc->natts;

		/* Initialize Arrow schema from tuple descriptor */
		arrow_schema_from_tupdesc(&builder->schema, tupdesc);
	}

	/* Process the change based on action */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			tuplebuf = change->data.tp.newtuple;
			break;

		case REORDER_BUFFER_CHANGE_UPDATE:
			tuplebuf = change->data.tp.newtuple;
			break;

		case REORDER_BUFFER_CHANGE_DELETE:
			tuplebuf = change->data.tp.oldtuple;
			break;

		default:
			return;
	}

	/* Convert tuple to Arrow row */
	if (tuplebuf != NULL)
	{
		tuple = &tuplebuf->tuple;

		/* TODO: Add row to Arrow batch */
		/* For now, just count the rows */
		builder->num_rows++;

		/* Flush batch if full */
		if (builder->num_rows >= builder->capacity)
		{
			/* TODO: Serialize and output Arrow IPC batch */
			builder->num_rows = 0;
		}
	}
}

/*
 * Transaction commit callback
 */
static void
wal2arrow_commit_txn(LogicalDecodingContext *ctx,
					  ReorderBufferTXN *txn,
					  XLogRecPtr commit_lsn)
{
	Wal2ArrowData *data = ctx->output_plugin_private;
	ArrowBatchBuilder *builder = data->builder;

	/* Flush any remaining rows */
	if (builder != NULL && builder->num_rows > 0)
	{
		/* TODO: Serialize and output Arrow IPC batch */

		/* Clean up builder */
		MemoryContextDelete(builder->context);
		pfree(builder);
		data->builder = NULL;
	}
}
