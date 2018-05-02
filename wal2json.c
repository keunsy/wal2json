#include "postgres.h"

#include "catalog/pg_type.h"

#include "replication/logical.h"

#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <unistd.h>

PG_MODULE_MAGIC;

extern void		_PG_init(void);
extern void	PGDLLEXPORT	_PG_output_plugin_init(OutputPluginCallbacks *cb);

typedef struct
{
	MemoryContext context;
	bool		include_xids;		/* include transaction ids */
	bool		include_timestamp;	/* include transaction timestamp */
	bool		include_schemas;	/* qualify tables */
	bool		include_types;		/* include data types */
	bool		include_type_oids;	/* include data type oids */
	bool		include_typmod;		/* include typmod in types */
	bool		include_not_null;	/* include not-null constraints */
	bool		include_unchanged_toast;	/* include unchanged TOAST field values in output */

	bool		pretty_print;		/* pretty-print JSON? */
	bool		write_in_chunks;	/* write in chunks? */

	List		*filter_tables;		/* filter out tables */
	List		*add_tables;		/* add only these tables */

	/*
	 * LSN pointing to the end of commit record + 1 (txn->end_lsn)
	 * It is useful for tools that wants a position to restart from.
	 */
	bool		include_lsn;		/* include LSNs */

	uint64		nr_changes;			/* # of passes in pg_decode_change() */
									/* FIXME replace with txn->nentries */

	bool		use_socket;	        /*directly return result or by socket ,default for true*/
    /* required start */
	int		    socket_port;	    /*socket connect server port */
	char		*socket_ip;	        /*socket connect server ip */
	char		*topic;	            /*message group by*/
    /* required end */

} JsonDecodingData;

typedef struct SelectTable
{
	char	*schemaname;
	char	*tablename;
	bool	allschemas;				/* true means any schema */
	bool	alltables;				/* true means any table */
} SelectTable;

/* These must be available to pg_dlsym() */
static void pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext *ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
					 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx,
				 ReorderBufferTXN *txn, Relation rel,
				 ReorderBufferChange *change);
#if	PG_VERSION_NUM >= 90600
static void pg_decode_message(LogicalDecodingContext *ctx,
					ReorderBufferTXN *txn, XLogRecPtr lsn,
					bool transactional, const char *prefix,
					Size content_size, const char *content);
#endif

static bool parse_table_identifier(List *qualified_tables, char separator, List **select_tables);
static bool string_to_SelectTable(char *rawstring, char separator, List **select_tables);


void
_PG_init(void)
{
}

/* Specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

	cb->startup_cb = pg_decode_startup;
	cb->begin_cb = pg_decode_begin_txn;
	cb->change_cb = pg_decode_change;
	cb->commit_cb = pg_decode_commit_txn;
	cb->shutdown_cb = pg_decode_shutdown;
#if	PG_VERSION_NUM >= 90600
	cb->message_cb = pg_decode_message;
#endif
}

/* Initialize this plugin  创建slot以及查询时会调用*/
static void
pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
	ListCell	*option;
	JsonDecodingData *data;
	SelectTable	*t;

	data = palloc0(sizeof(JsonDecodingData));
	data->context = AllocSetContextCreate(TopMemoryContext,
										"wal2json output context",
#if PG_VERSION_NUM >= 90600
										ALLOCSET_DEFAULT_SIZES
#else
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE
#endif
                                        );
	data->include_xids = false;
	data->include_timestamp = false;
	data->include_schemas = true;
	data->include_types = true;
	data->include_type_oids = false;
	data->include_typmod = true;
	data->pretty_print = false;
	data->write_in_chunks = false;
	data->include_lsn = false;
	data->include_not_null = false;
	data->include_unchanged_toast = true;
	data->filter_tables = NIL;

    //myupdate
	data->socket_port = 0;
	data->use_socket = true;

	/* add all tables in all schemas by default */
	t = palloc0(sizeof(SelectTable));
	t->allschemas = true;
	t->alltables = true;
	data->add_tables = lappend(data->add_tables, t);

	data->nr_changes = 0;

	ctx->output_plugin_private = data;

	opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

	foreach(option, ctx->output_plugin_options)
	{
		DefElem *elem = lfirst(option);

		Assert(elem->arg == NULL || IsA(elem->arg, String));

		if (strcmp(elem->defname, "include-xids") == 0)
		{
			/* If option does not provide a value, it means its value is true */
			if (elem->arg == NULL)
			{
				elog(LOG, "include-xids argument is null");
				data->include_xids = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_xids))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-timestamp") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "include-timestamp argument is null");
				data->include_timestamp = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_timestamp))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-schemas") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "include-schemas argument is null");
				data->include_schemas = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_schemas))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-types") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "include-types argument is null");
				data->include_types = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_types))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-type-oids") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "include-type-oids argument is null");
				data->include_type_oids = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_type_oids))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-typmod") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "include-typmod argument is null");
				data->include_typmod = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_typmod))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-not-null") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "include-not-null argument is null");
				data->include_not_null = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_not_null))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "pretty-print") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "pretty-print argument is null");
				data->pretty_print = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->pretty_print))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "write-in-chunks") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "write-in-chunks argument is null");
				data->write_in_chunks = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->write_in_chunks))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-lsn") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "include-lsn argument is null");
				data->include_lsn = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_lsn))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "include-unchanged-toast") == 0)
		{
			if (elem->arg == NULL)
			{
				elog(LOG, "include-unchanged-toast is null");
				data->include_unchanged_toast = true;
			}
			else if (!parse_bool(strVal(elem->arg), &data->include_unchanged_toast))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
		}
		else if (strcmp(elem->defname, "filter-tables") == 0)
		{
			char	*rawstr;

			if (elem->arg == NULL)
			{
				elog(LOG, "filter-tables argument is null");
				data->filter_tables = NIL;
			}

			rawstr = pstrdup(strVal(elem->arg));
			if (!string_to_SelectTable(rawstr, ',', &data->filter_tables))
			{
				pfree(rawstr);
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_NAME),
						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
							 strVal(elem->arg), elem->defname)));
			}
			pfree(rawstr);
		}
		else if (strcmp(elem->defname, "add-tables") == 0)
		{
			char	*rawstr;

			/*
			 * If this parameter is specified, remove 'all tables in all
			 * schemas' value from list.
			 */
			list_free_deep(data->add_tables);
			data->add_tables = NIL;

			if (elem->arg == NULL)
			{
				elog(LOG, "add-tables argument is null");
				data->add_tables = NIL;
			}
			else
			{
				rawstr = pstrdup(strVal(elem->arg));
				if (!string_to_SelectTable(rawstr, ',', &data->add_tables))
				{
					pfree(rawstr);
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_NAME),
							 errmsg("could not parse value \"%s\" for parameter \"%s\"",
								 strVal(elem->arg), elem->defname)));
				}
				pfree(rawstr);
			}
		}
		//myupdate
		else if (strcmp(elem->defname, "topic") == 0)
        {
             data->topic = strVal(elem->arg);
        }
		else if (strcmp(elem->defname, "socket-ip") == 0)
        {
        	data->socket_ip = strVal(elem->arg);
        }
        else if (strcmp(elem->defname, "socket-port") == 0)
        {
            if (elem->arg == NULL)
            {
            	elog(LOG, "socket-port argument is null");
            	data->socket_port = 0;
            }
            else if ( atoi(strVal(elem->arg)) <= 0)
            {
                        	ereport(ERROR,
                        			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        			 errmsg("could not parse value \"%s\" for parameter \"%s\"",
                        				 strVal(elem->arg), elem->defname)));

            }
            else
            {
                data->socket_port = atoi(strVal(elem->arg));
            }
        }
        else if (strcmp(elem->defname, "use-socket") == 0)
        		{
        			if (elem->arg == NULL)
        			{
        				elog(LOG, "use-socket argument is null");
        				data->use_socket = true;
        			}
        			else if (!parse_bool(strVal(elem->arg), &data->use_socket))
        				ereport(ERROR,
        						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        						 errmsg("could not parse value \"%s\" for parameter \"%s\"",
        							 strVal(elem->arg), elem->defname)));
        		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" = \"%s\" is unknown",
						elem->defname,
						elem->arg ? strVal(elem->arg) : "(null)")));
		}
	}

}

/* cleanup this plugin's resources */
static void
pg_decode_shutdown(LogicalDecodingContext *ctx)
{
	JsonDecodingData *data = ctx->output_plugin_private;

	/* cleanup our own resources via memory context reset */
	MemoryContextDelete(data->context);
}

/* BEGIN callback */
static void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{

	JsonDecodingData *data = ctx->output_plugin_private;

    if(data->use_socket){
        if(data->topic == NULL ){
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_NAME),
                     errmsg("option \"%s\" is required","topic")));
        }
        if(data->socket_ip == NULL ){
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_NAME),
                     errmsg("option \"%s\" is required","socket-ip")));
        }
        if(data->socket_port == 0 ){
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_NAME),
                     errmsg("option \"%s\" is required","socket-port")));
        }
    }

	data->nr_changes = 0;


}

/* COMMIT callback */
static void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
					 XLogRecPtr commit_lsn)
{
	JsonDecodingData *data = ctx->output_plugin_private;

	if (txn->has_catalog_changes)
		elog(DEBUG1, "txn has catalog changes: yes");
	else
		elog(DEBUG1, "txn has catalog changes: no");
	elog(DEBUG1, "my change counter: %lu ; # of changes: %lu ; # of changes in memory: %lu", data->nr_changes, txn->nentries, txn->nentries_mem);
	elog(DEBUG1, "# of subxacts: %d", txn->nsubtxns);

}


/*
 * Accumulate tuple information and stores it at the end
 *
 * replident: is this tuple a replica identity?
 * hasreplident: does this tuple has an associated replica identity?
 */
static void
tuple_to_stringinfo(LogicalDecodingContext *ctx, TupleDesc tupdesc, HeapTuple tuple, TupleDesc indexdesc, bool replident, bool hasreplident)
{
	JsonDecodingData	*data;
	int					natt;

	StringInfoData		colnames;
	StringInfoData		coltypes;
	StringInfoData		coltypeoids;
	StringInfoData		colnotnulls;
	StringInfoData		colvalues;
	char				*comma = "";

	data = ctx->output_plugin_private;

	initStringInfo(&colnames);
	initStringInfo(&coltypes);
	if (data->include_type_oids)
		initStringInfo(&coltypeoids);
	if (data->include_not_null)
		initStringInfo(&colnotnulls);
	initStringInfo(&colvalues);

	/*
	 * If replident is true, it will output info about replica identity. In this
	 * case, there are special JSON objects for it. Otherwise, it will print new
	 * tuple data.
	 */
	if (replident)
	{
		if (data->pretty_print)
		{
			appendStringInfoString(&colnames, "\t\t\t\"oldkeys\": {\n");
			appendStringInfoString(&colnames, "\t\t\t\t\"keynames\": [");
			appendStringInfoString(&coltypes, "\t\t\t\t\"keytypes\": [");
			if (data->include_type_oids)
				appendStringInfoString(&coltypeoids, "\t\t\t\"keytypeoids\": [");
			appendStringInfoString(&colvalues, "\t\t\t\t\"keyvalues\": [");
		}
		else
		{
			appendStringInfoString(&colnames, "\"oldkeys\":{");
			appendStringInfoString(&colnames, "\"keynames\":[");
			appendStringInfoString(&coltypes, "\"keytypes\":[");
			if (data->include_type_oids)
				appendStringInfoString(&coltypeoids, "\"keytypeoids\": [");
			appendStringInfoString(&colvalues, "\"keyvalues\":[");
		}
	}
	else
	{
		if (data->pretty_print)
		{
			appendStringInfoString(&colnames, "\t\t\t\"columnnames\": [");
			appendStringInfoString(&coltypes, "\t\t\t\"columntypes\": [");
			if (data->include_type_oids)
				appendStringInfoString(&coltypeoids, "\t\t\t\"columntypeoids\": [");
			if (data->include_not_null)
				appendStringInfoString(&colnotnulls, "\t\t\t\"columnoptionals\": [");
			appendStringInfoString(&colvalues, "\t\t\t\"columnvalues\": [");
		}
		else
		{
			appendStringInfoString(&colnames, "\"columnnames\":[");
			appendStringInfoString(&coltypes, "\"columntypes\":[");
			if (data->include_type_oids)
				appendStringInfoString(&coltypeoids, "\"columntypeoids\": [");
			if (data->include_not_null)
				appendStringInfoString(&colnotnulls, "\"columnoptionals\": [");
			appendStringInfoString(&colvalues, "\"columnvalues\":[");
		}
	}

	/* Print column information (name, type, value) */
	for (natt = 0; natt < tupdesc->natts; natt++)
	{
		Form_pg_attribute	attr;		/* the attribute itself */
		Oid					typid;		/* type of current attribute */
		HeapTuple			type_tuple;	/* information about a type */
		Oid					typoutput;	/* output function */
		bool				typisvarlena;
		Datum				origval;	/* possibly toasted Datum */
		Datum				val;		/* definitely detoasted Datum */
		char				*outputstr = NULL;
		bool				isnull;		/* column is null? */

		/*
		 * Commit d34a74dd064af959acd9040446925d9d53dff15b introduced
		 * TupleDescAttr() in back branches. If the version supports
		 * this macro, use it. Version 10 and later already support it.
		 */
#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 90605) || (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90509) || (PG_VERSION_NUM >= 90400 && PG_VERSION_NUM < 90414)
		attr = tupdesc->attrs[natt];
#else
		attr = TupleDescAttr(tupdesc, natt);
#endif

		elog(DEBUG1, "attribute \"%s\" (%d/%d)", NameStr(attr->attname), natt, tupdesc->natts);

		/* Do not print dropped or system columns */
		if (attr->attisdropped || attr->attnum < 0)
			continue;

		/* Search indexed columns in whole heap tuple */
		if (indexdesc != NULL)
		{
			int		j;
			bool	found_col = false;

			for (j = 0; j < indexdesc->natts; j++)
			{
				Form_pg_attribute	iattr;

				/* See explanation a few lines above. */
#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 90605) || (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90509) || (PG_VERSION_NUM >= 90400 && PG_VERSION_NUM < 90414)
				iattr = indexdesc->attrs[j];
#else
				iattr = TupleDescAttr(indexdesc, j);
#endif

				if (strcmp(NameStr(attr->attname), NameStr(iattr->attname)) == 0)
					found_col = true;

			}

			/* Print only indexed columns */
			if (!found_col)
				continue;
		}

		typid = attr->atttypid;

		/* Figure out type name */
		type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
		if (!HeapTupleIsValid(type_tuple))
			elog(ERROR, "cache lookup failed for type %u", typid);

		/* Get information needed for printing values of a type */
		getTypeOutputInfo(typid, &typoutput, &typisvarlena);

		/* Get Datum from tuple */
		origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

		/* Skip nulls iif printing key/identity */

		//fixme 不进行continue 反而比较快？why？
//		if (isnull && replident)
//			continue;

		if (!isnull && typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval) && !data->include_unchanged_toast)
		{
			/* TOAST value is not returned if include-unchanged-toast is specified */
			elog(DEBUG2, "column \"%s\" has an unchanged TOAST - excluding", NameStr(attr->attname));
			continue;
		}

		/* Accumulate each column info */
		appendStringInfoString(&colnames, comma);
		escape_json(&colnames, NameStr(attr->attname));

		if (data->include_types)
		{
			if (data->include_typmod)
			{
				char	*type_str;

				type_str = TextDatumGetCString(DirectFunctionCall2(format_type, attr->atttypid, attr->atttypmod));
				appendStringInfoString(&coltypes, comma);
				escape_json(&coltypes, type_str);
				pfree(type_str);
			}
			else
			{
				Form_pg_type type_form = (Form_pg_type) GETSTRUCT(type_tuple);
				appendStringInfoString(&coltypes, comma);
				escape_json(&coltypes, NameStr(type_form->typname));
			}

			/* oldkeys doesn't print not-null constraints */
			if (!replident && data->include_not_null)
			{
				if (attr->attnotnull)
					appendStringInfo(&colnotnulls, "%sfalse", comma);
				else
					appendStringInfo(&colnotnulls, "%strue", comma);
			}
		}

		if (data->include_type_oids)
			appendStringInfo(&coltypeoids, "%s%u", comma, typid);

		ReleaseSysCache(type_tuple);

		if (isnull)
		{
			appendStringInfo(&colvalues, "%snull", comma);
		}
		else
		{
			if (typisvarlena)
				val = PointerGetDatum(PG_DETOAST_DATUM(origval));
			else
				val = origval;

			/* Finally got the value */
			outputstr = OidOutputFunctionCall(typoutput, val);

			/*
			 * Data types are printed with quotes unless they are number, true,
			 * false, null, an array or an object.
			 *
			 * The NaN and Infinity are not valid JSON symbols. Hence,
			 * regardless of sign they are represented as the string null.
			 */
			switch (typid)
			{
				case INT2OID:
				case INT4OID:
				case INT8OID:
				case OIDOID:
				case FLOAT4OID:
				case FLOAT8OID:
				case NUMERICOID:
					if (pg_strncasecmp(outputstr, "NaN", 3) == 0 ||
							pg_strncasecmp(outputstr, "Infinity", 8) == 0 ||
							pg_strncasecmp(outputstr, "-Infinity", 9) == 0)
					{
						appendStringInfo(&colvalues, "%snull", comma);
						elog(DEBUG1, "attribute \"%s\" is special: %s", NameStr(attr->attname), outputstr);
					}
					else if (strspn(outputstr, "0123456789+-eE.") == strlen(outputstr))
						appendStringInfo(&colvalues, "%s%s", comma, outputstr);
					else
						elog(ERROR, "%s is not a number", outputstr);
					break;
				case BOOLOID:
					if (strcmp(outputstr, "t") == 0)
						appendStringInfo(&colvalues, "%strue", comma);
					else
						appendStringInfo(&colvalues, "%sfalse", comma);
					break;
				case BYTEAOID:
					appendStringInfoString(&colvalues, comma);
					// XXX: strings here are "\xC0FFEE", we strip the "\x"
					escape_json(&colvalues, (outputstr+2));
					break;
				default:
					appendStringInfoString(&colvalues, comma);
					escape_json(&colvalues, outputstr);
					break;
			}
		}

		/* The first column does not have comma */
		if (strcmp(comma, "") == 0)
		{
			if (data->pretty_print)
				comma = ", ";
			else
				comma = ",";
		}
	}

	/* Column info ends */
	if (replident)
	{
		if (data->pretty_print)
		{
			appendStringInfoString(&colnames, "],\n");
			if (data->include_types)
				appendStringInfoString(&coltypes, "],\n");
			if (data->include_type_oids)
				appendStringInfoString(&coltypeoids, "],\n");
			appendStringInfoString(&colvalues, "]\n");
			appendStringInfoString(&colvalues, "\t\t\t}\n");
		}
		else
		{
			appendStringInfoString(&colnames, "],");
			if (data->include_types)
				appendStringInfoString(&coltypes, "],");
			if (data->include_type_oids)
				appendStringInfoString(&coltypeoids, "],");
			appendStringInfoCharMacro(&colvalues, ']');
			appendStringInfoCharMacro(&colvalues, '}');
		}
	}
	else
	{
		if (data->pretty_print)
		{
			appendStringInfoString(&colnames, "],\n");
			if (data->include_types)
				appendStringInfoString(&coltypes, "],\n");
			if (data->include_type_oids)
				appendStringInfoString(&coltypeoids, "],\n");
			if (data->include_not_null)
				appendStringInfoString(&colnotnulls, "],\n");
			if (hasreplident)
				appendStringInfoString(&colvalues, "],\n");
			else
				appendStringInfoString(&colvalues, "]\n");
		}
		else
		{
			appendStringInfoString(&colnames, "],");
			if (data->include_types)
				appendStringInfoString(&coltypes, "],");
			if (data->include_type_oids)
				appendStringInfoString(&coltypeoids, "],");
			if (data->include_not_null)
				appendStringInfoString(&colnotnulls, "],");
			if (hasreplident)
				appendStringInfoString(&colvalues, "],");
			else
				appendStringInfoCharMacro(&colvalues, ']');
		}
	}

	/* Print data */
	appendStringInfoString(ctx->out, colnames.data);
	if (data->include_types)
		appendStringInfoString(ctx->out, coltypes.data);
	if (data->include_type_oids)
		appendStringInfoString(ctx->out, coltypeoids.data);
	if (data->include_not_null)
		appendStringInfoString(ctx->out, colnotnulls.data);
	appendStringInfoString(ctx->out, colvalues.data);

	pfree(colnames.data);
	pfree(coltypes.data);
	if (data->include_type_oids)
		pfree(coltypeoids.data);
	if (data->include_not_null)
		pfree(colnotnulls.data);
	pfree(colvalues.data);
}

/* Print columns information */
static void
columns_to_stringinfo(LogicalDecodingContext *ctx, TupleDesc tupdesc, HeapTuple tuple, bool hasreplident)
{
	tuple_to_stringinfo(ctx, tupdesc, tuple, NULL, false, hasreplident);
}

/* Print replica identity information */
static void
identity_to_stringinfo(LogicalDecodingContext *ctx, TupleDesc tupdesc, HeapTuple tuple, TupleDesc indexdesc)
{
	/* Last parameter does not matter */
	tuple_to_stringinfo(ctx, tupdesc, tuple, indexdesc, true, false);
}

//myupdate 发送socket
static int
send_by_socket(LogicalDecodingContext *ctx ,char *buf)
{
	int sockfd;
    struct sockaddr_in dest_addr;
    char	   result[1];

    JsonDecodingData *data = ctx->output_plugin_private;

    dest_addr.sin_family=AF_INET;
    dest_addr.sin_port=htons(data->socket_port);
    dest_addr.sin_addr.s_addr=inet_addr(data->socket_ip);
    bzero(&(dest_addr.sin_zero),8);

    sockfd = socket(AF_INET,SOCK_STREAM,0);
    // 一直到成功为止
    if(connect(sockfd,(struct sockaddr*)&dest_addr,sizeof(struct sockaddr)) < 0){
        elog(ERROR, "connect [\"%s\",\"%d\"] failed for \"%s\" ,errono: \"%d\"",data->socket_ip,data->socket_port, strerror(errno) , errno);
        return 0;
    }

    elog(DEBUG2, "connect success ,start send msg");

    if(send(sockfd,buf,strlen(buf),0) < 0  || recv(sockfd,result,sizeof(result),0) < 0 ||  strcmp(result,"1") != 0){
//         fixme 注意日志级别 如果是error 是否会中断
         elog(ERROR, "send [\"%s\",\"%d\"] failed for \"%s\" ,errono: \"%d\" ,result: \"%s\"",data->socket_ip,data->socket_port, strerror(errno) , errno ,result);
         close(sockfd);
         return 0;
    }

    close(sockfd);
    //清空
    initStringInfo(ctx->out);

    return 1;
}


/* Callback for individual changed tuples */
static void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
				 Relation relation, ReorderBufferChange *change)
{
	JsonDecodingData *data;
	Form_pg_class class_form;
	TupleDesc	tupdesc;
	MemoryContext old;

	Relation	indexrel;
	TupleDesc	indexdesc;

	char		*schemaname;
	char		*tablename;

	AssertVariableIsOfType(&pg_decode_change, LogicalDecodeChangeCB);

	data = ctx->output_plugin_private;
	class_form = RelationGetForm(relation);
	tupdesc = RelationGetDescr(relation);

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/* schema and table names are used for select tables */
	schemaname = get_namespace_name(class_form->relnamespace);
	tablename = NameStr(class_form->relname);


	/* Make sure rd_replidindex is set */
	RelationGetIndexList(relation);

	/* Sanity checks */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (change->data.tp.newtuple == NULL)
			{
				elog(WARNING, "no tuple data for INSERT in table \"%s\"", NameStr(class_form->relname));
				MemoryContextSwitchTo(old);
				MemoryContextReset(data->context);
				return;
			}
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			/*
			 * Bail out iif:
			 * (i) doesn't have a pk and replica identity is not full;
			 * (ii) replica identity is nothing.
			 */
			if (!OidIsValid(relation->rd_replidindex) && relation->rd_rel->relreplident != REPLICA_IDENTITY_FULL)
			{
				/* FIXME this sentence is imprecise */
				elog(WARNING, "table \"%s\" without primary key or replica identity is nothing", NameStr(class_form->relname));
				MemoryContextSwitchTo(old);
				MemoryContextReset(data->context);
				return;
			}

			if (change->data.tp.newtuple == NULL)
			{
				elog(WARNING, "no tuple data for UPDATE in table \"%s\"", NameStr(class_form->relname));
				MemoryContextSwitchTo(old);
				MemoryContextReset(data->context);
				return;
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			/*
			 * Bail out iif:
			 * (i) doesn't have a pk and replica identity is not full;
			 * (ii) replica identity is nothing.
			 */
			if (!OidIsValid(relation->rd_replidindex) && relation->rd_rel->relreplident != REPLICA_IDENTITY_FULL)
			{
				/* FIXME this sentence is imprecise */
				elog(WARNING, "table \"%s\" without primary key or replica identity is nothing", NameStr(class_form->relname));
				MemoryContextSwitchTo(old);
				MemoryContextReset(data->context);
				return;
			}

			if (change->data.tp.oldtuple == NULL)
			{
				elog(WARNING, "no tuple data for DELETE in table \"%s\"", NameStr(class_form->relname));
				MemoryContextSwitchTo(old);
				MemoryContextReset(data->context);
				return;
			}
			break;
		default:
			Assert(false);
	}

	/* Filter tables, if available */
	if (list_length(data->filter_tables) > 0)
	{
		ListCell	*lc;

		foreach(lc, data->filter_tables)
		{
			SelectTable	*t = lfirst(lc);

			if (t->allschemas || strcmp(t->schemaname, schemaname) == 0)
			{
				if (t->alltables || strcmp(t->tablename, tablename) == 0)
				{
					elog(DEBUG2, "\"%s\".\"%s\" was filtered out",
								((t->allschemas) ? "*" : t->schemaname),
								((t->alltables) ? "*" : t->tablename));
					return;
				}
			}
		}
	}

	/* Add tables */
	if (list_length(data->add_tables) > 0)
	{
		ListCell	*lc;
		bool		skip = true;

		/* all tables in all schemas are added by default */
		foreach(lc, data->add_tables)
		{
			SelectTable	*t = lfirst(lc);

			if (t->allschemas || strcmp(t->schemaname, schemaname) == 0)
			{
				if (t->alltables || strcmp(t->tablename, tablename) == 0)
				{
					elog(DEBUG2, "\"%s\".\"%s\" was added",
								((t->allschemas) ? "*" : t->schemaname),
								((t->alltables) ? "*" : t->tablename));
					skip = false;
				}
			}
		}

		/* table was not found */
		if (skip)
			return;
	}

	/* Change counter */
	data->nr_changes++;

	/* Change starts */
	appendStringInfoCharMacro(ctx->out, '{');

	/* Print change kind */
	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			if (data->pretty_print)
				appendStringInfoString(ctx->out, "\t\t\t\"kind\": \"insert\",\n");
			else
				appendStringInfoString(ctx->out, "\"kind\":\"insert\",");
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			if(data->pretty_print)
				appendStringInfoString(ctx->out, "\t\t\t\"kind\": \"update\",\n");
			else
				appendStringInfoString(ctx->out, "\"kind\":\"update\",");
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			if (data->pretty_print)
				appendStringInfoString(ctx->out, "\t\t\t\"kind\": \"delete\",\n");
			else
				appendStringInfoString(ctx->out, "\"kind\":\"delete\",");
			break;
		default:
			Assert(false);
	}

    //myupdate
    appendStringInfo(ctx->out, "\"timestamp\":\"%s\",", timestamptz_to_str(txn->commit_time));

    if (data->include_xids)
    {
    	appendStringInfo(ctx->out, "\"xid\":%u,", txn->xid);
    }
    if (data->include_lsn)
    {
        char *lsn_str = DatumGetCString(DirectFunctionCall1(pg_lsn_out, txn->end_lsn));

        appendStringInfo(ctx->out, "\"nextlsn\":\"%s\",", lsn_str);

        pfree(lsn_str);
    }

    if(data->use_socket){
        appendStringInfo(ctx->out, "\"topic\":\"%s\",", data->topic);
    }
    appendStringInfo(ctx->out, "\"slot_name\":\"%s\",", NameStr(ctx->slot->data.name));

    //可能是串行执行 导致 重复
    appendStringInfo(ctx->out, "\"current_num\":\"%lu\",", data->nr_changes);
    appendStringInfo(ctx->out, "\"total_num\":\"%lu\",", txn->nentries);


	/* Print table name (possibly) qualified */
	if (data->pretty_print)
	{
		if (data->include_schemas)
		{
			appendStringInfoString(ctx->out, "\t\t\t\"schema\": ");
			escape_json(ctx->out, get_namespace_name(class_form->relnamespace));
			appendStringInfoString(ctx->out, ",\n");
		}
		appendStringInfoString(ctx->out, "\t\t\t\"table\": ");
		escape_json(ctx->out, NameStr(class_form->relname));
		appendStringInfoString(ctx->out, ",\n");
	}
	else
	{
		if (data->include_schemas)
		{
			appendStringInfoString(ctx->out, "\"schema\":");
			escape_json(ctx->out, get_namespace_name(class_form->relnamespace));
			appendStringInfoCharMacro(ctx->out, ',');
		}
		appendStringInfoString(ctx->out, "\"table\":");
		escape_json(ctx->out, NameStr(class_form->relname));
		appendStringInfoCharMacro(ctx->out, ',');
	}

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			/* Print the new tuple */
			columns_to_stringinfo(ctx, tupdesc, &change->data.tp.newtuple->tuple, false);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			/* Print the new tuple */
			columns_to_stringinfo(ctx, tupdesc, &change->data.tp.newtuple->tuple, true);

			/*
			 * The old tuple is available when:
			 * (i) pk changes;
			 * (ii) replica identity is full;
			 * (iii) replica identity is index and indexed column changes.
			 *
			 * FIXME if old tuple is not available we must get only the indexed
			 * columns (the whole tuple is printed).
			 */
			if (change->data.tp.oldtuple == NULL)
			{
				elog(DEBUG1, "old tuple is null");

				indexrel = RelationIdGetRelation(relation->rd_replidindex);
				if (indexrel != NULL)
				{
					indexdesc = RelationGetDescr(indexrel);
					identity_to_stringinfo(ctx, tupdesc, &change->data.tp.newtuple->tuple, indexdesc);
					RelationClose(indexrel);
				}
				else
				{
					identity_to_stringinfo(ctx, tupdesc, &change->data.tp.newtuple->tuple, NULL);
				}
			}
			else
			{
				elog(DEBUG1, "old tuple is not null");
				identity_to_stringinfo(ctx, tupdesc, &change->data.tp.oldtuple->tuple, NULL);
			}
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			/* Print the replica identity */
			indexrel = RelationIdGetRelation(relation->rd_replidindex);
			if (indexrel != NULL)
			{
				indexdesc = RelationGetDescr(indexrel);
				identity_to_stringinfo(ctx, tupdesc, &change->data.tp.oldtuple->tuple, indexdesc);
				RelationClose(indexrel);
			}
			else
			{
				identity_to_stringinfo(ctx, tupdesc, &change->data.tp.oldtuple->tuple, NULL);
			}

			if (change->data.tp.oldtuple == NULL)
				elog(DEBUG1, "old tuple is null");
			else
				elog(DEBUG1, "old tuple is not null");
			break;
		default:
			Assert(false);
	}

	if (data->pretty_print)
		appendStringInfoString(ctx->out, "\t\t}");
	else
		appendStringInfoCharMacro(ctx->out, '}');

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);

    //myupdate 转入socket 并将ctx->初始化 为事务数量
    if (data->use_socket && data->socket_port != 0 && data->socket_ip !=NULL){

    //传输值内容
        while(send_by_socket(ctx , ctx->out->data) != 1);
    }

    //DML语句在事物commit方法中会调用，因此调换至此处进行输出 fixme 待测试
    OutputPluginPrepareWrite(ctx, true);
    if(data->use_socket){
        appendStringInfo(ctx->out, "total_num:%lu,commitTimestamp:%s",txn->nentries,timestamptz_to_str(txn->commit_time));
    }
    OutputPluginWrite(ctx, true);
}

#if	PG_VERSION_NUM >= 90600
/* Callback for generic logical decoding messages */
static void
pg_decode_message(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
		XLogRecPtr lsn, bool transactional, const char *prefix, Size
		content_size, const char *content)
{
	JsonDecodingData *data;
	MemoryContext old;
	char *content_str;

	data = ctx->output_plugin_private;

	/* Avoid leaking memory by using and resetting our own context */
	old = MemoryContextSwitchTo(data->context);

	/*
	 * write immediately iif (i) write-in-chunks=1 or (ii) non-transactional
	 * messages.
	 */
	// 屏蔽，几乎不用调用此方法
//	if (data->write_in_chunks || !transactional)
//		OutputPluginPrepareWrite(ctx, true);

	/*
	 * increment counter only for transactional messages because
	 * non-transactional message has only one object.
	 */
	if (transactional)
		data->nr_changes++;

	if (data->pretty_print)
	{
		/* if we don't write in chunks, we need a newline here */
		if (!data->write_in_chunks && transactional)
			appendStringInfoChar(ctx->out, '\n');

		/* build a complete JSON object for non-transactional message */
		if (!transactional)
		{
			appendStringInfoString(ctx->out, "{\n");
			appendStringInfoString(ctx->out, "\t\"change\": [\n");
		}

		appendStringInfoString(ctx->out, "\t\t");

		if (data->nr_changes > 1)
			appendStringInfoChar(ctx->out, ',');

		appendStringInfoString(ctx->out, "{\n");

		appendStringInfoString(ctx->out, "\t\t\t\"kind\": \"message\",\n");

		if (transactional)
			appendStringInfoString(ctx->out, "\t\t\t\"transactional\": true,\n");
		else
			appendStringInfoString(ctx->out, "\t\t\t\"transactional\": false,\n");

		appendStringInfo(ctx->out, "\t\t\t\"prefix\": ");
		escape_json(ctx->out, prefix);
		appendStringInfoString(ctx->out, ",\n\t\t\t\"content\": ");

		// force null-terminated string
		content_str = (char *)palloc0(content_size+1);
		strncpy(content_str, content, content_size);
		escape_json(ctx->out, content_str);
		pfree(content_str);

		appendStringInfoString(ctx->out, "\n\t\t}");

		/* build a complete JSON object for non-transactional message */
		if (!transactional)
		{
			appendStringInfoString(ctx->out, "\n\t]");
			appendStringInfoString(ctx->out, "\n}");
		}
	}
	else
	{
		/* build a complete JSON object for non-transactional message */
		if (!transactional)
		{
			appendStringInfoString(ctx->out, "{\"change\":[");
		}

		if (data->nr_changes > 1)
			appendStringInfoString(ctx->out, ",{");
		else
			appendStringInfoChar(ctx->out, '{');

		appendStringInfoString(ctx->out, "\"kind\":\"message\",");

		if (transactional)
			appendStringInfoString(ctx->out, "\"transactional\":true,");
		else
			appendStringInfoString(ctx->out, "\"transactional\":false,");

		appendStringInfo(ctx->out, "\"prefix\":");
		escape_json(ctx->out, prefix);
		appendStringInfoString(ctx->out, ",\"content\":");

		// force null-terminated string
		content_str = (char *)palloc0(content_size+1);
		strncpy(content_str, content, content_size);
		escape_json(ctx->out, content_str);
		pfree(content_str);

		appendStringInfoChar(ctx->out, '}');

		/* build a complete JSON object for non-transactional message */
		if (!transactional)
		{
			appendStringInfoString(ctx->out, "]}");
		}
	}

	MemoryContextSwitchTo(old);
	MemoryContextReset(data->context);

//	if (data->write_in_chunks || !transactional)
//		OutputPluginWrite(ctx, true);
}
#endif

static bool
parse_table_identifier(List *qualified_tables, char separator, List **select_tables)
{
	ListCell	*lc;

	foreach(lc, qualified_tables)
	{
		char		*str = lfirst(lc);
		char		*startp;
		char		*nextp;
		int			len;
		SelectTable	*t = palloc0(sizeof(SelectTable));

		/*
		 * Detect a special character that means all schemas. There could be a
		 * schema named "*" thus this test should be before we remove the
		 * escape character.
		 */
		if (str[0] == '*' || str[1] == '.')
			t->allschemas = true;
		else
			t->allschemas = false;

		startp = nextp = str;
		while (*nextp && *nextp != separator)
		{
			/* remove escape character */
			if (*nextp == '\\')
				memmove(nextp, nextp + 1, strlen(nextp));
			nextp++;
		}
		len = nextp - startp;

		/* if separator was not found, schema was not informed */
		if (*nextp == '\0')
		{
			pfree(t);
			return false;
		}
		else
		{
			/* schema name */
			t->schemaname = (char *) palloc0((len + 1) * sizeof(char));
			strncpy(t->schemaname, startp, len);

			nextp++;			/* jump separator */
			startp = nextp;		/* start new identifier (table name) */

			/*
			 * Detect a special character that means all tables. There could be
			 * a table named "*" thus this test should be before that we remove
			 * the escape character.
			 */
			if (startp[0] == '*' && startp[1] == '\0')
				t->alltables = true;
			else
				t->alltables = false;

			while (*nextp)
			{
				/* remove escape character */
				if (*nextp == '\\')
					memmove(nextp, nextp + 1, strlen(nextp));
				nextp++;
			}
			len = nextp - startp;

			/* table name */
			t->tablename = (char *) palloc0((len + 1) * sizeof(char));
			strncpy(t->tablename, startp, len);
		}

		*select_tables = lappend(*select_tables, t);
	}

	return true;
}

static bool
string_to_SelectTable(char *rawstring, char separator, List **select_tables)
{
	char	   *nextp;
	bool		done = false;
	List	   *qualified_tables = NIL;

	nextp = rawstring;

	while (isspace(*nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return true;			/* allow empty string */

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char	   *curname;
		char	   *endp;
		char	   *qname;

		curname = nextp;
		while (*nextp && *nextp != separator && !isspace(*nextp))
		{
			if (*nextp == '\\')
				nextp++;	/* ignore next character because of escape */
			nextp++;
		}
		endp = nextp;
		if (curname == nextp)
			return false;	/* empty unquoted name not allowed */

		while (isspace(*nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == separator)
		{
			nextp++;
			while (isspace(*nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false;		/* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/*
		 * Finished isolating current name --- add it to list
		 */
		qname = pstrdup(curname);
		qualified_tables = lappend(qualified_tables, qname);

		/* Loop back if we didn't reach end of string */
	} while (!done);

	if (!parse_table_identifier(qualified_tables, '.', select_tables))
		return false;

	list_free_deep(qualified_tables);

	return true;
}
