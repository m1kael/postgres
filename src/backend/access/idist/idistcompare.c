/*-------------------------------------------------------------------------
 *
 * nbtcompare.c
 *	  Comparison functions for btree access method.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/idist/nbtcompare.c
 *
 * NOTES
 *
 *	These functions are stored in pg_amproc.  For each operator class
 *	defined on btrees, they compute
 *
 *				compare(a, b):
 *						< 0 if a < b,
 *						= 0 if a == b,
 *						> 0 if a > b.
 *
 *	The result is always an int32 regardless of the input datatype.
 *
 *	Although any negative int32 (except INT_MIN) is acceptable for reporting
 *	"<", and any positive int32 is acceptable for reporting ">", routines
 *	that work on 32-bit or wider datatypes can't just return "a - b".
 *	That could overflow and give the wrong answer.	Also, one must not
 *	return INT_MIN to report "<", since some callers will negate the result.
 *
 *	NOTE: it is critical that the comparison function impose a total order
 *	on all non-NULL values of the data type, and that the datatype's
 *	boolean comparison operators (= < >= etc) yield results consistent
 *	with the comparison routine.  Otherwise bad behavior may ensue.
 *	(For example, the comparison operators must NOT punt when faced with
 *	NAN or other funny values; you must devise some collation sequence for
 *	all such values.)  If the datatype is not trivial, this is most
 *	reliably done by having the boolean operators invoke the same
 *	three-way comparison code that the btree function does.  Therefore,
 *	this file contains only btree support for "trivial" datatypes ---
 *	all others are in the /utils/adt/ files that implement their datatypes.
 *
 *	NOTE: these routines must not leak memory, since memory allocated
 *	during an index access won't be recovered till end of query.  This
 *	primarily affects comparison routines for toastable datatypes;
 *	they have to be careful to free any detoasted copy of an input datum.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/sortsupport.h"


Datum
idbtboolcmp(PG_FUNCTION_ARGS)
{
	bool		a = PG_GETARG_BOOL(0);
	bool		b = PG_GETARG_BOOL(1);

	PG_RETURN_INT32((int32) a - (int32) b);
}

Datum
idbtint2cmp(PG_FUNCTION_ARGS)
{
	int16		a = PG_GETARG_INT16(0);
	int16		b = PG_GETARG_INT16(1);

	PG_RETURN_INT32((int32) a - (int32) b);
}

static int
idbtint2fastcmp(Datum x, Datum y, SortSupport ssup)
{
	int16		a = DatumGetInt16(x);
	int16		b = DatumGetInt16(y);

	return (int) a - (int) b;
}

Datum
idbtint2sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = idbtint2fastcmp;
	PG_RETURN_VOID();
}

Datum
idbtint4cmp(PG_FUNCTION_ARGS)
{
	int32		a = PG_GETARG_INT32(0);
	int32		b = PG_GETARG_INT32(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

static int
idbtint4fastcmp(Datum x, Datum y, SortSupport ssup)
{
	int32		a = DatumGetInt32(x);
	int32		b = DatumGetInt32(y);

	if (a > b)
		return 1;
	else if (a == b)
		return 0;
	else
		return -1;
}

Datum
idbtint4sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = idbtint4fastcmp;
	PG_RETURN_VOID();
}

Datum
idbtint8cmp(PG_FUNCTION_ARGS)
{
	int64		a = PG_GETARG_INT64(0);
	int64		b = PG_GETARG_INT64(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

static int
idbtint8fastcmp(Datum x, Datum y, SortSupport ssup)
{
	int64		a = DatumGetInt64(x);
	int64		b = DatumGetInt64(y);

	if (a > b)
		return 1;
	else if (a == b)
		return 0;
	else
		return -1;
}

Datum
idbtint8sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = idbtint8fastcmp;
	PG_RETURN_VOID();
}

Datum
idbtint48cmp(PG_FUNCTION_ARGS)
{
	int32		a = PG_GETARG_INT32(0);
	int64		b = PG_GETARG_INT64(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

Datum
idbtint84cmp(PG_FUNCTION_ARGS)
{
	int64		a = PG_GETARG_INT64(0);
	int32		b = PG_GETARG_INT32(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

Datum
idbtint24cmp(PG_FUNCTION_ARGS)
{
	int16		a = PG_GETARG_INT16(0);
	int32		b = PG_GETARG_INT32(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

Datum
idbtint42cmp(PG_FUNCTION_ARGS)
{
	int32		a = PG_GETARG_INT32(0);
	int16		b = PG_GETARG_INT16(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

Datum
idbtint28cmp(PG_FUNCTION_ARGS)
{
	int16		a = PG_GETARG_INT16(0);
	int64		b = PG_GETARG_INT64(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

Datum
idbtint82cmp(PG_FUNCTION_ARGS)
{
	int64		a = PG_GETARG_INT64(0);
	int16		b = PG_GETARG_INT16(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

Datum
idbtoidcmp(PG_FUNCTION_ARGS)
{
	Oid			a = PG_GETARG_OID(0);
	Oid			b = PG_GETARG_OID(1);

	if (a > b)
		PG_RETURN_INT32(1);
	else if (a == b)
		PG_RETURN_INT32(0);
	else
		PG_RETURN_INT32(-1);
}

static int
idbtoidfastcmp(Datum x, Datum y, SortSupport ssup)
{
	Oid			a = DatumGetObjectId(x);
	Oid			b = DatumGetObjectId(y);

	if (a > b)
		return 1;
	else if (a == b)
		return 0;
	else
		return -1;
}

Datum
idbtoidsortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = idbtoidfastcmp;
	PG_RETURN_VOID();
}

Datum
idbtoidvectorcmp(PG_FUNCTION_ARGS)
{
	oidvector  *a = (oidvector *) PG_GETARG_POINTER(0);
	oidvector  *b = (oidvector *) PG_GETARG_POINTER(1);
	int			i;

	/* We arbitrarily choose to sort first by vector length */
	if (a->dim1 != b->dim1)
		PG_RETURN_INT32(a->dim1 - b->dim1);

	for (i = 0; i < a->dim1; i++)
	{
		if (a->values[i] != b->values[i])
		{
			if (a->values[i] > b->values[i])
				PG_RETURN_INT32(1);
			else
				PG_RETURN_INT32(-1);
		}
	}
	PG_RETURN_INT32(0);
}

Datum
idbtcharcmp(PG_FUNCTION_ARGS)
{
	char		a = PG_GETARG_CHAR(0);
	char		b = PG_GETARG_CHAR(1);

	/* Be careful to compare chars as unsigned */
	PG_RETURN_INT32((int32) ((uint8) a) - (int32) ((uint8) b));
}

Datum
idbtnamecmp(PG_FUNCTION_ARGS)
{
	Name		a = PG_GETARG_NAME(0);
	Name		b = PG_GETARG_NAME(1);

	PG_RETURN_INT32(strncmp(NameStr(*a), NameStr(*b), NAMEDATALEN));
}

static int
idbtnamefastcmp(Datum x, Datum y, SortSupport ssup)
{
	Name		a = DatumGetName(x);
	Name		b = DatumGetName(y);

	return strncmp(NameStr(*a), NameStr(*b), NAMEDATALEN);
}

Datum
idbtnamesortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = idbtnamefastcmp;
	PG_RETURN_VOID();
}
