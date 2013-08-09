/*-------------------------------------------------------------------------
 *
 * nbtxlog.c
 *	  WAL replay logic for btrees.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/idist/nbtxlog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam_xlog.h"
#include "access/idist.h"
#include "access/transam.h"
#include "storage/procarray.h"
#include "miscadmin.h"

/*
 * We must keep track of expected insertions due to page splits, and apply
 * them manually if they are not seen in the WAL log during replay.  This
 * makes it safe for page insertion to be a multiple-WAL-action process.
 *
 * Similarly, deletion of an only child page and deletion of its parent page
 * form multiple WAL log entries, and we have to be prepared to follow through
 * with the deletion if the log ends between.
 *
 * The data structure is a simple linked list --- this should be good enough,
 * since we don't expect a page split or multi deletion to remain incomplete
 * for long.  In any case we need to respect the order of operations.
 */
typedef struct idbt_incomplete_action
{
	RelFileNode node;			/* the index */
	bool		is_split;		/* T = pending split, F = pending delete */
	/* these fields are for a split: */
	bool		is_root;		/* we split the root */
	BlockNumber leftblk;		/* left half of split */
	BlockNumber rightblk;		/* right half of split */
	/* these fields are for a delete: */
	BlockNumber delblk;			/* parent block to be deleted */
} idbt_incomplete_action;

static List *incomplete_actions;


static void
id_log_incomplete_split(RelFileNode node, BlockNumber leftblk,
					 BlockNumber rightblk, bool is_root)
{
	idbt_incomplete_action *action = palloc(sizeof(idbt_incomplete_action));

	action->node = node;
	action->is_split = true;
	action->is_root = is_root;
	action->leftblk = leftblk;
	action->rightblk = rightblk;
	incomplete_actions = lappend(incomplete_actions, action);
}

static void
id_forget_matching_split(RelFileNode node, BlockNumber downlink, bool is_root)
{
	ListCell   *l;

	foreach(l, incomplete_actions)
	{
		idbt_incomplete_action *action = (idbt_incomplete_action *) lfirst(l);

		if (RelFileNodeEquals(node, action->node) &&
			action->is_split &&
			downlink == action->rightblk)
		{
			if (is_root != action->is_root)
				elog(LOG, "id_forget_matching_split: fishy is_root data (expected %d, got %d)",
					 action->is_root, is_root);
			incomplete_actions = list_delete_ptr(incomplete_actions, action);
			pfree(action);
			break;				/* need not look further */
		}
	}
}

static void
id_log_incomplete_deletion(RelFileNode node, BlockNumber delblk)
{
	idbt_incomplete_action *action = palloc(sizeof(idbt_incomplete_action));

	action->node = node;
	action->is_split = false;
	action->delblk = delblk;
	incomplete_actions = lappend(incomplete_actions, action);
}

static void
id_forget_matching_deletion(RelFileNode node, BlockNumber delblk)
{
	ListCell   *l;

	foreach(l, incomplete_actions)
	{
		idbt_incomplete_action *action = (idbt_incomplete_action *) lfirst(l);

		if (RelFileNodeEquals(node, action->node) &&
			!action->is_split &&
			delblk == action->delblk)
		{
			incomplete_actions = list_delete_ptr(incomplete_actions, action);
			pfree(action);
			break;				/* need not look further */
		}
	}
}

/*
 * _idbt_restore_page -- re-enter all the index tuples on a page
 *
 * The page is freshly init'd, and *from (length len) is a copy of what
 * had been its upper part (pd_upper to pd_special).  We assume that the
 * tuples had been added to the page in item-number order, and therefore
 * the one with highest item number appears first (lowest on the page).
 *
 * NOTE: the way this routine is coded, the rebuilt page will have the items
 * in correct itemno sequence, but physically the opposite order from the
 * original, because we insert them in the opposite of itemno order.  This
 * does not matter in any current btree code, but it's something to keep an
 * eye on.	Is it worth changing just on general principles?  See also the
 * notes in idist_xlog_split().
 */
static void
_idbt_restore_page(Page page, char *from, int len)
{
	IndexTupleData itupdata;
	Size		itemsz;
	char	   *end = from + len;

	for (; from < end;)
	{
		/* Need to copy tuple header due to alignment considerations */
		memcpy(&itupdata, from, sizeof(IndexTupleData));
		itemsz = IndexTupleDSize(itupdata);
		itemsz = MAXALIGN(itemsz);
		if (PageAddItem(page, (Item) from, itemsz, FirstOffsetNumber,
						false, false) == InvalidOffsetNumber)
			elog(PANIC, "_idbt_restore_page: cannot add item to page");
		from += itemsz;
	}
}

static void
_idbt_restore_meta(RelFileNode rnode, XLogRecPtr lsn,
				 BlockNumber root, uint32 level,
				 BlockNumber fastroot, uint32 fastlevel)
{
	Buffer		metabuf;
	Page		metapg;
	IDBTMetaPageData *md;
	IDBTPageOpaque pageop;

	metabuf = XLogReadBuffer(rnode, IDIST_METAPAGE, true);
	Assert(BufferIsValid(metabuf));
	metapg = BufferGetPage(metabuf);

	_idbt_pageinit(metapg, BufferGetPageSize(metabuf));

	md = IDBTPageGetMeta(metapg);
	md->btm_magic = IDIST_MAGIC;
	md->btm_version = IDIST_VERSION;
	md->btm_root = root;
	md->btm_level = level;
	md->btm_fastroot = fastroot;
	md->btm_fastlevel = fastlevel;

	pageop = (IDBTPageOpaque) PageGetSpecialPointer(metapg);
	pageop->btpo_flags = IDBTP_META;

	/*
	 * Set pd_lower just past the end of the metadata.	This is not essential
	 * but it makes the page look compressible to xlog.c.
	 */
	((PageHeader) metapg)->pd_lower =
		((char *) md + sizeof(IDBTMetaPageData)) - (char *) metapg;

	PageSetLSN(metapg, lsn);
	MarkBufferDirty(metabuf);
	UnlockReleaseBuffer(metabuf);
}

static void
idist_xlog_insert(bool isleaf, bool ismeta,
				  XLogRecPtr lsn, XLogRecord *record)
{
	xl_idist_insert *xlrec = (xl_idist_insert *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	char	   *datapos;
	int			datalen;
	xl_idist_metadata md;
	BlockNumber downlink = 0;

	datapos = (char *) xlrec + SizeOfIDBtreeInsert;
	datalen = record->xl_len - SizeOfIDBtreeInsert;
	if (!isleaf)
	{
		memcpy(&downlink, datapos, sizeof(BlockNumber));
		datapos += sizeof(BlockNumber);
		datalen -= sizeof(BlockNumber);
	}
	if (ismeta)
	{
		memcpy(&md, datapos, sizeof(xl_idist_metadata));
		datapos += sizeof(xl_idist_metadata);
		datalen -= sizeof(xl_idist_metadata);
	}

	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
	{
		buffer = XLogReadBuffer(xlrec->target.node,
							 ItemPointerGetBlockNumber(&(xlrec->target.tid)),
								false);
		if (BufferIsValid(buffer))
		{
			page = (Page) BufferGetPage(buffer);

			if (lsn <= PageGetLSN(page))
			{
				UnlockReleaseBuffer(buffer);
			}
			else
			{
				if (PageAddItem(page, (Item) datapos, datalen,
							ItemPointerGetOffsetNumber(&(xlrec->target.tid)),
								false, false) == InvalidOffsetNumber)
					elog(PANIC, "idist_insert_redo: failed to add item");

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
				UnlockReleaseBuffer(buffer);
			}
		}
	}

	/*
	 * Note: in normal operation, we'd update the metapage while still holding
	 * lock on the page we inserted into.  But during replay it's not
	 * necessary to hold that lock, since no other index updates can be
	 * happening concurrently, and readers will cope fine with following an
	 * obsolete link from the metapage.
	 */
	if (ismeta)
		_idbt_restore_meta(xlrec->target.node, lsn,
						 md.root, md.level,
						 md.fastroot, md.fastlevel);

	/* Forget any split this insertion completes */
	if (!isleaf)
		id_forget_matching_split(xlrec->target.node, downlink, false);
}

static void
idist_xlog_split(bool onleft, bool isroot,
				 XLogRecPtr lsn, XLogRecord *record)
{
	xl_idist_split *xlrec = (xl_idist_split *) XLogRecGetData(record);
	Buffer		rbuf;
	Page		rpage;
	IDBTPageOpaque ropaque;
	char	   *datapos;
	int			datalen;
	OffsetNumber newitemoff = 0;
	Item		newitem = NULL;
	Size		newitemsz = 0;
	Item		left_hikey = NULL;
	Size		left_hikeysz = 0;

	datapos = (char *) xlrec + SizeOfIDBtreeSplit;
	datalen = record->xl_len - SizeOfIDBtreeSplit;

	/* Forget any split this insertion completes */
	if (xlrec->level > 0)
	{
		/* we assume SizeOfIDBtreeSplit is at least 16-bit aligned */
		BlockNumber downlink = BlockIdGetBlockNumber((BlockId) datapos);

		datapos += sizeof(BlockIdData);
		datalen -= sizeof(BlockIdData);

		id_forget_matching_split(xlrec->node, downlink, false);

		/* Extract left hikey and its size (still assuming 16-bit alignment) */
		if (!(record->xl_info & XLR_BKP_BLOCK(0)))
		{
			/* We assume 16-bit alignment is enough for IndexTupleSize */
			left_hikey = (Item) datapos;
			left_hikeysz = MAXALIGN(IndexTupleSize(left_hikey));

			datapos += left_hikeysz;
			datalen -= left_hikeysz;
		}
	}

	/* Extract newitem and newitemoff, if present */
	if (onleft)
	{
		/* Extract the offset (still assuming 16-bit alignment) */
		memcpy(&newitemoff, datapos, sizeof(OffsetNumber));
		datapos += sizeof(OffsetNumber);
		datalen -= sizeof(OffsetNumber);
	}

	if (onleft && !(record->xl_info & XLR_BKP_BLOCK(0)))
	{
		/*
		 * We assume that 16-bit alignment is enough to apply IndexTupleSize
		 * (since it's fetching from a uint16 field) and also enough for
		 * PageAddItem to insert the tuple.
		 */
		newitem = (Item) datapos;
		newitemsz = MAXALIGN(IndexTupleSize(newitem));
		datapos += newitemsz;
		datalen -= newitemsz;
	}

	/* Reconstruct right (new) sibling page from scratch */
	rbuf = XLogReadBuffer(xlrec->node, xlrec->rightsib, true);
	Assert(BufferIsValid(rbuf));
	rpage = (Page) BufferGetPage(rbuf);

	_idbt_pageinit(rpage, BufferGetPageSize(rbuf));
	ropaque = (IDBTPageOpaque) PageGetSpecialPointer(rpage);

	ropaque->btpo_prev = xlrec->leftsib;
	ropaque->btpo_next = xlrec->rnext;
	ropaque->btpo.level = xlrec->level;
	ropaque->btpo_flags = (xlrec->level == 0) ? IDBTP_LEAF : 0;
	ropaque->btpo_cycleid = 0;

	_idbt_restore_page(rpage, datapos, datalen);

	/*
	 * On leaf level, the high key of the left page is equal to the first key
	 * on the right page.
	 */
	if (xlrec->level == 0)
	{
		ItemId		hiItemId = PageGetItemId(rpage, IDP_FIRSTDATAKEY(ropaque));

		left_hikey = PageGetItem(rpage, hiItemId);
		left_hikeysz = ItemIdGetLength(hiItemId);
	}

	PageSetLSN(rpage, lsn);
	MarkBufferDirty(rbuf);

	/* don't release the buffer yet; we touch right page's first item below */

	/* Now reconstruct left (original) sibling page */
	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
	{
		Buffer		lbuf = XLogReadBuffer(xlrec->node, xlrec->leftsib, false);

		if (BufferIsValid(lbuf))
		{
			/*
			 * Note that this code ensures that the items remaining on the
			 * left page are in the correct item number order, but it does not
			 * reproduce the physical order they would have had.  Is this
			 * worth changing?	See also _idbt_restore_page().
			 */
			Page		lpage = (Page) BufferGetPage(lbuf);
			IDBTPageOpaque lopaque = (IDBTPageOpaque) PageGetSpecialPointer(lpage);

			if (lsn > PageGetLSN(lpage))
			{
				OffsetNumber off;
				OffsetNumber maxoff = PageGetMaxOffsetNumber(lpage);
				OffsetNumber deletable[MaxOffsetNumber];
				int			ndeletable = 0;

				/*
				 * Remove the items from the left page that were copied to the
				 * right page.	Also remove the old high key, if any. (We must
				 * remove everything before trying to insert any items, else
				 * we risk not having enough space.)
				 */
				if (!IDP_RIGHTMOST(lopaque))
				{
					deletable[ndeletable++] = IDP_HIKEY;

					/*
					 * newitemoff is given to us relative to the original
					 * page's item numbering, so adjust it for this deletion.
					 */
					newitemoff--;
				}
				for (off = xlrec->firstright; off <= maxoff; off++)
					deletable[ndeletable++] = off;
				if (ndeletable > 0)
					PageIndexMultiDelete(lpage, deletable, ndeletable);

				/*
				 * Add the new item if it was inserted on left page.
				 */
				if (onleft)
				{
					if (PageAddItem(lpage, newitem, newitemsz, newitemoff,
									false, false) == InvalidOffsetNumber)
						elog(PANIC, "failed to add new item to left page after split");
				}

				/* Set high key */
				if (PageAddItem(lpage, left_hikey, left_hikeysz,
								IDP_HIKEY, false, false) == InvalidOffsetNumber)
					elog(PANIC, "failed to add high key to left page after split");

				/* Fix opaque fields */
				lopaque->btpo_flags = (xlrec->level == 0) ? IDBTP_LEAF : 0;
				lopaque->btpo_next = xlrec->rightsib;
				lopaque->btpo_cycleid = 0;

				PageSetLSN(lpage, lsn);
				MarkBufferDirty(lbuf);
			}

			UnlockReleaseBuffer(lbuf);
		}
	}

	/* We no longer need the right buffer */
	UnlockReleaseBuffer(rbuf);

	/*
	 * Fix left-link of the page to the right of the new right sibling.
	 *
	 * Note: in normal operation, we do this while still holding lock on the
	 * two split pages.  However, that's not necessary for correctness in WAL
	 * replay, because no other index update can be in progress, and readers
	 * will cope properly when following an obsolete left-link.
	 */
	if (record->xl_info & XLR_BKP_BLOCK(1))
		(void) RestoreBackupBlock(lsn, record, 1, false, false);
	else if (xlrec->rnext != IDP_NONE)
	{
		Buffer		buffer = XLogReadBuffer(xlrec->node, xlrec->rnext, false);

		if (BufferIsValid(buffer))
		{
			Page		page = (Page) BufferGetPage(buffer);

			if (lsn > PageGetLSN(page))
			{
				IDBTPageOpaque pageop = (IDBTPageOpaque) PageGetSpecialPointer(page);

				pageop->btpo_prev = xlrec->rightsib;

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
			}
			UnlockReleaseBuffer(buffer);
		}
	}

	/* The job ain't done till the parent link is inserted... */
	id_log_incomplete_split(xlrec->node,
						 xlrec->leftsib, xlrec->rightsib, isroot);
}

static void
idist_xlog_vacuum(XLogRecPtr lsn, XLogRecord *record)
{
	xl_idist_vacuum *xlrec = (xl_idist_vacuum *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	IDBTPageOpaque opaque;

	/*
	 * If queries might be active then we need to ensure every block is
	 * unpinned between the lastBlockVacuumed and the current block, if there
	 * are any. This ensures that every block in the index is touched during
	 * VACUUM as required to ensure scans work correctly.
	 */
	if (standbyState == STANDBY_SNAPSHOT_READY &&
		(xlrec->lastBlockVacuumed + 1) != xlrec->block)
	{
		BlockNumber blkno = xlrec->lastBlockVacuumed + 1;

		for (; blkno < xlrec->block; blkno++)
		{
			/*
			 * XXX we don't actually need to read the block, we just need to
			 * confirm it is unpinned. If we had a special call into the
			 * buffer manager we could optimise this so that if the block is
			 * not in shared_buffers we confirm it as unpinned.
			 *
			 * Another simple optimization would be to check if there's any
			 * backends running; if not, we could just skip this.
			 */
			buffer = XLogReadBufferExtended(xlrec->node, MAIN_FORKNUM, blkno, RBM_NORMAL);
			if (BufferIsValid(buffer))
			{
				LockBufferForCleanup(buffer);
				UnlockReleaseBuffer(buffer);
			}
		}
	}

	/*
	 * If we have a full-page image, restore it (using a cleanup lock) and
	 * we're done.
	 */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, true, false);
		return;
	}

	/*
	 * Like in idbtvacuumpage(), we need to take a cleanup lock on every leaf
	 * page. See idist/README for details.
	 */
	buffer = XLogReadBufferExtended(xlrec->node, MAIN_FORKNUM, xlrec->block, RBM_NORMAL);
	if (!BufferIsValid(buffer))
		return;
	LockBufferForCleanup(buffer);
	page = (Page) BufferGetPage(buffer);

	if (lsn <= PageGetLSN(page))
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	if (record->xl_len > SizeOfIDBtreeVacuum)
	{
		OffsetNumber *unused;
		OffsetNumber *unend;

		unused = (OffsetNumber *) ((char *) xlrec + SizeOfIDBtreeVacuum);
		unend = (OffsetNumber *) ((char *) xlrec + record->xl_len);

		if ((unend - unused) > 0)
			PageIndexMultiDelete(page, unused, unend - unused);
	}

	/*
	 * Mark the page as not containing any LP_DEAD items --- see comments in
	 * _idbt_delitems_vacuum().
	 */
	opaque = (IDBTPageOpaque) PageGetSpecialPointer(page);
	opaque->btpo_flags &= ~IDBTP_HAS_GARBAGE;

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

/*
 * Get the latestRemovedXid from the heap pages pointed at by the index
 * tuples being deleted. This puts the work for calculating latestRemovedXid
 * into the recovery path rather than the primary path.
 *
 * It's possible that this generates a fair amount of I/O, since an index
 * block may have hundreds of tuples being deleted. Repeat accesses to the
 * same heap blocks are common, though are not yet optimised.
 *
 * XXX optimise later with something like XLogPrefetchBuffer()
 */
static TransactionId
idist_xlog_delete_get_latestRemovedXid(xl_idist_delete *xlrec)
{
	OffsetNumber *unused;
	Buffer		ibuffer,
				hbuffer;
	Page		ipage,
				hpage;
	ItemId		iitemid,
				hitemid;
	IndexTuple	itup;
	HeapTupleHeader htuphdr;
	BlockNumber hblkno;
	OffsetNumber hoffnum;
	TransactionId latestRemovedXid = InvalidTransactionId;
	int			i;

	/*
	 * If there's nothing running on the standby we don't need to derive a
	 * full latestRemovedXid value, so use a fast path out of here.  This
	 * returns InvalidTransactionId, and so will conflict with all HS
	 * transactions; but since we just worked out that that's zero people,
	 * it's OK.
	 *
	 * XXX There is a race condition here, which is that a new backend might
	 * start just after we look.  If so, it cannot need to conflict, but this
	 * coding will result in throwing a conflict anyway.
	 */
	if (CountDBBackends(InvalidOid) == 0)
		return latestRemovedXid;

	/*
	 * In what follows, we have to examine the previous state of the index
	 * page, as well as the heap page(s) it points to.	This is only valid if
	 * WAL replay has reached a consistent database state; which means that
	 * the preceding check is not just an optimization, but is *necessary*. We
	 * won't have let in any user sessions before we reach consistency.
	 */
	if (!reachedConsistency)
		elog(PANIC, "idist_xlog_delete_get_latestRemovedXid: cannot operate with inconsistent data");

	/*
	 * Get index page.	If the DB is consistent, this should not fail, nor
	 * should any of the heap page fetches below.  If one does, we return
	 * InvalidTransactionId to cancel all HS transactions.	That's probably
	 * overkill, but it's safe, and certainly better than panicking here.
	 */
	ibuffer = XLogReadBuffer(xlrec->node, xlrec->block, false);
	if (!BufferIsValid(ibuffer))
		return InvalidTransactionId;
	ipage = (Page) BufferGetPage(ibuffer);

	/*
	 * Loop through the deleted index items to obtain the TransactionId from
	 * the heap items they point to.
	 */
	unused = (OffsetNumber *) ((char *) xlrec + SizeOfIDBtreeDelete);

	for (i = 0; i < xlrec->nitems; i++)
	{
		/*
		 * Identify the index tuple about to be deleted
		 */
		iitemid = PageGetItemId(ipage, unused[i]);
		itup = (IndexTuple) PageGetItem(ipage, iitemid);

		/*
		 * Locate the heap page that the index tuple points at
		 */
		hblkno = ItemPointerGetBlockNumber(&(itup->t_tid));
		hbuffer = XLogReadBuffer(xlrec->hnode, hblkno, false);
		if (!BufferIsValid(hbuffer))
		{
			UnlockReleaseBuffer(ibuffer);
			return InvalidTransactionId;
		}
		hpage = (Page) BufferGetPage(hbuffer);

		/*
		 * Look up the heap tuple header that the index tuple points at by
		 * using the heap node supplied with the xlrec. We can't use
		 * heap_fetch, since it uses ReadBuffer rather than XLogReadBuffer.
		 * Note that we are not looking at tuple data here, just headers.
		 */
		hoffnum = ItemPointerGetOffsetNumber(&(itup->t_tid));
		hitemid = PageGetItemId(hpage, hoffnum);

		/*
		 * Follow any redirections until we find something useful.
		 */
		while (ItemIdIsRedirected(hitemid))
		{
			hoffnum = ItemIdGetRedirect(hitemid);
			hitemid = PageGetItemId(hpage, hoffnum);
			CHECK_FOR_INTERRUPTS();
		}

		/*
		 * If the heap item has storage, then read the header and use that to
		 * set latestRemovedXid.
		 *
		 * Some LP_DEAD items may not be accessible, so we ignore them.
		 */
		if (ItemIdHasStorage(hitemid))
		{
			htuphdr = (HeapTupleHeader) PageGetItem(hpage, hitemid);

			HeapTupleHeaderAdvanceLatestRemovedXid(htuphdr, &latestRemovedXid);
		}
		else if (ItemIdIsDead(hitemid))
		{
			/*
			 * Conjecture: if hitemid is dead then it had xids before the xids
			 * marked on LP_NORMAL items. So we just ignore this item and move
			 * onto the next, for the purposes of calculating
			 * latestRemovedxids.
			 */
		}
		else
			Assert(!ItemIdIsUsed(hitemid));

		UnlockReleaseBuffer(hbuffer);
	}

	UnlockReleaseBuffer(ibuffer);

	/*
	 * XXX If all heap tuples were LP_DEAD then we will be returning
	 * InvalidTransactionId here, causing conflict for all HS transactions.
	 * That should happen very rarely (reasoning please?). Also note that
	 * caller can't tell the difference between this case and the fast path
	 * exit above. May need to change that in future.
	 */
	return latestRemovedXid;
}

static void
idist_xlog_delete(XLogRecPtr lsn, XLogRecord *record)
{
	xl_idist_delete *xlrec = (xl_idist_delete *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	IDBTPageOpaque opaque;

	/*
	 * If we have any conflict processing to do, it must happen before we
	 * update the page.
	 *
	 * Btree delete records can conflict with standby queries.	You might
	 * think that vacuum records would conflict as well, but we've handled
	 * that already.  XLOG_HEAP2_CLEANUP_INFO records provide the highest xid
	 * cleaned by the vacuum of the heap and so we can resolve any conflicts
	 * just once when that arrives.  After that we know that no conflicts
	 * exist from individual btree vacuum records on that index.
	 */
	if (InHotStandby)
	{
		TransactionId latestRemovedXid = idist_xlog_delete_get_latestRemovedXid(xlrec);

		ResolveRecoveryConflictWithSnapshot(latestRemovedXid, xlrec->node);
	}

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	/*
	 * We don't need to take a cleanup lock to apply these changes. See
	 * idist/README for details.
	 */
	buffer = XLogReadBuffer(xlrec->node, xlrec->block, false);
	if (!BufferIsValid(buffer))
		return;
	page = (Page) BufferGetPage(buffer);

	if (lsn <= PageGetLSN(page))
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	if (record->xl_len > SizeOfIDBtreeDelete)
	{
		OffsetNumber *unused;

		unused = (OffsetNumber *) ((char *) xlrec + SizeOfIDBtreeDelete);

		PageIndexMultiDelete(page, unused, xlrec->nitems);
	}

	/*
	 * Mark the page as not containing any LP_DEAD items --- see comments in
	 * _idbt_delitems_delete().
	 */
	opaque = (IDBTPageOpaque) PageGetSpecialPointer(page);
	opaque->btpo_flags &= ~IDBTP_HAS_GARBAGE;

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
idist_xlog_delete_page(uint8 info, XLogRecPtr lsn, XLogRecord *record)
{
	xl_idist_delete_page *xlrec = (xl_idist_delete_page *) XLogRecGetData(record);
	BlockNumber parent;
	BlockNumber target;
	BlockNumber leftsib;
	BlockNumber rightsib;
	Buffer		buffer;
	Page		page;
	IDBTPageOpaque pageop;

	parent = ItemPointerGetBlockNumber(&(xlrec->target.tid));
	target = xlrec->deadblk;
	leftsib = xlrec->leftblk;
	rightsib = xlrec->rightblk;

	/*
	 * In normal operation, we would lock all the pages this WAL record
	 * touches before changing any of them.  In WAL replay, it should be okay
	 * to lock just one page at a time, since no concurrent index updates can
	 * be happening, and readers should not care whether they arrive at the
	 * target page or not (since it's surely empty).
	 */

	/* parent page */
	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
	{
		buffer = XLogReadBuffer(xlrec->target.node, parent, false);
		if (BufferIsValid(buffer))
		{
			page = (Page) BufferGetPage(buffer);
			pageop = (IDBTPageOpaque) PageGetSpecialPointer(page);
			if (lsn <= PageGetLSN(page))
			{
				UnlockReleaseBuffer(buffer);
			}
			else
			{
				OffsetNumber poffset;

				poffset = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
				if (poffset >= PageGetMaxOffsetNumber(page))
				{
					Assert(info == XLOG_IDIST_DELETE_PAGE_HALF);
					Assert(poffset == IDP_FIRSTDATAKEY(pageop));
					PageIndexTupleDelete(page, poffset);
					pageop->btpo_flags |= IDBTP_HALF_DEAD;
				}
				else
				{
					ItemId		itemid;
					IndexTuple	itup;
					OffsetNumber nextoffset;

					Assert(info != XLOG_IDIST_DELETE_PAGE_HALF);
					itemid = PageGetItemId(page, poffset);
					itup = (IndexTuple) PageGetItem(page, itemid);
					ItemPointerSet(&(itup->t_tid), rightsib, IDP_HIKEY);
					nextoffset = OffsetNumberNext(poffset);
					PageIndexTupleDelete(page, nextoffset);
				}

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
				UnlockReleaseBuffer(buffer);
			}
		}
	}

	/* Fix left-link of right sibling */
	if (record->xl_info & XLR_BKP_BLOCK(1))
		(void) RestoreBackupBlock(lsn, record, 1, false, false);
	else
	{
		buffer = XLogReadBuffer(xlrec->target.node, rightsib, false);
		if (BufferIsValid(buffer))
		{
			page = (Page) BufferGetPage(buffer);
			if (lsn <= PageGetLSN(page))
			{
				UnlockReleaseBuffer(buffer);
			}
			else
			{
				pageop = (IDBTPageOpaque) PageGetSpecialPointer(page);
				pageop->btpo_prev = leftsib;

				PageSetLSN(page, lsn);
				MarkBufferDirty(buffer);
				UnlockReleaseBuffer(buffer);
			}
		}
	}

	/* Fix right-link of left sibling, if any */
	if (record->xl_info & XLR_BKP_BLOCK(2))
		(void) RestoreBackupBlock(lsn, record, 2, false, false);
	else
	{
		if (leftsib != IDP_NONE)
		{
			buffer = XLogReadBuffer(xlrec->target.node, leftsib, false);
			if (BufferIsValid(buffer))
			{
				page = (Page) BufferGetPage(buffer);
				if (lsn <= PageGetLSN(page))
				{
					UnlockReleaseBuffer(buffer);
				}
				else
				{
					pageop = (IDBTPageOpaque) PageGetSpecialPointer(page);
					pageop->btpo_next = rightsib;

					PageSetLSN(page, lsn);
					MarkBufferDirty(buffer);
					UnlockReleaseBuffer(buffer);
				}
			}
		}
	}

	/* Rewrite target page as empty deleted page */
	buffer = XLogReadBuffer(xlrec->target.node, target, true);
	Assert(BufferIsValid(buffer));
	page = (Page) BufferGetPage(buffer);

	_idbt_pageinit(page, BufferGetPageSize(buffer));
	pageop = (IDBTPageOpaque) PageGetSpecialPointer(page);

	pageop->btpo_prev = leftsib;
	pageop->btpo_next = rightsib;
	pageop->btpo.xact = xlrec->btpo_xact;
	pageop->btpo_flags = IDBTP_DELETED;
	pageop->btpo_cycleid = 0;

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	/* Update metapage if needed */
	if (info == XLOG_IDIST_DELETE_PAGE_META)
	{
		xl_idist_metadata md;

		memcpy(&md, (char *) xlrec + SizeOfIDBtreeDeletePage,
			   sizeof(xl_idist_metadata));
		_idbt_restore_meta(xlrec->target.node, lsn,
						 md.root, md.level,
						 md.fastroot, md.fastlevel);
	}

	/* Forget any completed deletion */
	id_forget_matching_deletion(xlrec->target.node, target);

	/* If parent became half-dead, remember it for deletion */
	if (info == XLOG_IDIST_DELETE_PAGE_HALF)
		id_log_incomplete_deletion(xlrec->target.node, parent);
}

static void
idist_xlog_newroot(XLogRecPtr lsn, XLogRecord *record)
{
	xl_idist_newroot *xlrec = (xl_idist_newroot *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	IDBTPageOpaque pageop;
	BlockNumber downlink = 0;

	/* Backup blocks are not used in newroot records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	buffer = XLogReadBuffer(xlrec->node, xlrec->rootblk, true);
	Assert(BufferIsValid(buffer));
	page = (Page) BufferGetPage(buffer);

	_idbt_pageinit(page, BufferGetPageSize(buffer));
	pageop = (IDBTPageOpaque) PageGetSpecialPointer(page);

	pageop->btpo_flags = IDBTP_ROOT;
	pageop->btpo_prev = pageop->btpo_next = IDP_NONE;
	pageop->btpo.level = xlrec->level;
	if (xlrec->level == 0)
		pageop->btpo_flags |= IDBTP_LEAF;
	pageop->btpo_cycleid = 0;

	if (record->xl_len > SizeOfIDBtreeNewroot)
	{
		IndexTuple	itup;

		_idbt_restore_page(page,
						 (char *) xlrec + SizeOfIDBtreeNewroot,
						 record->xl_len - SizeOfIDBtreeNewroot);
		/* extract downlink to the right-hand split page */
		itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, IDP_FIRSTKEY));
		downlink = ItemPointerGetBlockNumber(&(itup->t_tid));
		Assert(ItemPointerGetOffsetNumber(&(itup->t_tid)) == IDP_HIKEY);
	}

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	_idbt_restore_meta(xlrec->node, lsn,
					 xlrec->rootblk, xlrec->level,
					 xlrec->rootblk, xlrec->level);

	/* Check to see if this satisfies any incomplete insertions */
	if (record->xl_len > SizeOfIDBtreeNewroot)
		id_forget_matching_split(xlrec->node, downlink, true);
}

static void
idist_xlog_reuse_page(XLogRecPtr lsn, XLogRecord *record)
{
	xl_idist_reuse_page *xlrec = (xl_idist_reuse_page *) XLogRecGetData(record);

	/*
	 * Btree reuse_page records exist to provide a conflict point when we
	 * reuse pages in the index via the FSM.  That's all they do though.
	 *
	 * latestRemovedXid was the page's btpo.xact.  The btpo.xact <
	 * RecentGlobalXmin test in _idbt_page_recyclable() conceptually mirrors the
	 * pgxact->xmin > limitXmin test in GetConflictingVirtualXIDs().
	 * Consequently, one XID value achieves the same exclusion effect on
	 * master and standby.
	 */
	if (InHotStandby)
	{
		ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid,
											xlrec->node);
	}

	/* Backup blocks are not used in reuse_page records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));
}


void
idist_redo(XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_IDIST_INSERT_LEAF:
			idist_xlog_insert(true, false, lsn, record);
			break;
		case XLOG_IDIST_INSERT_UPPER:
			idist_xlog_insert(false, false, lsn, record);
			break;
		case XLOG_IDIST_INSERT_META:
			idist_xlog_insert(false, true, lsn, record);
			break;
		case XLOG_IDIST_SPLIT_L:
			idist_xlog_split(true, false, lsn, record);
			break;
		case XLOG_IDIST_SPLIT_R:
			idist_xlog_split(false, false, lsn, record);
			break;
		case XLOG_IDIST_SPLIT_L_ROOT:
			idist_xlog_split(true, true, lsn, record);
			break;
		case XLOG_IDIST_SPLIT_R_ROOT:
			idist_xlog_split(false, true, lsn, record);
			break;
		case XLOG_IDIST_VACUUM:
			idist_xlog_vacuum(lsn, record);
			break;
		case XLOG_IDIST_DELETE:
			idist_xlog_delete(lsn, record);
			break;
		case XLOG_IDIST_DELETE_PAGE:
		case XLOG_IDIST_DELETE_PAGE_META:
		case XLOG_IDIST_DELETE_PAGE_HALF:
			idist_xlog_delete_page(info, lsn, record);
			break;
		case XLOG_IDIST_NEWROOT:
			idist_xlog_newroot(lsn, record);
			break;
		case XLOG_IDIST_REUSE_PAGE:
			idist_xlog_reuse_page(lsn, record);
			break;
		default:
			elog(PANIC, "idist_redo: unknown op code %u", info);
	}
}

void
idist_xlog_startup(void)
{
	incomplete_actions = NIL;
}

void
idist_xlog_cleanup(void)
{
	ListCell   *l;

	foreach(l, incomplete_actions)
	{
		idbt_incomplete_action *action = (idbt_incomplete_action *) lfirst(l);

		if (action->is_split)
		{
			/* finish an incomplete split */
			Buffer		lbuf,
						rbuf;
			Page		lpage,
						rpage;
			IDBTPageOpaque lpageop,
						rpageop;
			bool		is_only;
			Relation	reln;

			lbuf = XLogReadBuffer(action->node, action->leftblk, false);
			/* failure is impossible because we wrote this page earlier */
			if (!BufferIsValid(lbuf))
				elog(PANIC, "idist_xlog_cleanup: left block unfound");
			lpage = (Page) BufferGetPage(lbuf);
			lpageop = (IDBTPageOpaque) PageGetSpecialPointer(lpage);
			rbuf = XLogReadBuffer(action->node, action->rightblk, false);
			/* failure is impossible because we wrote this page earlier */
			if (!BufferIsValid(rbuf))
				elog(PANIC, "idist_xlog_cleanup: right block unfound");
			rpage = (Page) BufferGetPage(rbuf);
			rpageop = (IDBTPageOpaque) PageGetSpecialPointer(rpage);

			/* if the pages are all of their level, it's a only-page split */
			is_only = IDP_LEFTMOST(lpageop) && IDP_RIGHTMOST(rpageop);

			reln = CreateFakeRelcacheEntry(action->node);
			_idbt_insert_parent(reln, lbuf, rbuf, NULL,
							  action->is_root, is_only);
			FreeFakeRelcacheEntry(reln);
		}
		else
		{
			/* finish an incomplete deletion (of a half-dead page) */
			Buffer		buf;

			buf = XLogReadBuffer(action->node, action->delblk, false);
			if (BufferIsValid(buf))
			{
				Relation	reln;

				reln = CreateFakeRelcacheEntry(action->node);
				if (_idbt_pagedel(reln, buf, NULL) == 0)
					elog(PANIC, "idist_xlog_cleanup: _idbt_pagedel failed");
				FreeFakeRelcacheEntry(reln);
			}
		}
	}
	incomplete_actions = NIL;
}

bool
idist_safe_restartpoint(void)
{
	if (incomplete_actions)
		return false;
	return true;
}
