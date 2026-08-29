#include "postgres.h"

#include "storage/md.h"
#include "storage/rd.h"
#include "miscadmin.h"
#include "utils/memutils.h"

typedef struct _RdBuffer
{
	dlist_node            node;
    RelFileLocatorBackend rlocator;
    ForkNumber            forknum;
    BlockNumber           ballocated;
    BlockNumber           bsize;
    char                 *data;
} _RdBuffer;

/*
 * Size of buffer in blocks. After the buffer is exhausted the storage switches
 * to 'md' and flushed all the data to disk.
 */
int temp_rd_buffers = 4;

bool enable_temp_rd_buffers = false;

static MemoryContext mctx;
static dlist_head    buffers;


static _RdBuffer*
_find_buffer(RelFileLocatorBackend* rlocator, ForkNumber forknum)
{
	dlist_iter iter;
	dlist_foreach(iter, &buffers)
	{
		_RdBuffer* buffer = dlist_container(_RdBuffer, node, iter.cur);
		if (RelFileLocatorBackendEquals(buffer->rlocator, *rlocator) && buffer->forknum == forknum)
			return buffer;
	}
	return NULL;
}


static _RdBuffer*
_open_buffer(SMgrRelation reln, ForkNumber forknum)
{
	_RdBuffer* tbuf = reln->rd_bufs[forknum] ;
	if (tbuf)
		return tbuf;

	tbuf = _find_buffer(&reln->smgr_rlocator, forknum);
	if (tbuf)
		return tbuf;

	ereport(ERROR,
		(errcode_for_file_access(),
		errmsg("temporary page doesn't exists")));

	return NULL;
}


static void
switch_to_md(SMgrRelation reln)
{
	dlist_mutable_iter iter;

	for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		reln->rd_bufs[forknum] = 0;
		
	reln->smgr_which = 0;

	mdopen(reln);

	dlist_foreach_modify(iter, &buffers)
	{
		_RdBuffer* buffer = dlist_container(_RdBuffer, node, iter.cur);
		if (RelFileLocatorBackendEquals(buffer->rlocator, reln->smgr_rlocator))
		{
			smgrcreate(reln, buffer->forknum, false);
			for(BlockNumber bn=0; bn < buffer->bsize; bn++)
				smgrextend(reln, buffer->forknum, bn, buffer->data + bn*BLCKSZ, true);

			dlist_delete(&buffer->node);
			pfree(buffer->data);
			pfree(buffer);
		}
	}
}


void
rd_reset(SMgrRelation reln)
{
	BlockNumber nblocks[MAX_FORKNUM+1];
	char* buf;

	if (reln->smgr_which == 1)
		return;

	for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
	{
		if (smgrexists(reln, forknum))
		{
			nblocks[forknum] = smgrnblocks(reln, forknum);

			if (nblocks[forknum] > temp_rd_buffers)
				return;
		}
		else
			nblocks[forknum] = InvalidBlockNumber;
	}


	buf = palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE, 0);

	for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
	{
		if (nblocks[forknum] == InvalidBlockNumber)
			continue;

		rd_create(reln, forknum, false);
		for (BlockNumber bn=0; bn < nblocks[forknum]; bn++)
		{
			smgrread(reln, forknum, bn, buf);
			rd_extend(reln, forknum, bn, buf, true);
		}
	}

	pfree(buf);

	smgrdounlinkall(&reln, 1, false);

	reln->smgr_which = 1;
}


void
rd_init(void)
{
	mctx = AllocSetContextCreate(TopMemoryContext, "RdSmgr", ALLOCSET_DEFAULT_SIZES);
	dlist_init(&buffers);
}


void
rd_shutdown(void)
{
}


void
rd_open(SMgrRelation reln)
{
	mdopen(reln);

    for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
        reln->rd_bufs[forknum] = 0;
}


void
rd_close(SMgrRelation reln, ForkNumber forknum)
{
    (void) reln;
    (void) forknum;
}


void
rd_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	_RdBuffer* tbuf = _find_buffer(&reln->smgr_rlocator, forknum);
	if (tbuf)
		ereport(ERROR,
			(errcode_for_file_access(),
			errmsg("temporary page already exists")));

	tbuf = MemoryContextAlloc(mctx, sizeof(_RdBuffer));
	tbuf->rlocator   = reln->smgr_rlocator;
	tbuf->forknum    = forknum;
	tbuf->ballocated = temp_rd_buffers;
	tbuf->bsize      = 0;
	tbuf->data       = MemoryContextAllocAligned(mctx, tbuf->ballocated*BLCKSZ, PG_IO_ALIGN_SIZE, 0);

	dlist_push_tail(&buffers, &tbuf->node);

	reln->rd_bufs[forknum] = tbuf;
}


bool
rd_exists(SMgrRelation reln, ForkNumber forknum)
{
	return _find_buffer(&reln->smgr_rlocator, forknum);
}


void
rd_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	_RdBuffer* tbuf = _find_buffer(&rlocator, forknum);
	if (tbuf)
	{
		dlist_delete(&tbuf->node);
		pfree(tbuf->data);
		pfree(tbuf);
	}
}


void
rd_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	_RdBuffer* tbuf = _open_buffer(reln, forknum);

	while (nblocks)
	{
		if (blocknum >= tbuf->bsize)
			ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("temporary page write beyond size")));

		if (blocknum >= tbuf->ballocated)
		{
			switch_to_md(reln);
			smgrwritev(reln, forknum, blocknum, buffers, nblocks, skipFsync);
			return;
		}

		memcpy(tbuf->data + blocknum*BLCKSZ, *buffers, BLCKSZ);

		buffers++;
		nblocks--;
		blocknum++;
	}
}


void
rd_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void *buffer, bool skipFsync)
{
	_RdBuffer* tbuf = _open_buffer(reln, forknum);

	if (blocknum >= tbuf->ballocated)
	{
		switch_to_md(reln);
		smgrextend(reln, forknum, blocknum, buffer, skipFsync);
		return;
	}

	memcpy(tbuf->data + blocknum*BLCKSZ, buffer, BLCKSZ);

	tbuf->bsize = Max(tbuf->bsize, blocknum+1);
}


void
rd_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks, bool skipFsync)
{
	_RdBuffer* tbuf = _open_buffer(reln, forknum);

	if (blocknum + nblocks > tbuf->ballocated)
	{
		switch_to_md(reln);
		smgrzeroextend(reln, forknum, blocknum, nblocks, skipFsync);
		return;
	}

	memset(tbuf->data + blocknum*BLCKSZ, 0, BLCKSZ*nblocks);

	tbuf->bsize = Max(tbuf->bsize, blocknum+nblocks);
}


void
rd_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, void **buffers, BlockNumber nblocks)
{
	_RdBuffer* tbuf = _open_buffer(reln, forknum);

	while (nblocks)
	{
		if (blocknum >= tbuf->bsize)
			ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not read block %u in rd of size %u", blocknum, tbuf->bsize)));

		memcpy(*buffers, tbuf->data + blocknum*BLCKSZ, BLCKSZ);

		buffers++;
		nblocks--;
		blocknum++;
	}
}


BlockNumber
rd_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	_RdBuffer* tbuf = _open_buffer(reln, forknum);
	return tbuf->bsize;
}


void
rd_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks, BlockNumber nblocks)
{
	_RdBuffer* tbuf = _open_buffer(reln, forknum);
	tbuf->bsize = nblocks;
	(void) old_blocks;
}


bool
rd_prefetch(SMgrRelation reln,
            ForkNumber forknum,
            BlockNumber blocknum,
            int nblocks)
{
	(void) reln;
	(void) forknum;
	(void) blocknum;
	(void) nblocks;
	return true;
}


void
rd_writeback(SMgrRelation reln,
                   ForkNumber forknum,
                   BlockNumber blocknum,
                   BlockNumber nblocks)
{
	(void) reln;
	(void) forknum;
	(void) blocknum;
	(void) nblocks;
}


void
rd_immedsync(SMgrRelation reln,
                   ForkNumber forknum)
{
	(void) reln;
	(void) forknum;
}


void
rd_registersync(SMgrRelation reln, ForkNumber forknum)
{
	(void) reln;
	(void) forknum;
}


int
rd_fd(SMgrRelation reln, ForkNumber forknum,
	BlockNumber blocknum, uint32 *off)
{
	return -1;
}
