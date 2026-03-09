#ifndef RD_H
#define RD_H

#include "postgres.h"

#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "storage/sync.h"

extern void rd_init(void);
extern void rd_shutdown(void);
extern void rd_open(SMgrRelation reln);
extern void rd_close(SMgrRelation reln, ForkNumber forknum);
extern void rd_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool rd_exists(SMgrRelation reln, ForkNumber forknum);
extern void rd_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo);
extern void rd_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void *buffer, bool skipFsync);
extern void rd_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks, bool skipFsync);
extern bool rd_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks);
extern void rd_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, void **buffers, BlockNumber nblocks);
extern void rd_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void **buffers, BlockNumber nblocks, bool skipFsync);
extern void rd_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber rd_nblocks(SMgrRelation reln, ForkNumber forknum);
extern void rd_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks, BlockNumber nblocks);
extern void rd_immedsync(SMgrRelation reln, ForkNumber forknum);
extern void rd_registersync(SMgrRelation reln, ForkNumber forknum);
extern void rd_reset(SMgrRelation reln);
extern int	rd_fd(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, uint32 *off);

extern int temp_rd_buffers;
extern bool enable_temp_rd_buffers;

#endif /* RD_H */
