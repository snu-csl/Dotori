/*
 * NVM Express device driver
 * Copyright (c) 2011-2014, Intel Corporation.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 */

#include <linux/bio.h>
#include <linux/bitops.h>
#include <linux/blkdev.h>
#include <linux/cpu.h>
#include <linux/delay.h>
#include <linux/errno.h>
#include <linux/fs.h>
#include <linux/genhd.h>
#include <linux/hdreg.h>
#include <linux/idr.h>
#include <linux/init.h>
#include <linux/interrupt.h>
#include <linux/io.h>
#include <linux/kdev_t.h>
#include <linux/kthread.h>
#include <linux/kernel.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/moduleparam.h>
#include <linux/pci.h>
#include <linux/percpu.h>
#include <linux/poison.h>
#include <linux/ptrace.h>
#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/types.h>
#include <scsi/sg.h>
#include <asm-generic/io-64-nonatomic-lo-hi.h>

#include <trace/events/block.h>
#include <linux/file.h>
#include <linux/fdtable.h>
#include <linux/eventfd.h>
#include <linux/kref.h>
#include "nvme.h"

#define NVME_Q_DEPTH		1024
#define SQ_SIZE(depth)		(depth * sizeof(struct nvme_command))
#define CQ_SIZE(depth)		(depth * sizeof(struct nvme_completion))
#define ADMIN_TIMEOUT		(admin_timeout * HZ)
#define IOD_TIMEOUT		(retry_time * HZ)

static unsigned char admin_timeout = 60;
module_param(admin_timeout, byte, 0644);
MODULE_PARM_DESC(admin_timeout, "timeout in seconds for admin commands");

unsigned char nvme_io_timeout = 30;
module_param_named(io_timeout, nvme_io_timeout, byte, 0644);
MODULE_PARM_DESC(io_timeout, "timeout in seconds for I/O");

static unsigned char retry_time = 30;
module_param(retry_time, byte, 0644);
MODULE_PARM_DESC(retry_time, "time in seconds to retry failed I/O");

static int nvme_major;
module_param(nvme_major, int, 0);

static int use_threaded_interrupts;
module_param(use_threaded_interrupts, int, 0);

static DEFINE_SPINLOCK(dev_list_lock);
static LIST_HEAD(dev_list);
static struct task_struct *nvme_thread;
static struct workqueue_struct *nvme_workq;
static wait_queue_head_t nvme_kthread_wait;
static struct notifier_block nvme_nb;

static void nvme_reset_failed_dev(struct work_struct *ws);

struct async_cmd_info {
	struct kthread_work work;
	struct kthread_worker *worker;
	u32 result;
	int status;
	void *ctx;
};

/*
 * PORT AIO Support logic from Kvepic.
 */
// AIO data structures
static struct kmem_cache        *kaioctx_cachep = 0;
static struct kmem_cache        *kaiocb_cachep = 0;
static mempool_t *kaiocb_mempool = 0;
static mempool_t *kaioctx_mempool = 0;

static __u32 aio_context_id;

#define AIOCTX_MAX 1024
#define AIOCB_MAX (1024*64)

static __u64 debug_completed  =0;
static int debug_outstanding  =0;

struct nvme_kaioctx
{
	struct nvme_aioctx uctx;
	struct eventfd_ctx *eventctx;
	struct list_head kaiocb_list;
	spinlock_t kaioctx_spinlock;
	struct kref ref;
};

static struct nvme_kaioctx **g_kaioctx_tb = NULL;
static spinlock_t g_kaioctx_tb_spinlock;

struct aio_user_ctx {
	int nents;
	int len;
	struct page ** pages;
	struct scatterlist *sg;
	char data[1];
};

struct nvme_kaiocb
{
	struct list_head aiocb_list;
	struct nvme_aioevent event;
	struct nvme_completion cqe;
	int opcode;
	bool use_meta;
	struct nvme_iod *iod;
	struct scatterlist meta_sg;
	void *meta;
	char *kv_data;
	struct aio_user_ctx *user_ctx;
};

static void remove_kaioctx(struct nvme_kaioctx * ctx)
{
	struct nvme_kaiocb *tmpcb;
	struct list_head *pos, *q;
	unsigned long flags;
	if (ctx) {
		spin_lock_irqsave(&ctx->kaioctx_spinlock, flags);
		list_for_each_safe(pos, q, &ctx->kaiocb_list){
			tmpcb = list_entry(pos, struct nvme_kaiocb, aiocb_list);
			list_del(pos);
			mempool_free(tmpcb, kaiocb_mempool);
		}
		spin_unlock_irqrestore(&ctx->kaioctx_spinlock, flags);
		eventfd_ctx_put(ctx->eventctx);	
		mempool_free(ctx, kaioctx_mempool);
	}
}

static void cleanup_kaioctx(struct kref *kref) {
	struct nvme_kaioctx *ctx = container_of(kref, struct nvme_kaioctx, ref);
	remove_kaioctx(ctx);
}

static void ref_kaioctx(struct nvme_kaioctx *ctx) {
	kref_get(&ctx->ref);
}

static void deref_kaioctx(struct nvme_kaioctx *ctx) {
	kref_put(&ctx->ref, cleanup_kaioctx);
}

/* destroy memory pools
*/
static void destroy_aio_mempool(void)
{
	int i = 0;
	if (g_kaioctx_tb) {
		for (i = 0; i < AIOCTX_MAX; i++) {
			if (g_kaioctx_tb[i]) {
				remove_kaioctx(g_kaioctx_tb[i]);
				g_kaioctx_tb[i] = NULL;
			}
		}
		kfree(g_kaioctx_tb);
		g_kaioctx_tb = NULL;
	}
	if (kaiocb_mempool)
		mempool_destroy(kaiocb_mempool);
	if (kaioctx_mempool)
		mempool_destroy(kaioctx_mempool);
	if (kaioctx_cachep)
		kmem_cache_destroy(kaioctx_cachep);
	if (kaiocb_cachep)
		kmem_cache_destroy(kaiocb_cachep);
}

/* prepare basic data structures
 * to support aio context and requests
 */
static int aio_service_init(void)
{
	g_kaioctx_tb = (struct nvme_kaioctx **)kmalloc(sizeof(struct nvme_kaioctx *) * AIOCTX_MAX, GFP_KERNEL);
	if (!g_kaioctx_tb)
		goto fail;
	memset(g_kaioctx_tb, 0, sizeof(struct nvme_kaioctx *) * AIOCTX_MAX);

	// slap allocator and memory pool
	kaioctx_cachep = kmem_cache_create("nvme_kaioctx", sizeof(struct nvme_kaioctx), 0, 0, NULL);
	if (!kaioctx_cachep)
		goto fail;

	kaiocb_cachep = kmem_cache_create("nvme_kaiocb", sizeof(struct nvme_kaiocb), 0, 0, NULL);
	if (!kaiocb_cachep)
		goto fail;

	kaiocb_mempool = mempool_create_slab_pool(AIOCB_MAX, kaiocb_cachep);
	if (!kaiocb_mempool)
		goto fail;

	kaioctx_mempool = mempool_create_slab_pool(AIOCTX_MAX, kaioctx_cachep);
	if (!kaioctx_mempool)
		goto fail;

	// global variables
	// context id 0 is reserved for normal I/O operations (synchronous)
	aio_context_id = 1;
	spin_lock_init(&g_kaioctx_tb_spinlock);
	printk(KERN_DEBUG"nvme-aio: initialized\n");
	return 0;

fail:
	destroy_aio_mempool();
	return -ENOMEM;
}

/*
 * release memory before exit
 */
static int aio_service_exit(void)
{
	destroy_aio_mempool();
	printk(KERN_DEBUG"nvme-aio: unloaded\n");
	return 0;
}

static struct nvme_kaioctx * find_kaioctx(__u32 ctxid) {
	struct nvme_kaioctx * tmp = NULL;
	//unsigned long flags;
	//spin_lock_irqsave(&g_kaioctx_tb_spinlock, flags);
	tmp = g_kaioctx_tb[ctxid];
	if (tmp) ref_kaioctx(tmp);
	//spin_unlock_irqrestore(&g_kaioctx_tb_spinlock, flags);
	return tmp;
}


/*
 * find an aio context with a given id
 */
static int  set_aioctx_event(__u32 ctxid, struct nvme_kaiocb * kaiocb)
{
	struct nvme_kaioctx *tmp;
	unsigned long flags;
	tmp = find_kaioctx(ctxid);
	if (tmp) {
		spin_lock_irqsave(&tmp->kaioctx_spinlock, flags);
		list_add_tail(&kaiocb->aiocb_list, &tmp->kaiocb_list);
		eventfd_signal(tmp->eventctx, 1);
#if 0
		printk("nvme_set_aioctx: success to signal event %d %llu\n", kaiocb->event.ctxid, kaiocb->event.reqid);
#endif
		spin_unlock_irqrestore(&tmp->kaioctx_spinlock, flags);
		deref_kaioctx(tmp);
		return 0;
	} else {
#if 0
		printk("nvme_set_aioctx: failed to signal event %d.\n", ctxid);
#endif
	}

	return -1;
}

/*
 * delete an aio context
 * it will release any resources allocated for this context
 */
static int nvme_del_aioctx(struct nvme_aioctx __user *uctx )
{
	struct nvme_kaioctx ctx;
	unsigned long flags;
	if (copy_from_user(&ctx, uctx, sizeof(struct nvme_aioctx)))
		return -EFAULT;

	spin_lock_irqsave(&g_kaioctx_tb_spinlock, flags);
	if (g_kaioctx_tb[ctx.uctx.ctxid]) {
		deref_kaioctx(g_kaioctx_tb[ctx.uctx.ctxid]);
		g_kaioctx_tb[ctx.uctx.ctxid] = NULL;
	}
	spin_unlock_irqrestore(&g_kaioctx_tb_spinlock, flags);
	return 0;
}

/*
 * set up an aio context
 * allocate a new context with given parameters and prepare a eventfd_context
 */
static int nvme_set_aioctx(struct nvme_aioctx __user *uctx )
{
	struct nvme_kaioctx *ctx;
	struct fd efile;
	struct eventfd_ctx *eventfd_ctx = NULL;
	unsigned long flags;
	int ret = 0;
	int i = 0;
	if (!capable(CAP_SYS_ADMIN))
		return -EACCES;

	ctx = mempool_alloc(kaioctx_mempool, GFP_NOIO); //ctx = kmem_cache_zalloc(kaioctx_cachep, GFP_KERNEL);
	if (!ctx)
		return -ENOMEM;

	if (copy_from_user(ctx, uctx, sizeof(struct nvme_aioctx)))
		return -EFAULT;

	efile = fdget(ctx->uctx.eventfd);
	if (!efile.file) {
		printk("nvme_set_aioctx: failed to get efile for efd %d.\n", ctx->uctx.eventfd);
		ret = -EBADF;
		goto exit;
	}

	eventfd_ctx = eventfd_ctx_fileget(efile.file);
	if (IS_ERR(eventfd_ctx)) {
		printk("nvme_set_aioctx: failed to get eventfd_ctx for efd %d.\n", ctx->uctx.eventfd);
		ret = PTR_ERR(eventfd_ctx);
		goto put_efile;
	}
	// set context id
	spin_lock_irqsave(&g_kaioctx_tb_spinlock, flags);
	if(g_kaioctx_tb[aio_context_id]) {
		for(i = 0; i < AIOCTX_MAX; i++) {
			if(g_kaioctx_tb[i] == NULL) {
				aio_context_id = i;
				break;
			}
		}
		if (i >= AIOCTX_MAX) {
			spin_unlock_irqrestore(&g_kaioctx_tb_spinlock, flags);
			printk("nvme_set_aioctx: too many aioctx open.\n");
			ret = -EMFILE;
			goto put_event_fd;
		}
	}
	ctx->uctx.ctxid = aio_context_id++;
	if (aio_context_id == AIOCTX_MAX)
		aio_context_id = 0;
	ctx->eventctx = eventfd_ctx;
	spin_lock_init(&ctx->kaioctx_spinlock);
	INIT_LIST_HEAD(&ctx->kaiocb_list);
	kref_init(&ctx->ref);
	g_kaioctx_tb[ctx->uctx.ctxid] = ctx;
	spin_unlock_irqrestore(&g_kaioctx_tb_spinlock, flags);

	if (copy_to_user(&uctx->ctxid, &ctx->uctx.ctxid, sizeof(ctx->uctx.ctxid)))
	{
		printk("nvme_set_aioctx: failed to copy context id %d to user.\n", ctx->uctx.ctxid);
		ret =  -EINVAL;
		goto cleanup;
	}
	eventfd_ctx = NULL;
	debug_outstanding = 0;
	debug_completed = 0;
	fdput(efile);
	return 0;
cleanup:
	spin_lock_irqsave(&g_kaioctx_tb_spinlock, flags);
	g_kaioctx_tb[ctx->uctx.ctxid - 1] = NULL;
	spin_unlock_irqrestore(&g_kaioctx_tb_spinlock, flags);
	mempool_free(ctx, kaiocb_mempool);
put_event_fd:
	eventfd_ctx_put(eventfd_ctx);	
put_efile:
	fdput(efile);
exit:
	return ret;
}

/* get an aiocb, which represents a single I/O request.
*/
static struct nvme_kaiocb *get_aiocb(__u64 reqid) {
	struct nvme_kaiocb *req;
	req = mempool_alloc(kaiocb_mempool, GFP_NOIO); //ctx = kmem_cache_zalloc(kaioctx_cachep, GFP_KERNEL);
	if (!req) return 0;

	memset(req, 0, sizeof(*req));

	INIT_LIST_HEAD(&req->aiocb_list);

	req->event.reqid = reqid;

	return req;
}

/* returns the completed events to users
*/
static int nvme_get_aioevents(struct nvme_aioevents __user *uevents)
{
	struct list_head *pos, *q;
	struct nvme_kaiocb *tmp;
	struct nvme_kaioctx *tmp_ctx;
	unsigned long flags;
	LIST_HEAD(tmp_head);
	__u16 count =0;
	__u16 nr = 0;
	__u32 ctxid = 0;

	if (!capable(CAP_SYS_ADMIN))
		return -EACCES;

	if (get_user(nr, &uevents->nr) < 0) { 	return -EINVAL;    }

	if (nr == 0 || nr > 128) return -EINVAL;

	if (get_user(ctxid, &uevents->ctxid) < 0) { return -EINVAL; }

	tmp_ctx = find_kaioctx(ctxid);
	if (tmp_ctx) {
		spin_lock_irqsave(&tmp_ctx->kaioctx_spinlock, flags);
		list_for_each_safe(pos, q, &tmp_ctx->kaiocb_list){
			list_del_init(pos);
			list_add(pos, &tmp_head);
			count++;
			if (nr == count) break;
		}
		spin_unlock_irqrestore(&tmp_ctx->kaioctx_spinlock, flags);
		deref_kaioctx(tmp_ctx);
		count = 0; 
		list_for_each_safe(pos, q, &tmp_head){
			list_del(pos);
			tmp = list_entry(pos, struct nvme_kaiocb, aiocb_list);
			copy_to_user(&uevents->events[count], &tmp->event, sizeof(struct nvme_aioevent));
			mempool_free(tmp, kaiocb_mempool);
			count++;
		}
	}
	if (put_user(count, &uevents->nr)  < 0) { return -EINVAL; }

	return 0;
}

/*
 * An NVM Express queue.  Each device has at least two (one for admin
 * commands and one for I/O commands).
 */
struct nvme_queue {
	struct rcu_head r_head;
	struct device *q_dmadev;
	struct nvme_dev *dev;
	char irqname[24];	/* nvme4294967295-65535\0 */
	spinlock_t q_lock;
	struct nvme_command *sq_cmds;
	volatile struct nvme_completion *cqes;
	dma_addr_t sq_dma_addr;
	dma_addr_t cq_dma_addr;
	wait_queue_head_t sq_full;
	wait_queue_t sq_cong_wait;
	struct bio_list sq_cong;
	struct list_head iod_bio;
	u32 __iomem *q_db;
	u16 q_depth;
	u16 cq_vector;
	u16 sq_head;
	u16 sq_tail;
	u16 cq_head;
	u16 qid;
	u8 cq_phase;
	u8 cqe_seen;
	u8 q_suspended;
	cpumask_var_t cpu_mask;
	struct async_cmd_info cmdinfo;
	unsigned long cmdid_data[];
};

static void kv_async_completion(struct nvme_queue *nvmeq, void *ctx, struct nvme_completion *cqe){
	struct nvme_dev* dev = nvmeq->dev;
	struct nvme_kaiocb *cmdinfo = (struct nvme_kaiocb *)ctx;
	cmdinfo->event.result = le32_to_cpu(cqe->result);
	cmdinfo->event.status = le16_to_cpu(cqe->status) >> 1;
	if (cmdinfo->kv_data && cmdinfo->user_ctx) {
	if (is_kv_retrieve_cmd(cmdinfo->opcode) || is_kv_iter_read_cmd(cmdinfo->opcode)) {
		(void)sg_copy_from_buffer(cmdinfo->user_ctx->sg, cmdinfo->user_ctx->nents,
				cmdinfo->kv_data, cmdinfo->user_ctx->len);
			}
	}

	if (cmdinfo->iod) {
		nvme_unmap_user_pages(dev, kv_nvme_is_write(cmdinfo->opcode), cmdinfo->iod);
		nvme_free_iod(dev, cmdinfo->iod);
	}

	if (cmdinfo->user_ctx) { 
		int i = 0;
		for (i = 0; i < cmdinfo->user_ctx->nents; i++)
			put_page(sg_page(&cmdinfo->user_ctx->sg[i]));
		kfree(cmdinfo->user_ctx);
	}
	if (cmdinfo->kv_data) kfree(cmdinfo->kv_data);

	if (cmdinfo->use_meta) {
		dma_unmap_sg(&dev->pci_dev->dev, &cmdinfo->meta_sg, 1, DMA_TO_DEVICE);
		put_page(sg_page(&cmdinfo->meta_sg));
		if (cmdinfo->meta) {
			kfree(cmdinfo->meta);
		}
	}

	if (set_aioctx_event(cmdinfo->event.ctxid, cmdinfo)) {
		mempool_free(cmdinfo, kaiocb_mempool);
	}
}

/*
 * Check we didin't inadvertently grow the command struct
 */
static inline void _nvme_check_size(void)
{
	BUILD_BUG_ON(sizeof(struct nvme_rw_command) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_create_cq) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_create_sq) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_delete_queue) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_features) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_format_cmd) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_abort_cmd) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_command) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_id_ctrl) != 4096);
	BUILD_BUG_ON(sizeof(struct nvme_id_ns) != 4096);
	BUILD_BUG_ON(sizeof(struct nvme_lba_range_type) != 64);
	BUILD_BUG_ON(sizeof(struct nvme_smart_log) != 512);
}

typedef void (*nvme_completion_fn)(struct nvme_queue *, void *,
						struct nvme_completion *);

struct nvme_cmd_info {
	nvme_completion_fn fn;
	void *ctx;
	unsigned long timeout;
	int aborted;
};

static struct nvme_cmd_info *nvme_cmd_info(struct nvme_queue *nvmeq)
{
	return (void *)&nvmeq->cmdid_data[BITS_TO_LONGS(nvmeq->q_depth)];
}

static unsigned nvme_queue_extra(int depth)
{
	return DIV_ROUND_UP(depth, 8) + (depth * sizeof(struct nvme_cmd_info));
}

/**
 * alloc_cmdid() - Allocate a Command ID
 * @nvmeq: The queue that will be used for this command
 * @ctx: A pointer that will be passed to the handler
 * @handler: The function to call on completion
 *
 * Allocate a Command ID for a queue.  The data passed in will
 * be passed to the completion handler.  This is implemented by using
 * the bottom two bits of the ctx pointer to store the handler ID.
 * Passing in a pointer that's not 4-byte aligned will cause a BUG.
 * We can change this if it becomes a problem.
 *
 * May be called with local interrupts disabled and the q_lock held,
 * or with interrupts enabled and no locks held.
 */
static int alloc_cmdid(struct nvme_queue *nvmeq, void *ctx,
				nvme_completion_fn handler, unsigned timeout)
{
	int depth = nvmeq->q_depth - 1;
	struct nvme_cmd_info *info = nvme_cmd_info(nvmeq);
	int cmdid;

	do {
		cmdid = find_first_zero_bit(nvmeq->cmdid_data, depth);
		if (cmdid >= depth)
			return -EBUSY;
	} while (test_and_set_bit(cmdid, nvmeq->cmdid_data));

	info[cmdid].fn = handler;
	info[cmdid].ctx = ctx;
	info[cmdid].timeout = jiffies + timeout;
	info[cmdid].aborted = 0;
	return cmdid;
}

static int alloc_cmdid_killable(struct nvme_queue *nvmeq, void *ctx,
				nvme_completion_fn handler, unsigned timeout)
{
	int cmdid;
	wait_event_killable(nvmeq->sq_full,
		(cmdid = alloc_cmdid(nvmeq, ctx, handler, timeout)) >= 0);
	return (cmdid < 0) ? -EINTR : cmdid;
}

/* Special values must be less than 0x1000 */
#define CMD_CTX_BASE		((void *)POISON_POINTER_DELTA)
#define CMD_CTX_CANCELLED	(0x30C + CMD_CTX_BASE)
#define CMD_CTX_COMPLETED	(0x310 + CMD_CTX_BASE)
#define CMD_CTX_INVALID		(0x314 + CMD_CTX_BASE)
#define CMD_CTX_ABORT		(0x318 + CMD_CTX_BASE)

static void special_completion(struct nvme_queue *nvmeq, void *ctx,
						struct nvme_completion *cqe)
{
	if (ctx == CMD_CTX_CANCELLED)
		return;
	if (ctx == CMD_CTX_ABORT) {
		++nvmeq->dev->abort_limit;
		return;
	}
	if (ctx == CMD_CTX_COMPLETED) {
		dev_warn(nvmeq->q_dmadev,
				"completed id %d twice on queue %d\n",
				cqe->command_id, le16_to_cpup(&cqe->sq_id));
		return;
	}
	if (ctx == CMD_CTX_INVALID) {
		dev_warn(nvmeq->q_dmadev,
				"invalid id %d completed on queue %d\n",
				cqe->command_id, le16_to_cpup(&cqe->sq_id));
		return;
	}

	dev_warn(nvmeq->q_dmadev, "Unknown special completion %p\n", ctx);
}

static void async_completion(struct nvme_queue *nvmeq, void *ctx,
						struct nvme_completion *cqe)
{
	struct async_cmd_info *cmdinfo = ctx;
	cmdinfo->result = le32_to_cpup(&cqe->result);
	cmdinfo->status = le16_to_cpup(&cqe->status) >> 1;
	queue_kthread_work(cmdinfo->worker, &cmdinfo->work);
}

/*
 * Called with local interrupts disabled and the q_lock held.  May not sleep.
 */
static void *free_cmdid(struct nvme_queue *nvmeq, int cmdid,
						nvme_completion_fn *fn)
{
	void *ctx;
	struct nvme_cmd_info *info = nvme_cmd_info(nvmeq);

	if (cmdid >= nvmeq->q_depth || !info[cmdid].fn) {
		if (fn)
			*fn = special_completion;
		return CMD_CTX_INVALID;
	}
	if (fn)
		*fn = info[cmdid].fn;
	ctx = info[cmdid].ctx;
	info[cmdid].fn = special_completion;
	info[cmdid].ctx = CMD_CTX_COMPLETED;
	clear_bit(cmdid, nvmeq->cmdid_data);
	wake_up(&nvmeq->sq_full);
	return ctx;
}

static void *cancel_cmdid(struct nvme_queue *nvmeq, int cmdid,
						nvme_completion_fn *fn)
{
	void *ctx;
	struct nvme_cmd_info *info = nvme_cmd_info(nvmeq);
	if (fn)
		*fn = info[cmdid].fn;
	ctx = info[cmdid].ctx;
	info[cmdid].fn = special_completion;
	info[cmdid].ctx = CMD_CTX_CANCELLED;
	return ctx;
}

static struct nvme_queue *raw_nvmeq(struct nvme_dev *dev, int qid)
{
	return rcu_dereference_raw(dev->queues[qid]);
}

static struct nvme_queue *get_nvmeq(struct nvme_dev *dev) __acquires(RCU)
{
	struct nvme_queue *nvmeq;
	unsigned queue_id = get_cpu_var(*dev->io_queue);

	rcu_read_lock();
	nvmeq = rcu_dereference(dev->queues[queue_id]);
	if (nvmeq)
		return nvmeq;

	rcu_read_unlock();
	put_cpu_var(*dev->io_queue);
	return NULL;
}

static void put_nvmeq(struct nvme_queue *nvmeq) __releases(RCU)
{
	rcu_read_unlock();
	put_cpu_var(nvmeq->dev->io_queue);
}

static struct nvme_queue *lock_nvmeq(struct nvme_dev *dev, int q_idx)
							__acquires(RCU)
{
	struct nvme_queue *nvmeq;

	rcu_read_lock();
	nvmeq = rcu_dereference(dev->queues[q_idx]);
	if (nvmeq)
		return nvmeq;

	rcu_read_unlock();
	return NULL;
}

static void unlock_nvmeq(struct nvme_queue *nvmeq) __releases(RCU)
{
	rcu_read_unlock();
}

/**
 * nvme_submit_cmd() - Copy a command into a queue and ring the doorbell
 * @nvmeq: The queue to use
 * @cmd: The command to send
 *
 * Safe to use from interrupt context
 */
static int nvme_submit_cmd(struct nvme_queue *nvmeq, struct nvme_command *cmd)
{
	unsigned long flags;
	u16 tail;
	spin_lock_irqsave(&nvmeq->q_lock, flags);
	if (nvmeq->q_suspended) {
		spin_unlock_irqrestore(&nvmeq->q_lock, flags);
		return -EBUSY;
	}
	tail = nvmeq->sq_tail;
	memcpy(&nvmeq->sq_cmds[tail], cmd, sizeof(*cmd));
	if (++tail == nvmeq->q_depth)
		tail = 0;
	writel(tail, nvmeq->q_db);
	nvmeq->sq_tail = tail;
	spin_unlock_irqrestore(&nvmeq->q_lock, flags);

	return 0;
}

static __le64 **iod_list(struct nvme_iod *iod)
{
	return ((void *)iod) + iod->offset;
}

/*
 * Will slightly overestimate the number of pages needed.  This is OK
 * as it only leads to a small amount of wasted memory for the lifetime of
 * the I/O.
 */
static int nvme_npages(unsigned size)
{
	unsigned nprps = DIV_ROUND_UP(size + PAGE_SIZE, PAGE_SIZE);
	return DIV_ROUND_UP(8 * nprps, PAGE_SIZE - 8);
}

static struct nvme_iod *
nvme_alloc_iod(unsigned nseg, unsigned nbytes, gfp_t gfp)
{
	struct nvme_iod *iod = kmalloc(sizeof(struct nvme_iod) +
				sizeof(__le64 *) * nvme_npages(nbytes) +
				sizeof(struct scatterlist) * nseg, gfp);

	if (iod) {
		iod->offset = offsetof(struct nvme_iod, sg[nseg]);
		iod->npages = -1;
		iod->length = nbytes;
		iod->nents = 0;
		iod->first_dma = 0ULL;
		iod->start_time = jiffies;
	}

	return iod;
}

void nvme_free_iod(struct nvme_dev *dev, struct nvme_iod *iod)
{
	const int last_prp = PAGE_SIZE / 8 - 1;
	int i;
	__le64 **list = iod_list(iod);
	dma_addr_t prp_dma = iod->first_dma;

	if (iod->npages == 0)
		dma_pool_free(dev->prp_small_pool, list[0], prp_dma);
	for (i = 0; i < iod->npages; i++) {
		__le64 *prp_list = list[i];
		dma_addr_t next_prp_dma = le64_to_cpu(prp_list[last_prp]);
		dma_pool_free(dev->prp_page_pool, prp_list, prp_dma);
		prp_dma = next_prp_dma;
	}
	kfree(iod);
}

static void nvme_start_io_acct(struct bio *bio)
{
	struct gendisk *disk = bio->bi_bdev->bd_disk;
	if (blk_queue_io_stat(disk->queue)) {
		const int rw = bio_data_dir(bio);
		int cpu = part_stat_lock();
		part_round_stats(cpu, &disk->part0);
		part_stat_inc(cpu, &disk->part0, ios[rw]);
		part_stat_add(cpu, &disk->part0, sectors[rw],
							bio_sectors(bio));
		part_inc_in_flight(&disk->part0, rw);
		part_stat_unlock();
	}
}

static void nvme_end_io_acct(struct bio *bio, unsigned long start_time)
{
	struct gendisk *disk = bio->bi_bdev->bd_disk;
	if (blk_queue_io_stat(disk->queue)) {
		const int rw = bio_data_dir(bio);
		unsigned long duration = jiffies - start_time;
		int cpu = part_stat_lock();
		part_stat_add(cpu, &disk->part0, ticks[rw], duration);
		part_round_stats(cpu, &disk->part0);
		part_dec_in_flight(&disk->part0, rw);
		part_stat_unlock();
	}
}

static void bio_completion(struct nvme_queue *nvmeq, void *ctx,
						struct nvme_completion *cqe)
{
	struct nvme_iod *iod = ctx;
	struct bio *bio = iod->private;
	u16 status = le16_to_cpup(&cqe->status) >> 1;
	int error = 0;

	if (unlikely(status)) {
		if (!(status & NVME_SC_DNR ||
				bio->bi_rw & REQ_FAILFAST_MASK) &&
				(jiffies - iod->start_time) < IOD_TIMEOUT) {
			if (!waitqueue_active(&nvmeq->sq_full))
				add_wait_queue(&nvmeq->sq_full,
							&nvmeq->sq_cong_wait);
			list_add_tail(&iod->node, &nvmeq->iod_bio);
			wake_up(&nvmeq->sq_full);
			return;
		}
		error = -EIO;
	}
	if (iod->nents) {
		dma_unmap_sg(nvmeq->q_dmadev, iod->sg, iod->nents,
			bio_data_dir(bio) ? DMA_TO_DEVICE : DMA_FROM_DEVICE);
		nvme_end_io_acct(bio, iod->start_time);
	}
	nvme_free_iod(nvmeq->dev, iod);

	trace_block_bio_complete(bdev_get_queue(bio->bi_bdev), bio, error);
	bio_endio(bio, error);
}

static int nvme_setup_kv_prps(struct nvme_dev *dev, struct nvme_common_command *cmd,
		struct nvme_iod *iod, int total_len, gfp_t gfp)
{
	struct dma_pool *pool;
	int length = total_len;
	struct scatterlist *sg = iod->sg;
	int dma_len = sg_dma_len(sg);
	u64 dma_addr = sg_dma_address(sg);
	int offset = offset_in_page(dma_addr);
	__le64 *prp_list;
	__le64 **list = iod_list(iod);
	dma_addr_t prp_dma;
	int nprps, i;

	cmd->prp1 = cpu_to_le64(dma_addr);
	length -= (PAGE_SIZE - offset);
	if (length <= 0)
		return total_len;

	dma_len -= (PAGE_SIZE - offset);
	if (dma_len) {
		dma_addr += (PAGE_SIZE - offset);
	} else {
		sg = sg_next(sg);
		dma_addr = sg_dma_address(sg);
		dma_len = sg_dma_len(sg);
	}

	if (length <= PAGE_SIZE) {
		cmd->prp2 = cpu_to_le64(dma_addr);
		return total_len;
	}

	nprps = DIV_ROUND_UP(length, PAGE_SIZE);
	if (nprps <= (256 / 8)) {
		pool = dev->prp_small_pool;
		iod->npages = 0;
	} else {
		pool = dev->prp_page_pool;
		iod->npages = 1;
	}

	prp_list = dma_pool_alloc(pool, gfp, &prp_dma);
	if (!prp_list) {
		cmd->prp2 = cpu_to_le64(dma_addr);
		iod->npages = -1;
		return (total_len - length) + PAGE_SIZE;
	}
	list[0] = prp_list;
	iod->first_dma = prp_dma;
	cmd->prp2 = cpu_to_le64(prp_dma);
	i = 0;
	for (;;) {
		if (i == PAGE_SIZE / 8) {
			__le64 *old_prp_list = prp_list;
			prp_list = dma_pool_alloc(pool, gfp, &prp_dma);
			if (!prp_list)
				return total_len - length;
			list[iod->npages++] = prp_list;
			prp_list[0] = old_prp_list[i - 1];
			old_prp_list[i - 1] = cpu_to_le64(prp_dma);
			i = 1;
		}
		prp_list[i++] = cpu_to_le64(dma_addr);
		dma_len -= PAGE_SIZE;
		dma_addr += PAGE_SIZE;
		length -= PAGE_SIZE;
		if (length <= 0)
			break;
		if (dma_len > 0)
			continue;
		BUG_ON(dma_len < 0);
		sg = sg_next(sg);
		dma_addr = sg_dma_address(sg);
		dma_len = sg_dma_len(sg);
	}

	return total_len;
}

/* length is in bytes.  gfp flags indicates whether we may sleep. */
int nvme_setup_prps(struct nvme_dev *dev, struct nvme_iod *iod, int total_len,
								gfp_t gfp)
{
	struct dma_pool *pool;
	int length = total_len;
	struct scatterlist *sg = iod->sg;
	int dma_len = sg_dma_len(sg);
	u64 dma_addr = sg_dma_address(sg);
	int offset = offset_in_page(dma_addr);
	__le64 *prp_list;
	__le64 **list = iod_list(iod);
	dma_addr_t prp_dma;
	int nprps, i;

	length -= (PAGE_SIZE - offset);
	if (length <= 0)
		return total_len;

	dma_len -= (PAGE_SIZE - offset);
	if (dma_len) {
		dma_addr += (PAGE_SIZE - offset);
	} else {
		sg = sg_next(sg);
		dma_addr = sg_dma_address(sg);
		dma_len = sg_dma_len(sg);
	}

	if (length <= PAGE_SIZE) {
		iod->first_dma = dma_addr;
		return total_len;
	}

	nprps = DIV_ROUND_UP(length, PAGE_SIZE);
	if (nprps <= (256 / 8)) {
		pool = dev->prp_small_pool;
		iod->npages = 0;
	} else {
		pool = dev->prp_page_pool;
		iod->npages = 1;
	}

	prp_list = dma_pool_alloc(pool, gfp, &prp_dma);
	if (!prp_list) {
		iod->first_dma = dma_addr;
		iod->npages = -1;
		return (total_len - length) + PAGE_SIZE;
	}
	list[0] = prp_list;
	iod->first_dma = prp_dma;
	i = 0;
	for (;;) {
		if (i == PAGE_SIZE / 8) {
			__le64 *old_prp_list = prp_list;
			prp_list = dma_pool_alloc(pool, gfp, &prp_dma);
			if (!prp_list)
				return total_len - length;
			list[iod->npages++] = prp_list;
			prp_list[0] = old_prp_list[i - 1];
			old_prp_list[i - 1] = cpu_to_le64(prp_dma);
			i = 1;
		}
		prp_list[i++] = cpu_to_le64(dma_addr);
		dma_len -= PAGE_SIZE;
		dma_addr += PAGE_SIZE;
		length -= PAGE_SIZE;
		if (length <= 0)
			break;
		if (dma_len > 0)
			continue;
		BUG_ON(dma_len < 0);
		sg = sg_next(sg);
		dma_addr = sg_dma_address(sg);
		dma_len = sg_dma_len(sg);
	}

	return total_len;
}

struct nvme_bio_pair {
	struct bio b1, b2, *parent;
	struct bio_vec *bv1, *bv2;
	int err;
	atomic_t cnt;
};

static void nvme_bio_pair_endio(struct bio *bio, int err)
{
	struct nvme_bio_pair *bp = bio->bi_private;

	if (err)
		bp->err = err;

	if (atomic_dec_and_test(&bp->cnt)) {
		bio_endio(bp->parent, bp->err);
		kfree(bp->bv1);
		kfree(bp->bv2);
		kfree(bp);
	}
}

static struct nvme_bio_pair *nvme_bio_split(struct bio *bio, int idx,
							int len, int offset)
{
	struct nvme_bio_pair *bp;

	BUG_ON(len > bio->bi_size);
	BUG_ON(idx > bio->bi_vcnt);

	bp = kmalloc(sizeof(*bp), GFP_ATOMIC);
	if (!bp)
		return NULL;
	bp->err = 0;

	bp->b1 = *bio;
	bp->b2 = *bio;

	bp->b1.bi_size = len;
	bp->b2.bi_size -= len;
	bp->b1.bi_vcnt = idx;
	bp->b2.bi_idx = idx;
	bp->b2.bi_sector += len >> 9;

	if (offset) {
		bp->bv1 = kmalloc(bio->bi_max_vecs * sizeof(struct bio_vec),
								GFP_ATOMIC);
		if (!bp->bv1)
			goto split_fail_1;

		bp->bv2 = kmalloc(bio->bi_max_vecs * sizeof(struct bio_vec),
								GFP_ATOMIC);
		if (!bp->bv2)
			goto split_fail_2;

		memcpy(bp->bv1, bio->bi_io_vec,
			bio->bi_max_vecs * sizeof(struct bio_vec));
		memcpy(bp->bv2, bio->bi_io_vec,
			bio->bi_max_vecs * sizeof(struct bio_vec));

		bp->b1.bi_io_vec = bp->bv1;
		bp->b2.bi_io_vec = bp->bv2;
		bp->b2.bi_io_vec[idx].bv_offset += offset;
		bp->b2.bi_io_vec[idx].bv_len -= offset;
		bp->b1.bi_io_vec[idx].bv_len = offset;
		bp->b1.bi_vcnt++;
	} else
		bp->bv1 = bp->bv2 = NULL;

	bp->b1.bi_private = bp;
	bp->b2.bi_private = bp;

	bp->b1.bi_end_io = nvme_bio_pair_endio;
	bp->b2.bi_end_io = nvme_bio_pair_endio;

	bp->parent = bio;
	atomic_set(&bp->cnt, 2);

	return bp;

 split_fail_2:
	kfree(bp->bv1);
 split_fail_1:
	kfree(bp);
	return NULL;
}

static int nvme_split_and_submit(struct bio *bio, struct nvme_queue *nvmeq,
						int idx, int len, int offset)
{
	struct nvme_bio_pair *bp = nvme_bio_split(bio, idx, len, offset);
	if (!bp)
		return -ENOMEM;

	trace_block_split(bdev_get_queue(bio->bi_bdev), bio,
					bio->bi_sector);

	if (!waitqueue_active(&nvmeq->sq_full))
		add_wait_queue(&nvmeq->sq_full, &nvmeq->sq_cong_wait);
	bio_list_add(&nvmeq->sq_cong, &bp->b1);
	bio_list_add(&nvmeq->sq_cong, &bp->b2);
	wake_up(&nvmeq->sq_full);

	return 0;
}

/* NVMe scatterlists require no holes in the virtual address */
#define BIOVEC_NOT_VIRT_MERGEABLE(vec1, vec2)	((vec2)->bv_offset || \
			(((vec1)->bv_offset + (vec1)->bv_len) % PAGE_SIZE))

static int nvme_map_bio(struct nvme_queue *nvmeq, struct nvme_iod *iod,
		struct bio *bio, enum dma_data_direction dma_dir, int psegs)
{
	struct bio_vec *bvec, *bvprv = NULL;
	struct scatterlist *sg = NULL;
	int i, length = 0, nsegs = 0, split_len = bio->bi_size;

	if (nvmeq->dev->stripe_size)
		split_len = nvmeq->dev->stripe_size -
			((bio->bi_sector << 9) & (nvmeq->dev->stripe_size - 1));

	sg_init_table(iod->sg, psegs);
	bio_for_each_segment(bvec, bio, i) {
		if (bvprv && BIOVEC_PHYS_MERGEABLE(bvprv, bvec)) {
			sg->length += bvec->bv_len;
		} else {
			if (bvprv && BIOVEC_NOT_VIRT_MERGEABLE(bvprv, bvec))
				return nvme_split_and_submit(bio, nvmeq, i,
								length, 0);

			sg = sg ? sg + 1 : iod->sg;
			sg_set_page(sg, bvec->bv_page, bvec->bv_len,
							bvec->bv_offset);
			nsegs++;
		}

		if (split_len - length < bvec->bv_len)
			return nvme_split_and_submit(bio, nvmeq, i, split_len,
							split_len - length);
		length += bvec->bv_len;
		bvprv = bvec;
	}
	iod->nents = nsegs;
	sg_mark_end(sg);
	if (dma_map_sg(nvmeq->q_dmadev, iod->sg, iod->nents, dma_dir) == 0)
		return -ENOMEM;

	BUG_ON(length != bio->bi_size);
	return length;
}

static int nvme_submit_discard(struct nvme_queue *nvmeq, struct nvme_ns *ns,
		struct bio *bio, struct nvme_iod *iod, int cmdid)
{
	struct nvme_dsm_range *range =
				(struct nvme_dsm_range *)iod_list(iod)[0];
	struct nvme_command *cmnd = &nvmeq->sq_cmds[nvmeq->sq_tail];

	range->cattr = cpu_to_le32(0);
	range->nlb = cpu_to_le32(bio->bi_size >> ns->lba_shift);
	range->slba = cpu_to_le64(nvme_block_nr(ns, bio->bi_sector));

	memset(cmnd, 0, sizeof(*cmnd));
	cmnd->dsm.opcode = nvme_cmd_dsm;
	cmnd->dsm.command_id = cmdid;
	cmnd->dsm.nsid = cpu_to_le32(ns->ns_id);
	cmnd->dsm.prp1 = cpu_to_le64(iod->first_dma);
	cmnd->dsm.nr = 0;
	cmnd->dsm.attributes = cpu_to_le32(NVME_DSMGMT_AD);

	if (++nvmeq->sq_tail == nvmeq->q_depth)
		nvmeq->sq_tail = 0;
	writel(nvmeq->sq_tail, nvmeq->q_db);

	return 0;
}

static int nvme_submit_flush(struct nvme_queue *nvmeq, struct nvme_ns *ns,
								int cmdid)
{
	struct nvme_command *cmnd = &nvmeq->sq_cmds[nvmeq->sq_tail];

	memset(cmnd, 0, sizeof(*cmnd));
	cmnd->common.opcode = nvme_cmd_flush;
	cmnd->common.command_id = cmdid;
	cmnd->common.nsid = cpu_to_le32(ns->ns_id);

	if (++nvmeq->sq_tail == nvmeq->q_depth)
		nvmeq->sq_tail = 0;
	writel(nvmeq->sq_tail, nvmeq->q_db);

	return 0;
}

static int nvme_submit_iod(struct nvme_queue *nvmeq, struct nvme_iod *iod)
{
	struct bio *bio = iod->private;
	struct nvme_ns *ns = bio->bi_bdev->bd_disk->private_data;
	struct nvme_command *cmnd;
	int cmdid;
	u16 control;
	u32 dsmgmt;

	cmdid = alloc_cmdid(nvmeq, iod, bio_completion, NVME_IO_TIMEOUT);
	if (unlikely(cmdid < 0))
		return cmdid;

	if (bio->bi_rw & REQ_DISCARD)
		return nvme_submit_discard(nvmeq, ns, bio, iod, cmdid);
	if (bio->bi_rw & REQ_FLUSH)
		return nvme_submit_flush(nvmeq, ns, cmdid);

	control = 0;
	if (bio->bi_rw & REQ_FUA)
		control |= NVME_RW_FUA;
	if (bio->bi_rw & (REQ_FAILFAST_DEV | REQ_RAHEAD))
		control |= NVME_RW_LR;

	dsmgmt = 0;
	if (bio->bi_rw & REQ_RAHEAD)
		dsmgmt |= NVME_RW_DSM_FREQ_PREFETCH;

	cmnd = &nvmeq->sq_cmds[nvmeq->sq_tail];
	memset(cmnd, 0, sizeof(*cmnd));

	cmnd->rw.opcode = bio_data_dir(bio) ? nvme_cmd_write : nvme_cmd_read;
	cmnd->rw.command_id = cmdid;
	cmnd->rw.nsid = cpu_to_le32(ns->ns_id);
	cmnd->rw.prp1 = cpu_to_le64(sg_dma_address(iod->sg));
	cmnd->rw.prp2 = cpu_to_le64(iod->first_dma);
	cmnd->rw.slba = cpu_to_le64(nvme_block_nr(ns, bio->bi_sector));
	cmnd->rw.length =
		cpu_to_le16((bio->bi_size >> ns->lba_shift) - 1);
	cmnd->rw.control = cpu_to_le16(control);
	cmnd->rw.dsmgmt = cpu_to_le32(dsmgmt);

	if (++nvmeq->sq_tail == nvmeq->q_depth)
		nvmeq->sq_tail = 0;
	writel(nvmeq->sq_tail, nvmeq->q_db);

	return 0;

}

static int nvme_split_flush_data(struct nvme_queue *nvmeq, struct bio *bio)
{
	struct nvme_bio_pair *bp = nvme_bio_split(bio, 0, 0, 0);
	if (!bp)
		return -ENOMEM;

	bp->b1.bi_phys_segments = 0;
	bp->b2.bi_rw &= ~REQ_FLUSH;

	if (!waitqueue_active(&nvmeq->sq_full))
		add_wait_queue(&nvmeq->sq_full, &nvmeq->sq_cong_wait);
	bio_list_add(&nvmeq->sq_cong, &bp->b1);
	bio_list_add(&nvmeq->sq_cong, &bp->b2);
	wake_up(&nvmeq->sq_full);

	return 0;
}

/*
 * Called with local interrupts disabled and the q_lock held.  May not sleep.
 */
static int nvme_submit_bio_queue(struct nvme_queue *nvmeq, struct nvme_ns *ns,
								struct bio *bio)
{
	struct nvme_iod *iod;
	int psegs = bio_phys_segments(ns->queue, bio);
	int result;

	if ((bio->bi_rw & REQ_FLUSH) && psegs)
		return nvme_split_flush_data(nvmeq, bio);

	iod = nvme_alloc_iod(psegs, bio->bi_size, GFP_ATOMIC);
	if (!iod)
		return -ENOMEM;

	iod->private = bio;
	if (bio->bi_rw & REQ_DISCARD) {
		void *range;
		/*
		 * We reuse the small pool to allocate the 16-byte range here
		 * as it is not worth having a special pool for these or
		 * additional cases to handle freeing the iod.
		 */
		range = dma_pool_alloc(nvmeq->dev->prp_small_pool,
						GFP_ATOMIC,
						&iod->first_dma);
		if (!range) {
			result = -ENOMEM;
			goto free_iod;
		}
		iod_list(iod)[0] = (__le64 *)range;
		iod->npages = 0;
	} else if (psegs) {
		result = nvme_map_bio(nvmeq, iod, bio,
			bio_data_dir(bio) ? DMA_TO_DEVICE : DMA_FROM_DEVICE,
			psegs);
		if (result <= 0)
			goto free_iod;
		if (nvme_setup_prps(nvmeq->dev, iod, result, GFP_ATOMIC) !=
								result) {
			result = -ENOMEM;
			goto free_iod;
		}
		nvme_start_io_acct(bio);
	}
	if (unlikely(nvme_submit_iod(nvmeq, iod))) {
		if (!waitqueue_active(&nvmeq->sq_full))
			add_wait_queue(&nvmeq->sq_full, &nvmeq->sq_cong_wait);
		list_add_tail(&iod->node, &nvmeq->iod_bio);
	}
	return 0;

 free_iod:
	nvme_free_iod(nvmeq->dev, iod);
	return result;
}

static int nvme_process_cq(struct nvme_queue *nvmeq)
{
	u16 head, phase;

	head = nvmeq->cq_head;
	phase = nvmeq->cq_phase;

	for (;;) {
		void *ctx;
		nvme_completion_fn fn;
		struct nvme_completion cqe = nvmeq->cqes[head];
		if ((le16_to_cpu(cqe.status) & 1) != phase)
			break;
		nvmeq->sq_head = le16_to_cpu(cqe.sq_head);
		if (++head == nvmeq->q_depth) {
			head = 0;
			phase = !phase;
		}

		ctx = free_cmdid(nvmeq, cqe.command_id, &fn);
		fn(nvmeq, ctx, &cqe);
	}

	/* If the controller ignores the cq head doorbell and continuously
	 * writes to the queue, it is theoretically possible to wrap around
	 * the queue twice and mistakenly return IRQ_NONE.  Linux only
	 * requires that 0.1% of your interrupts are handled, so this isn't
	 * a big problem.
	 */
	if (head == nvmeq->cq_head && phase == nvmeq->cq_phase)
		return 0;

	writel(head, nvmeq->q_db + nvmeq->dev->db_stride);
	nvmeq->cq_head = head;
	nvmeq->cq_phase = phase;

	nvmeq->cqe_seen = 1;
	return 1;
}

static void nvme_make_request(struct request_queue *q, struct bio *bio)
{
	struct nvme_ns *ns = q->queuedata;
	struct nvme_queue *nvmeq = get_nvmeq(ns->dev);
	int result = -EBUSY;

	if (!nvmeq) {
		bio_endio(bio, -EIO);
		return;
	}

	spin_lock_irq(&nvmeq->q_lock);
	if (!nvmeq->q_suspended && bio_list_empty(&nvmeq->sq_cong))
		result = nvme_submit_bio_queue(nvmeq, ns, bio);
	if (unlikely(result)) {
		if (!waitqueue_active(&nvmeq->sq_full))
			add_wait_queue(&nvmeq->sq_full, &nvmeq->sq_cong_wait);
		bio_list_add(&nvmeq->sq_cong, bio);
	}

	nvme_process_cq(nvmeq);
	spin_unlock_irq(&nvmeq->q_lock);
	put_nvmeq(nvmeq);
}

static irqreturn_t nvme_irq(int irq, void *data)
{
	irqreturn_t result;
	struct nvme_queue *nvmeq = data;
	spin_lock(&nvmeq->q_lock);
	nvme_process_cq(nvmeq);
	result = nvmeq->cqe_seen ? IRQ_HANDLED : IRQ_NONE;
	nvmeq->cqe_seen = 0;
	spin_unlock(&nvmeq->q_lock);
	return result;
}

static irqreturn_t nvme_irq_check(int irq, void *data)
{
	struct nvme_queue *nvmeq = data;
	struct nvme_completion cqe = nvmeq->cqes[nvmeq->cq_head];
	if ((le16_to_cpu(cqe.status) & 1) != nvmeq->cq_phase)
		return IRQ_NONE;
	return IRQ_WAKE_THREAD;
}

static void nvme_abort_command(struct nvme_queue *nvmeq, int cmdid)
{
	spin_lock_irq(&nvmeq->q_lock);
	cancel_cmdid(nvmeq, cmdid, NULL);
	spin_unlock_irq(&nvmeq->q_lock);
}

struct sync_cmd_info {
	struct task_struct *task;
	u32 result;
	int status;
};

static void sync_completion(struct nvme_queue *nvmeq, void *ctx,
						struct nvme_completion *cqe)
{
	struct sync_cmd_info *cmdinfo = ctx;
	cmdinfo->result = le32_to_cpup(&cqe->result);
	cmdinfo->status = le16_to_cpup(&cqe->status) >> 1;
	wake_up_process(cmdinfo->task);
}

/*
 * Returns 0 on success.  If the result is negative, it's a Linux error code;
 * if the result is positive, it's an NVM Express status code
 */
static int nvme_submit_sync_cmd(struct nvme_dev *dev, int q_idx,
						struct nvme_command *cmd,
						u32 *result, unsigned timeout)
{
	int cmdid, ret;
	struct sync_cmd_info cmdinfo;
	struct nvme_queue *nvmeq;

	nvmeq = lock_nvmeq(dev, q_idx);
	if (!nvmeq)
		return -ENODEV;

	cmdinfo.task = current;
	cmdinfo.status = -EINTR;

	cmdid = alloc_cmdid(nvmeq, &cmdinfo, sync_completion, timeout);
	if (cmdid < 0) {
		unlock_nvmeq(nvmeq);
		return cmdid;
	}
	cmd->common.command_id = cmdid;

	set_current_state(TASK_KILLABLE);
	ret = nvme_submit_cmd(nvmeq, cmd);
	if (ret) {
		free_cmdid(nvmeq, cmdid, NULL);
		unlock_nvmeq(nvmeq);
		set_current_state(TASK_RUNNING);
		return ret;
	}
	unlock_nvmeq(nvmeq);
	schedule_timeout(timeout);

	if (cmdinfo.status == -EINTR) {
		nvmeq = lock_nvmeq(dev, q_idx);
		if (nvmeq) {
			nvme_abort_command(nvmeq, cmdid);
			unlock_nvmeq(nvmeq);
		}
		return -EINTR;
	}

	if (result)
		*result = cmdinfo.result;

	return cmdinfo.status;
}

int nvme_submit_async_cmd(struct nvme_queue *nvmeq, struct nvme_command *cmd,
						struct async_cmd_info *cmdinfo,
						unsigned timeout)
{
	int cmdid;

	cmdid = alloc_cmdid_killable(nvmeq, cmdinfo, async_completion, timeout);
	if (cmdid < 0)
		return cmdid;
	cmdinfo->status = -EINTR;
	cmd->common.command_id = cmdid;
	return nvme_submit_cmd(nvmeq, cmd);
}

int nvme_submit_admin_cmd(struct nvme_dev *dev, struct nvme_command *cmd,
								u32 *result)
{
	return nvme_submit_sync_cmd(dev, 0, cmd, result, ADMIN_TIMEOUT);
}

int nvme_submit_io_cmd(struct nvme_dev *dev, struct nvme_command *cmd,
								u32 *result)
{
	return nvme_submit_sync_cmd(dev, smp_processor_id() + 1, cmd, result,
							NVME_IO_TIMEOUT);
}

int nvme_submit_admin_cmd_async(struct nvme_dev *dev, struct nvme_command *cmd,
						struct async_cmd_info *cmdinfo)
{
	return nvme_submit_async_cmd(raw_nvmeq(dev, 0), cmd, cmdinfo,
								ADMIN_TIMEOUT);
}

static int adapter_delete_queue(struct nvme_dev *dev, u8 opcode, u16 id)
{
	int status;
	struct nvme_command c;

	memset(&c, 0, sizeof(c));
	c.delete_queue.opcode = opcode;
	c.delete_queue.qid = cpu_to_le16(id);

	status = nvme_submit_admin_cmd(dev, &c, NULL);
	if (status)
		return -EIO;
	return 0;
}

static int adapter_alloc_cq(struct nvme_dev *dev, u16 qid,
						struct nvme_queue *nvmeq)
{
	int status;
	struct nvme_command c;
	int flags = NVME_QUEUE_PHYS_CONTIG | NVME_CQ_IRQ_ENABLED;

	memset(&c, 0, sizeof(c));
	c.create_cq.opcode = nvme_admin_create_cq;
	c.create_cq.prp1 = cpu_to_le64(nvmeq->cq_dma_addr);
	c.create_cq.cqid = cpu_to_le16(qid);
	c.create_cq.qsize = cpu_to_le16(nvmeq->q_depth - 1);
	c.create_cq.cq_flags = cpu_to_le16(flags);
	c.create_cq.irq_vector = cpu_to_le16(nvmeq->cq_vector);

	status = nvme_submit_admin_cmd(dev, &c, NULL);
	if (status)
		return -EIO;
	return 0;
}

static int adapter_alloc_sq(struct nvme_dev *dev, u16 qid,
						struct nvme_queue *nvmeq)
{
	int status;
	struct nvme_command c;
	int flags = NVME_QUEUE_PHYS_CONTIG | NVME_SQ_PRIO_MEDIUM;

	memset(&c, 0, sizeof(c));
	c.create_sq.opcode = nvme_admin_create_sq;
	c.create_sq.prp1 = cpu_to_le64(nvmeq->sq_dma_addr);
	c.create_sq.sqid = cpu_to_le16(qid);
	c.create_sq.qsize = cpu_to_le16(nvmeq->q_depth - 1);
	c.create_sq.sq_flags = cpu_to_le16(flags);
	c.create_sq.cqid = cpu_to_le16(qid);

	status = nvme_submit_admin_cmd(dev, &c, NULL);
	if (status)
		return -EIO;
	return 0;
}

static int adapter_delete_cq(struct nvme_dev *dev, u16 cqid)
{
	return adapter_delete_queue(dev, nvme_admin_delete_cq, cqid);
}

static int adapter_delete_sq(struct nvme_dev *dev, u16 sqid)
{
	return adapter_delete_queue(dev, nvme_admin_delete_sq, sqid);
}

int nvme_identify(struct nvme_dev *dev, unsigned nsid, unsigned cns,
							dma_addr_t dma_addr)
{
	struct nvme_command c;

	memset(&c, 0, sizeof(c));
	c.identify.opcode = nvme_admin_identify;
	c.identify.nsid = cpu_to_le32(nsid);
	c.identify.prp1 = cpu_to_le64(dma_addr);
	c.identify.cns = cpu_to_le32(cns);

	return nvme_submit_admin_cmd(dev, &c, NULL);
}

int nvme_get_features(struct nvme_dev *dev, unsigned fid, unsigned nsid,
					dma_addr_t dma_addr, u32 *result)
{
	struct nvme_command c;

	memset(&c, 0, sizeof(c));
	c.features.opcode = nvme_admin_get_features;
	c.features.nsid = cpu_to_le32(nsid);
	c.features.prp1 = cpu_to_le64(dma_addr);
	c.features.fid = cpu_to_le32(fid);

	return nvme_submit_admin_cmd(dev, &c, result);
}

int nvme_set_features(struct nvme_dev *dev, unsigned fid, unsigned dword11,
					dma_addr_t dma_addr, u32 *result)
{
	struct nvme_command c;

	memset(&c, 0, sizeof(c));
	c.features.opcode = nvme_admin_set_features;
	c.features.prp1 = cpu_to_le64(dma_addr);
	c.features.fid = cpu_to_le32(fid);
	c.features.dword11 = cpu_to_le32(dword11);

	return nvme_submit_admin_cmd(dev, &c, result);
}

/**
 * nvme_abort_cmd - Attempt aborting a command
 * @cmdid: Command id of a timed out IO
 * @queue: The queue with timed out IO
 *
 * Schedule controller reset if the command was already aborted once before and
 * still hasn't been returned to the driver, or if this is the admin queue.
 */
static void nvme_abort_cmd(int cmdid, struct nvme_queue *nvmeq)
{
	int a_cmdid;
	struct nvme_command cmd;
	struct nvme_dev *dev = nvmeq->dev;
	struct nvme_cmd_info *info = nvme_cmd_info(nvmeq);
	struct nvme_queue *adminq;

	if (!nvmeq->qid || info[cmdid].aborted) {
		if (work_busy(&dev->reset_work))
			return;
		list_del_init(&dev->node);
		dev_warn(&dev->pci_dev->dev,
			"I/O %d QID %d timeout, reset controller\n", cmdid,
								nvmeq->qid);
		PREPARE_WORK(&dev->reset_work, nvme_reset_failed_dev);
		queue_work(nvme_workq, &dev->reset_work);
		return;
	}

	if (!dev->abort_limit)
		return;

	adminq = rcu_dereference(dev->queues[0]);
	a_cmdid = alloc_cmdid(adminq, CMD_CTX_ABORT, special_completion,
								ADMIN_TIMEOUT);
	if (a_cmdid < 0)
		return;

	memset(&cmd, 0, sizeof(cmd));
	cmd.abort.opcode = nvme_admin_abort_cmd;
	cmd.abort.cid = cmdid;
	cmd.abort.sqid = nvmeq->qid;
	cmd.abort.command_id = a_cmdid;

	--dev->abort_limit;
	info[cmdid].aborted = 1;
	info[cmdid].timeout = jiffies + ADMIN_TIMEOUT;

	dev_warn(nvmeq->q_dmadev, "Aborting I/O %d QID %d\n", cmdid,
							nvmeq->qid);
	nvme_submit_cmd(adminq, &cmd);
}

/**
 * nvme_cancel_ios - Cancel outstanding I/Os
 * @queue: The queue to cancel I/Os on
 * @timeout: True to only cancel I/Os which have timed out
 */
static void nvme_cancel_ios(struct nvme_queue *nvmeq, bool timeout)
{
	int depth = nvmeq->q_depth - 1;
	struct nvme_cmd_info *info = nvme_cmd_info(nvmeq);
	unsigned long now = jiffies;
	int cmdid;

	for_each_set_bit(cmdid, nvmeq->cmdid_data, depth) {
		void *ctx;
		nvme_completion_fn fn;
		static struct nvme_completion cqe = {
			.status = cpu_to_le16(NVME_SC_ABORT_REQ << 1),
		};

		if (timeout && !time_after(now, info[cmdid].timeout))
			continue;
		if (info[cmdid].ctx == CMD_CTX_CANCELLED)
			continue;
		if (timeout && nvmeq->dev->initialized) {
			nvme_abort_cmd(cmdid, nvmeq);
			continue;
		}
		dev_warn(nvmeq->q_dmadev, "Cancelling I/O %d QID %d\n", cmdid,
								nvmeq->qid);
		ctx = cancel_cmdid(nvmeq, cmdid, &fn);
		fn(nvmeq, ctx, &cqe);
	}
}

static void nvme_free_queue(struct rcu_head *r)
{
	struct nvme_queue *nvmeq = container_of(r, struct nvme_queue, r_head);

	spin_lock_irq(&nvmeq->q_lock);
	while (bio_list_peek(&nvmeq->sq_cong)) {
		struct bio *bio = bio_list_pop(&nvmeq->sq_cong);
		bio_endio(bio, -EIO);
	}
	while (!list_empty(&nvmeq->iod_bio)) {
		static struct nvme_completion cqe = {
			.status = cpu_to_le16(
				(NVME_SC_ABORT_REQ | NVME_SC_DNR) << 1),
		};
		struct nvme_iod *iod = list_first_entry(&nvmeq->iod_bio,
							struct nvme_iod,
							node);
		list_del(&iod->node);
		bio_completion(nvmeq, iod, &cqe);
	}
	spin_unlock_irq(&nvmeq->q_lock);

	dma_free_coherent(nvmeq->q_dmadev, CQ_SIZE(nvmeq->q_depth),
				(void *)nvmeq->cqes, nvmeq->cq_dma_addr);
	dma_free_coherent(nvmeq->q_dmadev, SQ_SIZE(nvmeq->q_depth),
					nvmeq->sq_cmds, nvmeq->sq_dma_addr);
	if (nvmeq->qid)
		free_cpumask_var(nvmeq->cpu_mask);
	kfree(nvmeq);
}

static void nvme_free_queues(struct nvme_dev *dev, int lowest)
{
	int i;

	for (i = dev->queue_count - 1; i >= lowest; i--) {
		struct nvme_queue *nvmeq = raw_nvmeq(dev, i);
		rcu_assign_pointer(dev->queues[i], NULL);
		call_rcu(&nvmeq->r_head, nvme_free_queue);
		dev->queue_count--;
	}
}

/**
 * nvme_suspend_queue - put queue into suspended state
 * @nvmeq - queue to suspend
 *
 * Returns 1 if already suspended, 0 otherwise.
 */
static int nvme_suspend_queue(struct nvme_queue *nvmeq)
{
	int vector = nvmeq->dev->entry[nvmeq->cq_vector].vector;

	spin_lock_irq(&nvmeq->q_lock);
	if (nvmeq->q_suspended) {
		spin_unlock_irq(&nvmeq->q_lock);
		return 1;
	}
	nvmeq->q_suspended = 1;
	nvmeq->dev->online_queues--;
	spin_unlock_irq(&nvmeq->q_lock);

	irq_set_affinity_hint(vector, NULL);
	free_irq(vector, nvmeq);

	return 0;
}

static void nvme_clear_queue(struct nvme_queue *nvmeq)
{
	spin_lock_irq(&nvmeq->q_lock);
	nvme_process_cq(nvmeq);
	nvme_cancel_ios(nvmeq, false);
	spin_unlock_irq(&nvmeq->q_lock);
}

static void nvme_disable_queue(struct nvme_dev *dev, int qid)
{
	struct nvme_queue *nvmeq = raw_nvmeq(dev, qid);

	if (!nvmeq)
		return;
	if (nvme_suspend_queue(nvmeq))
		return;

	/* Don't tell the adapter to delete the admin queue.
	 * Don't tell a removed adapter to delete IO queues. */
	if (qid && readl(&dev->bar->csts) != -1) {
		adapter_delete_sq(dev, qid);
		adapter_delete_cq(dev, qid);
	}
	nvme_clear_queue(nvmeq);
}

static struct nvme_queue *nvme_alloc_queue(struct nvme_dev *dev, int qid,
							int depth, int vector)
{
	struct device *dmadev = &dev->pci_dev->dev;
	unsigned extra = nvme_queue_extra(depth);
	struct nvme_queue *nvmeq = kzalloc(sizeof(*nvmeq) + extra, GFP_KERNEL);
	if (!nvmeq)
		return NULL;

	nvmeq->cqes = dma_alloc_coherent(dmadev, CQ_SIZE(depth),
					&nvmeq->cq_dma_addr, GFP_KERNEL);
	if (!nvmeq->cqes)
		goto free_nvmeq;
	memset((void *)nvmeq->cqes, 0, CQ_SIZE(depth));

	nvmeq->sq_cmds = dma_alloc_coherent(dmadev, SQ_SIZE(depth),
					&nvmeq->sq_dma_addr, GFP_KERNEL);
	if (!nvmeq->sq_cmds)
		goto free_cqdma;

	if (qid && !zalloc_cpumask_var(&nvmeq->cpu_mask, GFP_KERNEL))
		goto free_sqdma;

	nvmeq->q_dmadev = dmadev;
	nvmeq->dev = dev;
	snprintf(nvmeq->irqname, sizeof(nvmeq->irqname), "nvme%dq%d",
			dev->instance, qid);
	spin_lock_init(&nvmeq->q_lock);
	nvmeq->cq_head = 0;
	nvmeq->cq_phase = 1;
	init_waitqueue_head(&nvmeq->sq_full);
	init_waitqueue_entry(&nvmeq->sq_cong_wait, nvme_thread);
	bio_list_init(&nvmeq->sq_cong);
	INIT_LIST_HEAD(&nvmeq->iod_bio);
	nvmeq->q_db = &dev->dbs[qid * 2 * dev->db_stride];
	nvmeq->q_depth = depth;
	nvmeq->cq_vector = vector;
	nvmeq->qid = qid;
	nvmeq->q_suspended = 1;
	dev->queue_count++;
	rcu_assign_pointer(dev->queues[qid], nvmeq);

	return nvmeq;

 free_sqdma:
	dma_free_coherent(dmadev, SQ_SIZE(depth), (void *)nvmeq->sq_cmds,
							nvmeq->sq_dma_addr);
 free_cqdma:
	dma_free_coherent(dmadev, CQ_SIZE(depth), (void *)nvmeq->cqes,
							nvmeq->cq_dma_addr);
 free_nvmeq:
	kfree(nvmeq);
	return NULL;
}

static int queue_request_irq(struct nvme_dev *dev, struct nvme_queue *nvmeq,
							const char *name)
{
	if (use_threaded_interrupts)
		return request_threaded_irq(dev->entry[nvmeq->cq_vector].vector,
					nvme_irq_check, nvme_irq, IRQF_SHARED,
					name, nvmeq);
	return request_irq(dev->entry[nvmeq->cq_vector].vector, nvme_irq,
				IRQF_SHARED, name, nvmeq);
}

static void nvme_init_queue(struct nvme_queue *nvmeq, u16 qid)
{
	struct nvme_dev *dev = nvmeq->dev;
	unsigned extra = nvme_queue_extra(nvmeq->q_depth);

	nvmeq->sq_tail = 0;
	nvmeq->cq_head = 0;
	nvmeq->cq_phase = 1;
	nvmeq->q_db = &dev->dbs[qid * 2 * dev->db_stride];
	memset(nvmeq->cmdid_data, 0, extra);
	memset((void *)nvmeq->cqes, 0, CQ_SIZE(nvmeq->q_depth));
	nvme_cancel_ios(nvmeq, false);
	nvmeq->q_suspended = 0;
	dev->online_queues++;
}

static int nvme_create_queue(struct nvme_queue *nvmeq, int qid)
{
	struct nvme_dev *dev = nvmeq->dev;
	int result;

	result = adapter_alloc_cq(dev, qid, nvmeq);
	if (result < 0)
		return result;

	result = adapter_alloc_sq(dev, qid, nvmeq);
	if (result < 0)
		goto release_cq;

	result = queue_request_irq(dev, nvmeq, nvmeq->irqname);
	if (result < 0)
		goto release_sq;

	spin_lock_irq(&nvmeq->q_lock);
	nvme_init_queue(nvmeq, qid);
	spin_unlock_irq(&nvmeq->q_lock);

	return result;

 release_sq:
	adapter_delete_sq(dev, qid);
 release_cq:
	adapter_delete_cq(dev, qid);
	return result;
}

static int nvme_wait_ready(struct nvme_dev *dev, u64 cap, bool enabled)
{
	unsigned long timeout;
	u32 bit = enabled ? NVME_CSTS_RDY : 0;

	timeout = ((NVME_CAP_TIMEOUT(cap) + 1) * HZ / 2) + jiffies;

	while ((readl(&dev->bar->csts) & NVME_CSTS_RDY) != bit) {
		msleep(100);
		if (fatal_signal_pending(current))
			return -EINTR;
		if (time_after(jiffies, timeout)) {
			dev_err(&dev->pci_dev->dev,
				"Device not ready; aborting %s\n", enabled ?
						"initialisation" : "reset");
			return -ENODEV;
		}
	}

	return 0;
}

/*
 * If the device has been passed off to us in an enabled state, just clear
 * the enabled bit.  The spec says we should set the 'shutdown notification
 * bits', but doing so may cause the device to complete commands to the
 * admin queue ... and we don't know what memory that might be pointing at!
 */
static int nvme_disable_ctrl(struct nvme_dev *dev, u64 cap)
{
	u32 cc = readl(&dev->bar->cc);

	if (cc & NVME_CC_ENABLE)
		writel(cc & ~NVME_CC_ENABLE, &dev->bar->cc);
	return nvme_wait_ready(dev, cap, false);
}

static int nvme_enable_ctrl(struct nvme_dev *dev, u64 cap)
{
	return nvme_wait_ready(dev, cap, true);
}

static int nvme_shutdown_ctrl(struct nvme_dev *dev)
{
	unsigned long timeout;
	u32 cc;

	cc = (readl(&dev->bar->cc) & ~NVME_CC_SHN_MASK) | NVME_CC_SHN_NORMAL;
	writel(cc, &dev->bar->cc);

	timeout = 2 * HZ + jiffies;
	while ((readl(&dev->bar->csts) & NVME_CSTS_SHST_MASK) !=
							NVME_CSTS_SHST_CMPLT) {
		msleep(100);
		if (fatal_signal_pending(current))
			return -EINTR;
		if (time_after(jiffies, timeout)) {
			dev_err(&dev->pci_dev->dev,
				"Device shutdown incomplete; abort shutdown\n");
			return -ENODEV;
		}
	}

	return 0;
}

static int nvme_configure_admin_queue(struct nvme_dev *dev)
{
	int result;
	u32 aqa;
	u64 cap = readq(&dev->bar->cap);
	struct nvme_queue *nvmeq;

	result = nvme_disable_ctrl(dev, cap);
	if (result < 0)
		return result;

	nvmeq = raw_nvmeq(dev, 0);
	if (!nvmeq) {
		nvmeq = nvme_alloc_queue(dev, 0, 64, 0);
		if (!nvmeq)
			return -ENOMEM;
	}

	aqa = nvmeq->q_depth - 1;
	aqa |= aqa << 16;

	dev->ctrl_config = NVME_CC_ENABLE | NVME_CC_CSS_NVM;
	dev->ctrl_config |= (PAGE_SHIFT - 12) << NVME_CC_MPS_SHIFT;
	dev->ctrl_config |= NVME_CC_ARB_RR | NVME_CC_SHN_NONE;
	dev->ctrl_config |= NVME_CC_IOSQES | NVME_CC_IOCQES;

	writel(aqa, &dev->bar->aqa);
	writeq(nvmeq->sq_dma_addr, &dev->bar->asq);
	writeq(nvmeq->cq_dma_addr, &dev->bar->acq);
	writel(dev->ctrl_config, &dev->bar->cc);

	result = nvme_enable_ctrl(dev, cap);
	if (result)
		return result;

	result = queue_request_irq(dev, nvmeq, nvmeq->irqname);
	if (result)
		return result;

	spin_lock_irq(&nvmeq->q_lock);
	nvme_init_queue(nvmeq, 0);
	spin_unlock_irq(&nvmeq->q_lock);
	return result;
}

struct nvme_iod *nvme_map_user_pages(struct nvme_dev *dev, int write,
				unsigned long addr, unsigned length)
{
	int i, err, count, nents, offset;
	struct scatterlist *sg;
	struct page **pages;
	struct nvme_iod *iod;

	if (addr & 3)
		return ERR_PTR(-EINVAL);
	if (!length || length > INT_MAX - PAGE_SIZE)
		return ERR_PTR(-EINVAL);

	offset = offset_in_page(addr);
	count = DIV_ROUND_UP(offset + length, PAGE_SIZE);
	pages = kcalloc(count, sizeof(*pages), GFP_KERNEL);
	if (!pages)
		return ERR_PTR(-ENOMEM);

	err = get_user_pages_fast(addr, count, 1, pages);
	if (err < count) {
		count = err;
		err = -EFAULT;
		goto put_pages;
	}

	err = -ENOMEM;
	iod = nvme_alloc_iod(count, length, GFP_KERNEL);
	if (!iod)
		goto put_pages;

	sg = iod->sg;
	sg_init_table(sg, count);
	for (i = 0; i < count; i++) {
		sg_set_page(&sg[i], pages[i],
			    min_t(unsigned, length, PAGE_SIZE - offset),
			    offset);
		length -= (PAGE_SIZE - offset);
		offset = 0;
	}
	sg_mark_end(&sg[i - 1]);
	iod->nents = count;

	nents = dma_map_sg(&dev->pci_dev->dev, sg, count,
				write ? DMA_TO_DEVICE : DMA_FROM_DEVICE);
	if (!nents)
		goto free_iod;

	kfree(pages);
	return iod;

 free_iod:
	kfree(iod);
 put_pages:
	for (i = 0; i < count; i++)
		put_page(pages[i]);
	kfree(pages);
	return ERR_PTR(err);
}

void nvme_unmap_user_pages(struct nvme_dev *dev, int write,
			struct nvme_iod *iod)
{
	int i;

	dma_unmap_sg(&dev->pci_dev->dev, iod->sg, iod->nents,
				write ? DMA_TO_DEVICE : DMA_FROM_DEVICE);

	for (i = 0; i < iod->nents; i++)
		put_page(sg_page(&iod->sg[i]));
}

static int nvme_submit_io(struct nvme_ns *ns, struct nvme_user_io __user *uio)
{
	struct nvme_dev *dev = ns->dev;
	struct nvme_user_io io;
	struct nvme_command c;
	unsigned length, meta_len;
	int status, i;
	struct nvme_iod *iod, *meta_iod = NULL;
	dma_addr_t meta_dma_addr;
	void *meta, *uninitialized_var(meta_mem);

	if (copy_from_user(&io, uio, sizeof(io)))
		return -EFAULT;
	length = (io.nblocks + 1) << ns->lba_shift;
	meta_len = (io.nblocks + 1) * ns->ms;

	if (meta_len && ((io.metadata & 3) || !io.metadata))
		return -EINVAL;

	switch (io.opcode) {
	case nvme_cmd_write:
	case nvme_cmd_read:
	case nvme_cmd_compare:
		iod = nvme_map_user_pages(dev, io.opcode & 1, io.addr, length);
		break;
	default:
		return -EINVAL;
	}

	if (IS_ERR(iod))
		return PTR_ERR(iod);

	memset(&c, 0, sizeof(c));
	c.rw.opcode = io.opcode;
	c.rw.flags = io.flags;
	c.rw.nsid = cpu_to_le32(ns->ns_id);
	c.rw.slba = cpu_to_le64(io.slba);
	c.rw.length = cpu_to_le16(io.nblocks);
	c.rw.control = cpu_to_le16(io.control);
	c.rw.dsmgmt = cpu_to_le32(io.dsmgmt);
	c.rw.reftag = cpu_to_le32(io.reftag);
	c.rw.apptag = cpu_to_le16(io.apptag);
	c.rw.appmask = cpu_to_le16(io.appmask);

	if (meta_len) {
		meta_iod = nvme_map_user_pages(dev, io.opcode & 1, io.metadata,
								meta_len);
		if (IS_ERR(meta_iod)) {
			status = PTR_ERR(meta_iod);
			meta_iod = NULL;
			goto unmap;
		}

		meta_mem = dma_alloc_coherent(&dev->pci_dev->dev, meta_len,
						&meta_dma_addr, GFP_KERNEL);
		if (!meta_mem) {
			status = -ENOMEM;
			goto unmap;
		}

		if (io.opcode & 1) {
			int meta_offset = 0;

			for (i = 0; i < meta_iod->nents; i++) {
				meta = kmap_atomic(sg_page(&meta_iod->sg[i])) +
						meta_iod->sg[i].offset;
				memcpy(meta_mem + meta_offset, meta,
						meta_iod->sg[i].length);
				kunmap_atomic(meta);
				meta_offset += meta_iod->sg[i].length;
			}
		}

		c.rw.metadata = cpu_to_le64(meta_dma_addr);
	}

	length = nvme_setup_prps(dev, iod, length, GFP_KERNEL);
	c.rw.prp1 = cpu_to_le64(sg_dma_address(iod->sg));
	c.rw.prp2 = cpu_to_le64(iod->first_dma);

	if (length != (io.nblocks + 1) << ns->lba_shift)
		status = -ENOMEM;
	else
		status = nvme_submit_io_cmd(dev, &c, NULL);

	if (meta_len) {
		if (status == NVME_SC_SUCCESS && !(io.opcode & 1)) {
			int meta_offset = 0;

			for (i = 0; i < meta_iod->nents; i++) {
				meta = kmap_atomic(sg_page(&meta_iod->sg[i])) +
						meta_iod->sg[i].offset;
				memcpy(meta, meta_mem + meta_offset,
						meta_iod->sg[i].length);
				kunmap_atomic(meta);
				meta_offset += meta_iod->sg[i].length;
			}
		}

		dma_free_coherent(&dev->pci_dev->dev, meta_len, meta_mem,
								meta_dma_addr);
	}

 unmap:
	nvme_unmap_user_pages(dev, io.opcode & 1, iod);
	nvme_free_iod(dev, iod);

	if (meta_iod) {
		nvme_unmap_user_pages(dev, io.opcode & 1, meta_iod);
		nvme_free_iod(dev, meta_iod);
	}

	return status;
}

static int nvme_user_admin_cmd(struct nvme_dev *dev,
					struct nvme_admin_cmd __user *ucmd)
{
	struct nvme_admin_cmd cmd;
	struct nvme_command c;
	int status, length;
	struct nvme_iod *uninitialized_var(iod);
	unsigned timeout;

	if (!capable(CAP_SYS_ADMIN))
		return -EACCES;
	if (copy_from_user(&cmd, ucmd, sizeof(cmd)))
		return -EFAULT;

	memset(&c, 0, sizeof(c));
	c.common.opcode = cmd.opcode;
	c.common.flags = cmd.flags;
	c.common.nsid = cpu_to_le32(cmd.nsid);
	c.common.cdw2[0] = cpu_to_le32(cmd.cdw2);
	c.common.cdw2[1] = cpu_to_le32(cmd.cdw3);
	c.common.cdw10[0] = cpu_to_le32(cmd.cdw10);
	c.common.cdw10[1] = cpu_to_le32(cmd.cdw11);
	c.common.cdw10[2] = cpu_to_le32(cmd.cdw12);
	c.common.cdw10[3] = cpu_to_le32(cmd.cdw13);
	c.common.cdw10[4] = cpu_to_le32(cmd.cdw14);
	c.common.cdw10[5] = cpu_to_le32(cmd.cdw15);

	length = cmd.data_len;
	if (cmd.data_len) {
		iod = nvme_map_user_pages(dev, cmd.opcode & 1, cmd.addr,
								length);
		if (IS_ERR(iod))
			return PTR_ERR(iod);
		length = nvme_setup_prps(dev, iod, length, GFP_KERNEL);
		c.common.prp1 = cpu_to_le64(sg_dma_address(iod->sg));
		c.common.prp2 = cpu_to_le64(iod->first_dma);
	}

	timeout = cmd.timeout_ms ? msecs_to_jiffies(cmd.timeout_ms) :
								ADMIN_TIMEOUT;
	if (length != cmd.data_len)
		status = -ENOMEM;
	else
		status = nvme_submit_sync_cmd(dev, 0, &c, &cmd.result, timeout);

	if (cmd.data_len) {
		nvme_unmap_user_pages(dev, cmd.opcode & 1, iod);
		nvme_free_iod(dev, iod);
	}

	if ((status >= 0) && copy_to_user(&ucmd->result, &cmd.result,
							sizeof(cmd.result)))
		status = -EFAULT;

	return status;
}

static int user_addr_npages(int offset, int size)
{
	unsigned count = DIV_ROUND_UP(offset + size, PAGE_SIZE);
	return count;
}

static struct aio_user_ctx *get_aio_user_ctx(void __user *addr, unsigned len)
{
	int offset = offset_in_page(addr);
	int datalen = len;
	int num_page = user_addr_npages(offset,len);
	int size = 0;
	struct aio_user_ctx  *user_ctx = NULL;
	int mapped_pages = 0;
	int i = 0;

	size = sizeof (struct aio_user_ctx) + sizeof(__le64 *) * num_page
			+ sizeof(struct scatterlist) * num_page -1;
	/* need to keep user address to map to copy when complete request */
	user_ctx = (struct aio_user_ctx *)kmalloc(size, GFP_KERNEL);
	if (!user_ctx)
		return NULL;

	user_ctx->nents = 0;
	user_ctx->pages =(struct page **)user_ctx->data;
	user_ctx->sg = (struct scatterlist *)(user_ctx->data + sizeof(__le64 *) * num_page);
	mapped_pages = get_user_pages_fast((unsigned long)addr, num_page, 1, user_ctx->pages);
	if (mapped_pages != num_page) {
		user_ctx->nents = mapped_pages;
		goto exit;
	}
	user_ctx->nents = num_page;
	user_ctx->len = datalen;
	sg_init_table(user_ctx->sg, num_page);
	for(i = 0; i < num_page; i++) {
			sg_set_page(&user_ctx->sg[i], user_ctx->pages[i],
							min_t(unsigned, datalen, PAGE_SIZE - offset), offset);
			datalen -= (PAGE_SIZE - offset);
			offset = 0;
	}
	sg_mark_end(&user_ctx->sg[i -1]);
	return user_ctx;
exit:
	if (user_ctx) {
		for (i = 0; i < user_ctx->nents; i++)
			put_page(user_ctx->pages[i]);
		kfree(user_ctx);
	}
	return NULL;
}

struct nvme_iod *nvme_map_kernel_pages(struct nvme_dev *dev, int write,
		unsigned long addr, unsigned length)
{
	int i, err, count, nents, offset;
	struct scatterlist *sg;
	struct page **pages;
	struct page *page = NULL;
	struct nvme_iod *iod;
	char *src_data = NULL;

	if (addr & 3)
		return ERR_PTR(-EINVAL);
	if (!length || length > INT_MAX - PAGE_SIZE)
		return ERR_PTR(-EINVAL);

	offset = offset_in_page(addr);
	count = DIV_ROUND_UP(offset + length, PAGE_SIZE);
	pages = kcalloc(count, sizeof(*pages), GFP_KERNEL);
	if (!pages)
		return ERR_PTR(-ENOMEM);

	src_data = (char *)addr;
	for (i = 0; i < count; i ++) {
		page = virt_to_page(src_data);
		get_page(page);
		pages[i] = page;
		src_data += PAGE_SIZE;
	}				
	iod = nvme_alloc_iod(count, length, GFP_KERNEL);
	sg = iod->sg;
	sg_init_table(sg, count);
	for (i = 0; i < count; i++) {
		sg_set_page(&sg[i], pages[i],
				min_t(unsigned, length, PAGE_SIZE - offset),
				offset);
		length -= (PAGE_SIZE - offset);
		offset = 0;
	}
	sg_mark_end(&sg[i - 1]);
	iod->nents = count;
	err = -ENOMEM;
	nents = dma_map_sg(&dev->pci_dev->dev, sg, count,
			write ? DMA_TO_DEVICE : DMA_FROM_DEVICE);
	if (!nents)
		goto free_iod;

	kfree(pages);
	return iod;

free_iod:
	kfree(iod);
	for (i = 0; i < count; i++)
		put_page(pages[i]);
	kfree(pages);
	return ERR_PTR(err);
}

int nvme_submit_async_kv_cmd(struct nvme_queue *nvmeq, struct nvme_command* cmd,
		u32 *result, unsigned timeout, struct nvme_kaiocb *aiocb) {
	int cmdid;
	aiocb->event.status = -EINTR;
	cmdid = alloc_cmdid_killable(nvmeq, aiocb, kv_async_completion, timeout);
	if (cmdid < 0)
		return cmdid;
	cmd->common.command_id = cmdid;
	nvme_submit_cmd(nvmeq, cmd);
	return 0;
}

#define KV_QUEUE_DAM_ALIGNMENT (0x03)
static bool check_for_single_phyaddress(void __user* address, unsigned length) {
	unsigned offset = 0;
	unsigned count = 0;
	offset = offset_in_page(address);
	count = DIV_ROUND_UP(offset + length, PAGE_SIZE);
	if (count > 1 || ((unsigned long)address & KV_QUEUE_DAM_ALIGNMENT)) {
		return false;
	}
	return true;
}

int __nvme_submit_kv_user_cmd(struct nvme_ns *ns, struct nvme_command *cmd,
		struct nvme_passthru_kv_cmd *pthr_cmd,
		void __user *ubuffer, unsigned bufflen,
		void __user *meta_buffer, unsigned meta_len,
		u32 *result, u32 *status, unsigned timeout, bool aio)
{
	struct nvme_dev *dev = ns->dev;
	struct nvme_queue *nvmeq = NULL;
	struct nvme_kaiocb *aiocb = NULL;
	struct scatterlist *meta_sg_ptr = NULL;
	struct scatterlist meta_sg;
	struct nvme_iod *iod = NULL;
	unsigned length = 0;
	int ret = 0;
	void* meta = NULL;
	char* kv_data = NULL;
	bool set_meta = false;
	bool need_to_copy = false;
	struct page *p_page = NULL;
	struct aio_user_ctx * user_ctx = NULL;
	if (aio) {
		aiocb = get_aiocb(pthr_cmd->reqid);
		if (!aiocb)
			return  -ENOMEM;
	}

	if (ubuffer && bufflen) {
		if ((unsigned long)ubuffer & KV_QUEUE_DAM_ALIGNMENT) {
			int len = DIV_ROUND_UP(bufflen, PAGE_SIZE)*PAGE_SIZE;
			need_to_copy = true;
			kv_data = kmalloc(len, GFP_KERNEL);
			if (kv_data == NULL) {
				ret = -ENOMEM;
				goto out;	
			}

			user_ctx = get_aio_user_ctx(ubuffer, bufflen);
			if (user_ctx == NULL) {
				ret = -ENOMEM;
				goto free_out;
			}
			if (is_kv_store_cmd(cmd->common.opcode) || is_kv_append_cmd(cmd->common.opcode)) {
				(void)sg_copy_to_buffer(user_ctx->sg, user_ctx->nents,
						kv_data, user_ctx->len);
			}
			iod = nvme_map_kernel_pages(dev, kv_nvme_is_write(pthr_cmd->opcode), (unsigned long)kv_data, bufflen);
		} else {
			iod = nvme_map_user_pages(dev, kv_nvme_is_write(pthr_cmd->opcode), (unsigned long)ubuffer, bufflen);
		}
		if (IS_ERR(iod)) {
			ret = -ENOMEM;
			goto free_out;
		}
		length = nvme_setup_kv_prps(dev, &cmd->common, iod, bufflen, GFP_KERNEL);
		if (length != bufflen) {
			ret = -ENOMEM;
			goto out_unmap;
		}
		if (aio) {
			aiocb->iod = iod;
			aiocb->kv_data = kv_data;
			aiocb->user_ctx = user_ctx;
		}
	}
	if (meta_buffer && meta_len) {
		int offset = 0, len = 0;
		if (!check_for_single_phyaddress(meta_buffer, meta_len)) {
			len = DIV_ROUND_UP(meta_len, 256)*256;
			meta = kmalloc(len, GFP_KERNEL);
			if (copy_from_user(meta, meta_buffer, meta_len)) {
				ret = -EFAULT;
				goto out;
			}
			offset = offset_in_page(meta);
			p_page = virt_to_page(meta);
			page_cache_get(p_page);
		} else {
			ret = get_user_pages_fast((unsigned long)meta_buffer, 1, 1, &p_page);
			if (ret < 1) {
				ret = -ENOMEM;
				goto out; 
			}
			offset = offset_in_page(meta_buffer);
		}
		if (aio) {
			aiocb->use_meta = true;
			aiocb->meta = meta;
			meta_sg_ptr = &aiocb->meta_sg;
		} else {
			meta_sg_ptr = &meta_sg;
		}
		sg_init_table(meta_sg_ptr, 1);
		sg_set_page(meta_sg_ptr, p_page, meta_len, offset);
		sg_mark_end(meta_sg_ptr);
		if (!dma_map_sg(&dev->pci_dev->dev, meta_sg_ptr, 1, DMA_TO_DEVICE)) {
			ret = -ENOMEM;
			goto out_unmap;
		}
		cmd->kv_store.key_prp = cpu_to_le64(sg_dma_address(meta_sg_ptr)); 
		set_meta = true;
	}

	nvmeq = get_nvmeq(dev);
	/*
	 * Since nvme_submit_sync_cmd sleeps, we can't keep preemption
	 * disabled.  We may be preempted at any point, and be rescheduled
	 * to a different CPU.  That will cause cacheline bouncing, but no
	 * additional races since q_lock already protects against other CPUs.
	 */
	put_nvmeq(nvmeq);
	if (aio) {
		aiocb->event.ctxid = pthr_cmd->ctxid;
		aiocb->event.reqid = pthr_cmd->reqid;
		aiocb->opcode = cmd->common.opcode;
		ret = nvme_submit_async_kv_cmd(nvmeq, cmd, result, timeout, aiocb);
		if (ret < 0) {
			*status = ret;
			goto  out_unmap;
		}
		return 0;
	} else {
		ret = nvme_submit_sync_cmd(dev, smp_processor_id() + 1, cmd, result, timeout);
		*status = ret;
	}

    if (need_to_copy) {
		if ((is_kv_retrieve_cmd(cmd->common.opcode) && !ret) ||
              (is_kv_iter_read_cmd(cmd->common.opcode) && (!ret || ((ret& 0x00ff) == 0x0093)))) {
#if 0
            char *data = kv_data;
            pr_err("recevied data %c:%c:%c:%c: %c:%c:%c:%c.\n",
                    data[0], data[1], data[2], data[3],
                    data[4], data[5], data[6], data[7]);
#endif
			(void)sg_copy_from_buffer(user_ctx->sg, user_ctx->nents,
					kv_data, user_ctx->len);
        }
    }

out_unmap:
	if (iod) {
		nvme_unmap_user_pages(dev, kv_nvme_is_write(pthr_cmd->opcode), iod);
		nvme_free_iod(dev, iod);
	}
	if (p_page) {
		if (set_meta)
			dma_unmap_sg(&dev->pci_dev->dev, meta_sg_ptr, 1, DMA_TO_DEVICE);
		put_page(sg_page(meta_sg_ptr));
	}
free_out:
	if (user_ctx)  {
		int i = 0;
		for (i = 0; i < user_ctx->nents; i++)
			put_page(sg_page(&user_ctx->sg[i]));
		kfree(user_ctx);
	}
	if (kv_data) kfree(kv_data);
out:
	if (aio)
		mempool_free(aiocb, kaiocb_mempool);
	return ret;
}


static int nvme_user_kv_cmd(struct nvme_ns *ns, struct nvme_passthru_kv_cmd __user *ucmd, bool aio)
{
	struct nvme_passthru_kv_cmd cmd;
	struct nvme_command c;
	int status = 0;
	unsigned timeout = NVME_IO_TIMEOUT;
	void __user *metadata = NULL;
	unsigned meta_len = 0;
	unsigned option = 0;
	unsigned iter_handle = 0;

	if (copy_from_user(&cmd, ucmd, sizeof(cmd)))
		return -EFAULT;
	if (cmd.flags)
		return -EINVAL;
	if (!is_kv_cmd(cmd.opcode))
		return -EINVAL;

	memset(&c, 0, sizeof(c));
	c.common.opcode = cmd.opcode;
	c.common.flags = cmd.flags;
#ifdef KSID_SUPPORT
	c.common.nsid = cmd.cdw3;
#else
	c.common.nsid = cpu_to_le32(cmd.nsid);
#endif
	if (cmd.timeout_ms)
		timeout = msecs_to_jiffies(cmd.timeout_ms);

	switch(cmd.opcode) {
		case nvme_cmd_kv_store:
		case nvme_cmd_kv_append:
			option = cpu_to_le32(cmd.cdw4);
			c.kv_store.offset = cpu_to_le32(cmd.cdw5);
			/* validate key length */
			if (cmd.key_length >  KVCMD_MAX_KEY_SIZE) {
				cmd.result = KVS_ERR_VALUE;
				status = -EINVAL;
				goto exit;
			}
			c.kv_store.key_len = cpu_to_le32(cmd.key_length -1); /* key len -1 */
			c.kv_store.option = (option & 0xff);
            /* set value size */
            if (cmd.data_length % 4) {
                c.kv_store.value_len = cpu_to_le32((cmd.data_length >> 2) + 1);
                c.kv_store.invalid_byte = 4 - (cmd.data_length % 4);
            } else {
                c.kv_store.value_len = cpu_to_le32((cmd.data_length >> 2));
            }

			if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
				metadata = (void __user*)cmd.key_addr;
				meta_len = cmd.key_length;
			} else {
				memcpy(c.kv_store.key, cmd.key, cmd.key_length);
			}
			break;
		case nvme_cmd_kv_retrieve:
			option = cpu_to_le32(cmd.cdw4);
			c.kv_retrieve.offset = cpu_to_le32(cmd.cdw5);
			/* validate key length */
			if (cmd.key_length >  KVCMD_MAX_KEY_SIZE ||
					cmd.key_length < KVCMD_MIN_KEY_SIZE) {
				cmd.result = KVS_ERR_VALUE;
				status = -EINVAL;
				goto exit;
			}
			c.kv_retrieve.key_len = cpu_to_le32(cmd.key_length -1); /* key len - 1 */
			c.kv_retrieve.option = (option & 0xff);
			c.kv_retrieve.value_len = cpu_to_le32((cmd.data_length >> 2));

			if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
				metadata = (void __user*)cmd.key_addr;
				meta_len = cmd.key_length;
			} else {
				memcpy(c.kv_retrieve.key, cmd.key, cmd.key_length);
			}
			break;
		case nvme_cmd_kv_delete:
			option = cpu_to_le32(cmd.cdw4);
			/* validate key length */
			if (cmd.key_length >  KVCMD_MAX_KEY_SIZE ||
					cmd.key_length < KVCMD_MIN_KEY_SIZE) {
				cmd.result = KVS_ERR_VALUE;
				status = -EINVAL;
				goto exit;
			}
			c.kv_delete.key_len = cpu_to_le32(cmd.key_length -1);
			c.kv_delete.option = (option & 0xff);	
			if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
				metadata = (void __user *)cmd.key_addr;
				meta_len = cmd.key_length;
			} else {
				memcpy(c.kv_delete.key, cmd.key, cmd.key_length);
			}
			break;
		case nvme_cmd_kv_exist:
			option = cpu_to_le32(cmd.cdw4);
			/* validate key length */
			if (cmd.key_length >  KVCMD_MAX_KEY_SIZE ||
					cmd.key_length < KVCMD_MIN_KEY_SIZE) {
				cmd.result = KVS_ERR_VALUE;
				status = -EINVAL;
				goto exit;
			}
			c.kv_exist.key_len = cpu_to_le32(cmd.key_length -1);
			c.kv_exist.option = (option & 0xff);	
			if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
				metadata = (void __user *)cmd.key_addr;
				meta_len = cmd.key_length;
			} else {
				memcpy(c.kv_exist.key, cmd.key, cmd.key_length);
			}
			break;

		case nvme_cmd_kv_iter_req:
			option = cpu_to_le32(cmd.cdw4);
			iter_handle = cpu_to_le32(cmd.cdw5);
			c.kv_iter_req.iter_handle = iter_handle & 0xff;
			c.kv_iter_req.option = option & 0xff;
			c.kv_iter_req.iter_val = cpu_to_le32(cmd.cdw12);
			c.kv_iter_req.iter_bitmask = cpu_to_le32(cmd.cdw13);
			break;
		case nvme_cmd_kv_iter_read:
			option = cpu_to_le32(cmd.cdw4);
			iter_handle = cpu_to_le32(cmd.cdw5);
			c.kv_iter_read.iter_handle = iter_handle & 0xff;
			c.kv_iter_read.option = option & 0xff;
			c.kv_iter_read.value_len = cpu_to_le32((cmd.data_length >> 2));
			break;
		default:
			cmd.result = KVS_ERR_IO;
			status = -EINVAL;
			goto exit;
	}

	status = __nvme_submit_kv_user_cmd(ns, &c, &cmd,
			(void __user *)(uintptr_t)cmd.data_addr, cmd.data_length, metadata, meta_len,
			&cmd.result, &cmd.status, timeout, aio);
exit:
	if (!aio) {
		if (put_user(cmd.result, &ucmd->result))
			return -EFAULT;
		if (put_user(cmd.status, &ucmd->status))
			return -EFAULT;
	}
	return status;
}

static int nvme_ioctl(struct block_device *bdev, fmode_t mode, unsigned int cmd,
							unsigned long arg)
{
	struct nvme_ns *ns = bdev->bd_disk->private_data;

	switch (cmd) {
	case NVME_IOCTL_ID:
		force_successful_syscall_return();
		return ns->ns_id;
	case NVME_IOCTL_ADMIN_CMD:
		return nvme_user_admin_cmd(ns->dev, (void __user *)arg);
	case NVME_IOCTL_SUBMIT_IO:
		return nvme_submit_io(ns, (void __user *)arg);
	case SG_GET_VERSION_NUM:
		return nvme_sg_get_version_num((void __user *)arg);
	case SG_IO:
		return nvme_sg_io(ns, (void __user *)arg);
	/*
	 * kv device ioctl.   
	 */
	case NVME_IOCTL_AIO_CMD:
		return nvme_user_kv_cmd(ns, (void __user *)arg, true);
	case NVME_IOCTL_IO_KV_CMD:
		return nvme_user_kv_cmd(ns, (void __user *)arg, false);
	case NVME_IOCTL_SET_AIOCTX:
		return nvme_set_aioctx((void __user *)arg);
	case NVME_IOCTL_DEL_AIOCTX:
		return nvme_del_aioctx((void __user *)arg);
	case NVME_IOCTL_GET_AIOEVENT:
		return nvme_get_aioevents((void __user *)arg);
	default:
		return -ENOTTY;
	}
}

#ifdef CONFIG_COMPAT
static int nvme_compat_ioctl(struct block_device *bdev, fmode_t mode,
					unsigned int cmd, unsigned long arg)
{
	struct nvme_ns *ns = bdev->bd_disk->private_data;

	switch (cmd) {
	case SG_IO:
		return nvme_sg_io32(ns, arg);
	}
	return nvme_ioctl(bdev, mode, cmd, arg);
}
#else
#define nvme_compat_ioctl	NULL
#endif

static int nvme_open(struct block_device *bdev, fmode_t mode)
{
	struct nvme_ns *ns = bdev->bd_disk->private_data;
	struct nvme_dev *dev = ns->dev;

	kref_get(&dev->kref);
	return 0;
}

static void nvme_free_dev(struct kref *kref);

static void nvme_release(struct gendisk *disk, fmode_t mode)
{
	struct nvme_ns *ns = disk->private_data;
	struct nvme_dev *dev = ns->dev;

	kref_put(&dev->kref, nvme_free_dev);
}

static int nvme_getgeo(struct block_device *bd, struct hd_geometry *geo)
{
	/* some standard values */
	geo->heads = 1 << 6;
	geo->sectors = 1 << 5;
	geo->cylinders = get_capacity(bd->bd_disk) >> 11;
	return 0;
}

static const struct block_device_operations nvme_fops = {
	.owner		= THIS_MODULE,
	.ioctl		= nvme_ioctl,
	.compat_ioctl	= nvme_compat_ioctl,
	.open		= nvme_open,
	.release	= nvme_release,
	.getgeo		= nvme_getgeo,
};

static void nvme_resubmit_iods(struct nvme_queue *nvmeq)
{
	struct nvme_iod *iod, *next;

	list_for_each_entry_safe(iod, next, &nvmeq->iod_bio, node) {
		if (unlikely(nvme_submit_iod(nvmeq, iod)))
			break;
		list_del(&iod->node);
		if (bio_list_empty(&nvmeq->sq_cong) &&
						list_empty(&nvmeq->iod_bio))
			remove_wait_queue(&nvmeq->sq_full,
						&nvmeq->sq_cong_wait);
	}
}

static void nvme_resubmit_bios(struct nvme_queue *nvmeq)
{
	while (bio_list_peek(&nvmeq->sq_cong)) {
		struct bio *bio = bio_list_pop(&nvmeq->sq_cong);
		struct nvme_ns *ns = bio->bi_bdev->bd_disk->private_data;

		if (bio_list_empty(&nvmeq->sq_cong) &&
						list_empty(&nvmeq->iod_bio))
			remove_wait_queue(&nvmeq->sq_full,
							&nvmeq->sq_cong_wait);
		if (nvme_submit_bio_queue(nvmeq, ns, bio)) {
			if (!waitqueue_active(&nvmeq->sq_full))
				add_wait_queue(&nvmeq->sq_full,
							&nvmeq->sq_cong_wait);
			bio_list_add_head(&nvmeq->sq_cong, bio);
			break;
		}
	}
}

static int nvme_kthread(void *data)
{
	struct nvme_dev *dev, *next;

	while (!kthread_should_stop()) {
		set_current_state(TASK_INTERRUPTIBLE);
		spin_lock(&dev_list_lock);
		list_for_each_entry_safe(dev, next, &dev_list, node) {
			int i;
			if (readl(&dev->bar->csts) & NVME_CSTS_CFS &&
							dev->initialized) {
				if (work_busy(&dev->reset_work))
					continue;
				list_del_init(&dev->node);
				dev_warn(&dev->pci_dev->dev,
					"Failed status, reset controller\n");
				PREPARE_WORK(&dev->reset_work,
							nvme_reset_failed_dev);
				queue_work(nvme_workq, &dev->reset_work);
				continue;
			}
			rcu_read_lock();
			for (i = 0; i < dev->queue_count; i++) {
				struct nvme_queue *nvmeq =
						rcu_dereference(dev->queues[i]);
				if (!nvmeq)
					continue;
				spin_lock_irq(&nvmeq->q_lock);
				if (nvmeq->q_suspended)
					goto unlock;
				nvme_process_cq(nvmeq);
				nvme_cancel_ios(nvmeq, true);
				nvme_resubmit_bios(nvmeq);
				nvme_resubmit_iods(nvmeq);
 unlock:
				spin_unlock_irq(&nvmeq->q_lock);
			}
			rcu_read_unlock();
		}
		spin_unlock(&dev_list_lock);
		schedule_timeout(round_jiffies_relative(HZ));
	}
	return 0;
}

static void nvme_config_discard(struct nvme_ns *ns)
{
	u32 logical_block_size = queue_logical_block_size(ns->queue);
	ns->queue->limits.discard_zeroes_data = 0;
	ns->queue->limits.discard_alignment = logical_block_size;
	ns->queue->limits.discard_granularity = logical_block_size;
	ns->queue->limits.max_discard_sectors = 0xffffffff;
	queue_flag_set_unlocked(QUEUE_FLAG_DISCARD, ns->queue);
}

static struct nvme_ns *nvme_alloc_ns(struct nvme_dev *dev, unsigned nsid,
			struct nvme_id_ns *id, struct nvme_lba_range_type *rt)
{
	struct nvme_ns *ns;
	struct gendisk *disk;
	int lbaf;

	if (rt->attributes & NVME_LBART_ATTRIB_HIDE)
		return NULL;

	ns = kzalloc(sizeof(*ns), GFP_KERNEL);
	if (!ns)
		return NULL;
	ns->queue = blk_alloc_queue(GFP_KERNEL);
	if (!ns->queue)
		goto out_free_ns;
	ns->queue->queue_flags = QUEUE_FLAG_DEFAULT;
	queue_flag_clear_unlocked(QUEUE_FLAG_STACKABLE, ns->queue);
	queue_flag_set_unlocked(QUEUE_FLAG_NOMERGES, ns->queue);
	queue_flag_set_unlocked(QUEUE_FLAG_NONROT, ns->queue);
	queue_flag_clear_unlocked(QUEUE_FLAG_ADD_RANDOM, ns->queue);
	blk_queue_make_request(ns->queue, nvme_make_request);
	ns->dev = dev;
	ns->queue->queuedata = ns;

	disk = alloc_disk(0);
	if (!disk)
		goto out_free_queue;
	ns->ns_id = nsid;
	ns->disk = disk;
	lbaf = id->flbas & 0xf;
	ns->lba_shift = id->lbaf[lbaf].ds;
	ns->ms = le16_to_cpu(id->lbaf[lbaf].ms);
	blk_queue_logical_block_size(ns->queue, 1 << ns->lba_shift);
	if (dev->max_hw_sectors)
		blk_queue_max_hw_sectors(ns->queue, dev->max_hw_sectors);
	if (dev->vwc & NVME_CTRL_VWC_PRESENT)
		blk_queue_flush(ns->queue, REQ_FLUSH | REQ_FUA);

	disk->major = nvme_major;
	disk->first_minor = 0;
	disk->fops = &nvme_fops;
	disk->private_data = ns;
	disk->queue = ns->queue;
	disk->driverfs_dev = &dev->pci_dev->dev;
	disk->flags = GENHD_FL_EXT_DEVT;
	sprintf(disk->disk_name, "nvme%dn%d", dev->instance, nsid);
	set_capacity(disk, le64_to_cpup(&id->nsze) << (ns->lba_shift - 9));

	if (dev->oncs & NVME_CTRL_ONCS_DSM)
		nvme_config_discard(ns);

	return ns;

 out_free_queue:
	blk_cleanup_queue(ns->queue);
 out_free_ns:
	kfree(ns);
	return NULL;
}

static int nvme_find_closest_node(int node)
{
	int n, val, min_val = INT_MAX, best_node = node;

	for_each_online_node(n) {
		if (n == node)
			continue;
		val = node_distance(node, n);
		if (val < min_val) {
			min_val = val;
			best_node = n;
		}
	}
	return best_node;
}

static void nvme_set_queue_cpus(cpumask_t *qmask, struct nvme_queue *nvmeq,
								int count)
{
	int cpu;
	for_each_cpu(cpu, qmask) {
		if (cpumask_weight(nvmeq->cpu_mask) >= count)
			break;
		if (!cpumask_test_and_set_cpu(cpu, nvmeq->cpu_mask))
			*per_cpu_ptr(nvmeq->dev->io_queue, cpu) = nvmeq->qid;
	}
}

static void nvme_add_cpus(cpumask_t *mask, const cpumask_t *unassigned_cpus,
	const cpumask_t *new_mask, struct nvme_queue *nvmeq, int cpus_per_queue)
{
	int next_cpu;
	for_each_cpu(next_cpu, new_mask) {
		cpumask_or(mask, mask, get_cpu_mask(next_cpu));
		cpumask_or(mask, mask, topology_thread_cpumask(next_cpu));
		cpumask_and(mask, mask, unassigned_cpus);
		nvme_set_queue_cpus(mask, nvmeq, cpus_per_queue);
	}
}

static void nvme_create_io_queues(struct nvme_dev *dev)
{
	unsigned i, max;

	max = min(dev->max_qid, num_online_cpus());
	for (i = dev->queue_count; i <= max; i++)
		if (!nvme_alloc_queue(dev, i, dev->q_depth, i - 1))
			break;

	max = min(dev->queue_count - 1, num_online_cpus());
	for (i = dev->online_queues; i <= max; i++)
		if (nvme_create_queue(raw_nvmeq(dev, i), i))
			break;
}

/*
 * If there are fewer queues than online cpus, this will try to optimally
 * assign a queue to multiple cpus by grouping cpus that are "close" together:
 * thread siblings, core, socket, closest node, then whatever else is
 * available.
 */
static void nvme_assign_io_queues(struct nvme_dev *dev)
{
	unsigned cpu, cpus_per_queue, queues, remainder, i;
	cpumask_var_t unassigned_cpus;

	nvme_create_io_queues(dev);

	queues = min(dev->online_queues - 1, num_online_cpus());
	if (!queues)
		return;

	cpus_per_queue = num_online_cpus() / queues;
	remainder = queues - (num_online_cpus() - queues * cpus_per_queue);

	if (!alloc_cpumask_var(&unassigned_cpus, GFP_KERNEL))
		return;

	cpumask_copy(unassigned_cpus, cpu_online_mask);
	cpu = cpumask_first(unassigned_cpus);
	for (i = 1; i <= queues; i++) {
		struct nvme_queue *nvmeq = lock_nvmeq(dev, i);
		cpumask_t mask;

		cpumask_clear(nvmeq->cpu_mask);
		if (!cpumask_weight(unassigned_cpus)) {
			unlock_nvmeq(nvmeq);
			break;
		}

		mask = *get_cpu_mask(cpu);
		nvme_set_queue_cpus(&mask, nvmeq, cpus_per_queue);
		if (cpus_weight(mask) < cpus_per_queue)
			nvme_add_cpus(&mask, unassigned_cpus,
				topology_thread_cpumask(cpu),
				nvmeq, cpus_per_queue);
		if (cpus_weight(mask) < cpus_per_queue)
			nvme_add_cpus(&mask, unassigned_cpus,
				topology_core_cpumask(cpu),
				nvmeq, cpus_per_queue);
		if (cpus_weight(mask) < cpus_per_queue)
			nvme_add_cpus(&mask, unassigned_cpus,
				cpumask_of_node(cpu_to_node(cpu)),
				nvmeq, cpus_per_queue);
		if (cpus_weight(mask) < cpus_per_queue)
			nvme_add_cpus(&mask, unassigned_cpus,
				cpumask_of_node(
					nvme_find_closest_node(
						cpu_to_node(cpu))),
				nvmeq, cpus_per_queue);
		if (cpus_weight(mask) < cpus_per_queue)
			nvme_add_cpus(&mask, unassigned_cpus,
				unassigned_cpus,
				nvmeq, cpus_per_queue);

		WARN(cpumask_weight(nvmeq->cpu_mask) != cpus_per_queue,
			"nvme%d qid:%d mis-matched queue-to-cpu assignment\n",
			dev->instance, i);

		irq_set_affinity_hint(dev->entry[nvmeq->cq_vector].vector,
							nvmeq->cpu_mask);
		cpumask_andnot(unassigned_cpus, unassigned_cpus,
						nvmeq->cpu_mask);
		cpu = cpumask_next(cpu, unassigned_cpus);
		if (remainder && !--remainder)
			cpus_per_queue++;
		unlock_nvmeq(nvmeq);
	}
	WARN(cpumask_weight(unassigned_cpus), "nvme%d unassigned online cpus\n",
								dev->instance);
	i = 0;
	cpumask_andnot(unassigned_cpus, cpu_possible_mask, cpu_online_mask);
	for_each_cpu(cpu, unassigned_cpus)
		*per_cpu_ptr(dev->io_queue, cpu) = (i++ % queues) + 1;
	free_cpumask_var(unassigned_cpus);
}

static int set_queue_count(struct nvme_dev *dev, int count)
{
	int status;
	u32 result;
	u32 q_count = (count - 1) | ((count - 1) << 16);

	status = nvme_set_features(dev, NVME_FEAT_NUM_QUEUES, q_count, 0,
								&result);
	if (status < 0)
		return status;
	if (status > 0) {
		dev_err(&dev->pci_dev->dev, "Could not set queue count (%d)\n",
									status);
		return -EBUSY;
	}
	return min(result & 0xffff, result >> 16) + 1;
}

static size_t db_bar_size(struct nvme_dev *dev, unsigned nr_io_queues)
{
	return 4096 + ((nr_io_queues + 1) * 8 * dev->db_stride);
}

static void nvme_cpu_workfn(struct work_struct *work)
{
	struct nvme_dev *dev = container_of(work, struct nvme_dev, cpu_work);
	if (dev->initialized)
		nvme_assign_io_queues(dev);
}

static int nvme_cpu_notify(struct notifier_block *self,
				unsigned long action, void *hcpu)
{
	struct nvme_dev *dev;

	switch (action) {
	case CPU_ONLINE:
	case CPU_DEAD:
		spin_lock(&dev_list_lock);
		list_for_each_entry(dev, &dev_list, node)
			schedule_work(&dev->cpu_work);
		spin_unlock(&dev_list_lock);
		break;
	}
	return NOTIFY_OK;
}

static int nvme_setup_io_queues(struct nvme_dev *dev)
{
	struct nvme_queue *adminq = raw_nvmeq(dev, 0);
	struct pci_dev *pdev = dev->pci_dev;
	int result, i, vecs, nr_io_queues, size;

	nr_io_queues = num_possible_cpus();
	result = set_queue_count(dev, nr_io_queues);
	if (result < 0)
		return result;
	if (result < nr_io_queues)
		nr_io_queues = result;

	size = db_bar_size(dev, nr_io_queues);
	if (size > 8192) {
		iounmap(dev->bar);
		do {
			dev->bar = ioremap(pci_resource_start(pdev, 0), size);
			if (dev->bar)
				break;
			if (!--nr_io_queues)
				return -ENOMEM;
			size = db_bar_size(dev, nr_io_queues);
		} while (1);
		dev->dbs = ((void __iomem *)dev->bar) + 4096;
		adminq->q_db = dev->dbs;
	}

	/* Deregister the admin queue's interrupt */
	free_irq(dev->entry[0].vector, adminq);

	vecs = nr_io_queues;
	for (i = 0; i < vecs; i++)
		dev->entry[i].entry = i;
	for (;;) {
		result = pci_enable_msix(pdev, dev->entry, vecs);
		if (result <= 0)
			break;
		vecs = result;
	}

	if (result < 0) {
		vecs = nr_io_queues;
		if (vecs > 32)
			vecs = 32;
		for (;;) {
			result = pci_enable_msi_block(pdev, vecs);
			if (result == 0) {
				for (i = 0; i < vecs; i++)
					dev->entry[i].vector = i + pdev->irq;
				break;
			} else if (result < 0) {
				vecs = 1;
				break;
			}
			vecs = result;
		}
	}

	/*
	 * Should investigate if there's a performance win from allocating
	 * more queues than interrupt vectors; it might allow the submission
	 * path to scale better, even if the receive path is limited by the
	 * number of interrupts.
	 */
	nr_io_queues = vecs;
	dev->max_qid = nr_io_queues;

	result = queue_request_irq(dev, adminq, adminq->irqname);
	if (result) {
		adminq->q_suspended = 1;
		goto free_queues;
	}

	/* Free previously allocated queues that are no longer usable */
	nvme_free_queues(dev, nr_io_queues + 1);
	nvme_assign_io_queues(dev);

	return 0;

 free_queues:
	nvme_free_queues(dev, 1);
	return result;
}

/*
 * Return: error value if an error occurred setting up the queues or calling
 * Identify Device.  0 if these succeeded, even if adding some of the
 * namespaces failed.  At the moment, these failures are silent.  TBD which
 * failures should be reported.
 */
static int nvme_dev_add(struct nvme_dev *dev)
{
	struct pci_dev *pdev = dev->pci_dev;
	int res;
	unsigned nn, i;
	struct nvme_ns *ns;
	struct nvme_id_ctrl *ctrl;
	struct nvme_id_ns *id_ns;
	void *mem;
	dma_addr_t dma_addr;
	int shift = NVME_CAP_MPSMIN(readq(&dev->bar->cap)) + 12;

	mem = dma_alloc_coherent(&pdev->dev, 8192, &dma_addr, GFP_KERNEL);
	if (!mem)
		return -ENOMEM;

	res = nvme_identify(dev, 0, 1, dma_addr);
	if (res) {
		dev_err(&pdev->dev, "Identify Controller failed (%d)\n", res);
		res = -EIO;
		goto out;
	}

	ctrl = mem;
	nn = le32_to_cpup(&ctrl->nn);
	dev->oncs = le16_to_cpup(&ctrl->oncs);
	dev->abort_limit = ctrl->acl + 1;
	dev->vwc = ctrl->vwc;
	memcpy(dev->serial, ctrl->sn, sizeof(ctrl->sn));
	memcpy(dev->model, ctrl->mn, sizeof(ctrl->mn));
	memcpy(dev->firmware_rev, ctrl->fr, sizeof(ctrl->fr));
	if (ctrl->mdts)
		dev->max_hw_sectors = 1 << (ctrl->mdts + shift - 9);
	if ((pdev->vendor == PCI_VENDOR_ID_INTEL) &&
			(pdev->device == 0x0953) && ctrl->vs[3])
		dev->stripe_size = 1 << (ctrl->vs[3] + shift);

	id_ns = mem;
	for (i = 1; i <= nn; i++) {
		res = nvme_identify(dev, i, 0, dma_addr);
		if (res)
			continue;

		if (id_ns->ncap == 0)
			continue;

		res = nvme_get_features(dev, NVME_FEAT_LBA_RANGE, i,
							dma_addr + 4096, NULL);
		if (res)
			memset(mem + 4096, 0, 4096);

		ns = nvme_alloc_ns(dev, i, mem, mem + 4096);
		if (ns)
			list_add_tail(&ns->list, &dev->namespaces);
	}
	list_for_each_entry(ns, &dev->namespaces, list)
		add_disk(ns->disk);
	res = 0;

 out:
	dma_free_coherent(&dev->pci_dev->dev, 8192, mem, dma_addr);
	return res;
}

static int nvme_dev_map(struct nvme_dev *dev)
{
	u64 cap;
	int bars, result = -ENOMEM;
	struct pci_dev *pdev = dev->pci_dev;

	if (pci_enable_device_mem(pdev))
		return result;

	dev->entry[0].vector = pdev->irq;
	pci_set_master(pdev);
	bars = pci_select_bars(pdev, IORESOURCE_MEM);
	if (pci_request_selected_regions(pdev, bars, "nvme"))
		goto disable_pci;

	if (dma_set_mask_and_coherent(&pdev->dev, DMA_BIT_MASK(64)) &&
	    dma_set_mask_and_coherent(&pdev->dev, DMA_BIT_MASK(32)))
		goto disable;

	dev->bar = ioremap(pci_resource_start(pdev, 0), 8192);
	if (!dev->bar)
		goto disable;
	if (readl(&dev->bar->csts) == -1) {
		result = -ENODEV;
		goto unmap;
	}
	cap = readq(&dev->bar->cap);
	dev->q_depth = min_t(int, NVME_CAP_MQES(cap) + 1, NVME_Q_DEPTH);
	dev->db_stride = 1 << NVME_CAP_STRIDE(cap);
	dev->dbs = ((void __iomem *)dev->bar) + 4096;

	return 0;

 unmap:
	iounmap(dev->bar);
	dev->bar = NULL;
 disable:
	pci_release_regions(pdev);
 disable_pci:
	pci_disable_device(pdev);
	return result;
}

static void nvme_dev_unmap(struct nvme_dev *dev)
{
	if (dev->pci_dev->msi_enabled)
		pci_disable_msi(dev->pci_dev);
	else if (dev->pci_dev->msix_enabled)
		pci_disable_msix(dev->pci_dev);

	if (dev->bar) {
		iounmap(dev->bar);
		dev->bar = NULL;
		pci_release_regions(dev->pci_dev);
	}

	if (pci_is_enabled(dev->pci_dev))
		pci_disable_device(dev->pci_dev);
}

struct nvme_delq_ctx {
	struct task_struct *waiter;
	struct kthread_worker* worker;
	atomic_t refcount;
};

static void nvme_wait_dq(struct nvme_delq_ctx *dq, struct nvme_dev *dev)
{
	dq->waiter = current;
	mb();

	for (;;) {
		set_current_state(TASK_KILLABLE);
		if (!atomic_read(&dq->refcount))
			break;
		if (!schedule_timeout(ADMIN_TIMEOUT) ||
					fatal_signal_pending(current)) {
			set_current_state(TASK_RUNNING);

			nvme_disable_ctrl(dev, readq(&dev->bar->cap));
			nvme_disable_queue(dev, 0);

			send_sig(SIGKILL, dq->worker->task, 1);
			flush_kthread_worker(dq->worker);
			return;
		}
	}
	set_current_state(TASK_RUNNING);
}

static void nvme_put_dq(struct nvme_delq_ctx *dq)
{
	atomic_dec(&dq->refcount);
	if (dq->waiter)
		wake_up_process(dq->waiter);
}

static struct nvme_delq_ctx *nvme_get_dq(struct nvme_delq_ctx *dq)
{
	atomic_inc(&dq->refcount);
	return dq;
}

static void nvme_del_queue_end(struct nvme_queue *nvmeq)
{
	struct nvme_delq_ctx *dq = nvmeq->cmdinfo.ctx;

	nvme_clear_queue(nvmeq);
	nvme_put_dq(dq);
}

static int adapter_async_del_queue(struct nvme_queue *nvmeq, u8 opcode,
						kthread_work_func_t fn)
{
	struct nvme_command c;

	memset(&c, 0, sizeof(c));
	c.delete_queue.opcode = opcode;
	c.delete_queue.qid = cpu_to_le16(nvmeq->qid);

	init_kthread_work(&nvmeq->cmdinfo.work, fn);
	return nvme_submit_admin_cmd_async(nvmeq->dev, &c, &nvmeq->cmdinfo);
}

static void nvme_del_cq_work_handler(struct kthread_work *work)
{
	struct nvme_queue *nvmeq = container_of(work, struct nvme_queue,
							cmdinfo.work);
	nvme_del_queue_end(nvmeq);
}

static int nvme_delete_cq(struct nvme_queue *nvmeq)
{
	return adapter_async_del_queue(nvmeq, nvme_admin_delete_cq,
						nvme_del_cq_work_handler);
}

static void nvme_del_sq_work_handler(struct kthread_work *work)
{
	struct nvme_queue *nvmeq = container_of(work, struct nvme_queue,
							cmdinfo.work);
	int status = nvmeq->cmdinfo.status;

	if (!status)
		status = nvme_delete_cq(nvmeq);
	if (status)
		nvme_del_queue_end(nvmeq);
}

static int nvme_delete_sq(struct nvme_queue *nvmeq)
{
	return adapter_async_del_queue(nvmeq, nvme_admin_delete_sq,
						nvme_del_sq_work_handler);
}

static void nvme_del_queue_start(struct kthread_work *work)
{
	struct nvme_queue *nvmeq = container_of(work, struct nvme_queue,
							cmdinfo.work);
	allow_signal(SIGKILL);
	if (nvme_delete_sq(nvmeq))
		nvme_del_queue_end(nvmeq);
}

static void nvme_disable_io_queues(struct nvme_dev *dev)
{
	int i;
	DEFINE_KTHREAD_WORKER_ONSTACK(worker);
	struct nvme_delq_ctx dq;
	struct task_struct *kworker_task = kthread_run(kthread_worker_fn,
					&worker, "nvme%d", dev->instance);

	if (IS_ERR(kworker_task)) {
		dev_err(&dev->pci_dev->dev,
			"Failed to create queue del task\n");
		for (i = dev->queue_count - 1; i > 0; i--)
			nvme_disable_queue(dev, i);
		return;
	}

	dq.waiter = NULL;
	atomic_set(&dq.refcount, 0);
	dq.worker = &worker;
	for (i = dev->queue_count - 1; i > 0; i--) {
		struct nvme_queue *nvmeq = raw_nvmeq(dev, i);

		if (nvme_suspend_queue(nvmeq))
			continue;
		nvmeq->cmdinfo.ctx = nvme_get_dq(&dq);
		nvmeq->cmdinfo.worker = dq.worker;
		init_kthread_work(&nvmeq->cmdinfo.work, nvme_del_queue_start);
		queue_kthread_work(dq.worker, &nvmeq->cmdinfo.work);
	}
	nvme_wait_dq(&dq, dev);
	kthread_stop(kworker_task);
}

/*
* Remove the node from the device list and check
* for whether or not we need to stop the nvme_thread.
*/
static void nvme_dev_list_remove(struct nvme_dev *dev)
{
	struct task_struct *tmp = NULL;

	spin_lock(&dev_list_lock);
	list_del_init(&dev->node);
	if (list_empty(&dev_list) && !IS_ERR_OR_NULL(nvme_thread)) {
		tmp = nvme_thread;
		nvme_thread = NULL;
	}
	spin_unlock(&dev_list_lock);

	if (tmp)
		kthread_stop(tmp);
}

static void nvme_dev_shutdown(struct nvme_dev *dev)
{
	int i;

	dev->initialized = 0;
	nvme_dev_list_remove(dev);

	if (!dev->bar || (dev->bar && readl(&dev->bar->csts) == -1)) {
		for (i = dev->queue_count - 1; i >= 0; i--) {
			struct nvme_queue *nvmeq = raw_nvmeq(dev, i);
			nvme_suspend_queue(nvmeq);
			nvme_clear_queue(nvmeq);
		}
	} else {
		nvme_disable_io_queues(dev);
		nvme_shutdown_ctrl(dev);
		nvme_disable_queue(dev, 0);
	}
	nvme_dev_unmap(dev);
}

static void nvme_dev_remove(struct nvme_dev *dev)
{
	struct nvme_ns *ns;

	list_for_each_entry(ns, &dev->namespaces, list) {
		if (ns->disk->flags & GENHD_FL_UP)
			del_gendisk(ns->disk);
		if (!blk_queue_dying(ns->queue))
			blk_cleanup_queue(ns->queue);
	}
}

static int nvme_setup_prp_pools(struct nvme_dev *dev)
{
	struct device *dmadev = &dev->pci_dev->dev;
	dev->prp_page_pool = dma_pool_create("prp list page", dmadev,
						PAGE_SIZE, PAGE_SIZE, 0);
	if (!dev->prp_page_pool)
		return -ENOMEM;

	/* Optimisation for I/Os between 4k and 128k */
	dev->prp_small_pool = dma_pool_create("prp list 256", dmadev,
						256, 256, 0);
	if (!dev->prp_small_pool) {
		dma_pool_destroy(dev->prp_page_pool);
		return -ENOMEM;
	}
	return 0;
}

static void nvme_release_prp_pools(struct nvme_dev *dev)
{
	dma_pool_destroy(dev->prp_page_pool);
	dma_pool_destroy(dev->prp_small_pool);
}

static DEFINE_IDA(nvme_instance_ida);

static int nvme_set_instance(struct nvme_dev *dev)
{
	int instance, error;

	do {
		if (!ida_pre_get(&nvme_instance_ida, GFP_KERNEL))
			return -ENODEV;

		spin_lock(&dev_list_lock);
		error = ida_get_new(&nvme_instance_ida, &instance);
		spin_unlock(&dev_list_lock);
	} while (error == -EAGAIN);

	if (error)
		return -ENODEV;

	dev->instance = instance;
	return 0;
}

static void nvme_release_instance(struct nvme_dev *dev)
{
	spin_lock(&dev_list_lock);
	ida_remove(&nvme_instance_ida, dev->instance);
	spin_unlock(&dev_list_lock);
}

static void nvme_free_namespaces(struct nvme_dev *dev)
{
	struct nvme_ns *ns, *next;

	list_for_each_entry_safe(ns, next, &dev->namespaces, list) {
		list_del(&ns->list);
		put_disk(ns->disk);
		kfree(ns);
	}
}

static void nvme_free_dev(struct kref *kref)
{
	struct nvme_dev *dev = container_of(kref, struct nvme_dev, kref);

	nvme_free_namespaces(dev);
	free_percpu(dev->io_queue);
	kfree(dev->queues);
	kfree(dev->entry);
	kfree(dev);
}

static int nvme_dev_open(struct inode *inode, struct file *f)
{
	struct nvme_dev *dev = container_of(f->private_data, struct nvme_dev,
								miscdev);
	kref_get(&dev->kref);
	f->private_data = dev;
	return 0;
}

static int nvme_dev_release(struct inode *inode, struct file *f)
{
	struct nvme_dev *dev = f->private_data;
	kref_put(&dev->kref, nvme_free_dev);
	return 0;
}

static long nvme_dev_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
	struct nvme_dev *dev = f->private_data;
	switch (cmd) {
	case NVME_IOCTL_ADMIN_CMD:
		return nvme_user_admin_cmd(dev, (void __user *)arg);
	default:
		return -ENOTTY;
	}
}

static const struct file_operations nvme_dev_fops = {
	.owner		= THIS_MODULE,
	.open		= nvme_dev_open,
	.release	= nvme_dev_release,
	.unlocked_ioctl	= nvme_dev_ioctl,
	.compat_ioctl	= nvme_dev_ioctl,
};

static int nvme_dev_start(struct nvme_dev *dev)
{
	int result;
	bool start_thread = false;

	result = nvme_dev_map(dev);
	if (result)
		return result;

	result = nvme_configure_admin_queue(dev);
	if (result)
		goto unmap;

	spin_lock(&dev_list_lock);
	if (list_empty(&dev_list) && IS_ERR_OR_NULL(nvme_thread)) {
		start_thread = true;
		nvme_thread = NULL;
	}
	list_add(&dev->node, &dev_list);
	spin_unlock(&dev_list_lock);

	if (start_thread) {
		nvme_thread = kthread_run(nvme_kthread, NULL, "nvme");
		wake_up(&nvme_kthread_wait);
	} else
		wait_event_killable(nvme_kthread_wait, nvme_thread);

	if (IS_ERR_OR_NULL(nvme_thread)) {
		result = nvme_thread ? PTR_ERR(nvme_thread) : -EINTR;
		goto disable;
	}

	result = nvme_setup_io_queues(dev);
	if (result && result != -EBUSY)
		goto disable;

	return result;

 disable:
	nvme_disable_queue(dev, 0);
	nvme_dev_list_remove(dev);
 unmap:
	nvme_dev_unmap(dev);
	return result;
}

static int nvme_remove_dead_ctrl(void *arg)
{
	struct nvme_dev *dev = (struct nvme_dev *)arg;
	struct pci_dev *pdev = dev->pci_dev;

	if (pci_get_drvdata(pdev))
		pci_stop_and_remove_bus_device(pdev);
	kref_put(&dev->kref, nvme_free_dev);
	return 0;
}

static void nvme_remove_disks(struct work_struct *ws)
{
	struct nvme_dev *dev = container_of(ws, struct nvme_dev, reset_work);

	nvme_dev_remove(dev);
	nvme_free_queues(dev, 1);
}

static int nvme_dev_resume(struct nvme_dev *dev)
{
	int ret;

	ret = nvme_dev_start(dev);
	if (ret && ret != -EBUSY)
		return ret;
	if (ret == -EBUSY) {
		spin_lock(&dev_list_lock);
		PREPARE_WORK(&dev->reset_work, nvme_remove_disks);
		queue_work(nvme_workq, &dev->reset_work);
		spin_unlock(&dev_list_lock);
	}
	dev->initialized = 1;
	return 0;
}

static void nvme_dev_reset(struct nvme_dev *dev)
{
	nvme_dev_shutdown(dev);
	if (nvme_dev_resume(dev)) {
		dev_err(&dev->pci_dev->dev, "Device failed to resume\n");
		kref_get(&dev->kref);
		if (IS_ERR(kthread_run(nvme_remove_dead_ctrl, dev, "nvme%d",
							dev->instance))) {
			dev_err(&dev->pci_dev->dev,
				"Failed to start controller remove task\n");
			kref_put(&dev->kref, nvme_free_dev);
		}
	}
}

static void nvme_reset_failed_dev(struct work_struct *ws)
{
	struct nvme_dev *dev = container_of(ws, struct nvme_dev, reset_work);
	nvme_dev_reset(dev);
}

static int nvme_probe(struct pci_dev *pdev, const struct pci_device_id *id)
{
	int result = -ENOMEM;
	struct nvme_dev *dev;

	dev = kzalloc(sizeof(*dev), GFP_KERNEL);
	if (!dev)
		return -ENOMEM;
	dev->entry = kcalloc(num_possible_cpus(), sizeof(*dev->entry),
								GFP_KERNEL);
	if (!dev->entry)
		goto free;
	dev->queues = kcalloc(num_possible_cpus() + 1, sizeof(void *),
								GFP_KERNEL);
	if (!dev->queues)
		goto free;
	dev->io_queue = alloc_percpu(unsigned short);
	if (!dev->io_queue)
		goto free;

	INIT_LIST_HEAD(&dev->namespaces);
	INIT_WORK(&dev->reset_work, nvme_reset_failed_dev);
	INIT_WORK(&dev->cpu_work, nvme_cpu_workfn);
	dev->pci_dev = pdev;
	pci_set_drvdata(pdev, dev);
	result = nvme_set_instance(dev);
	if (result)
		goto free;

	result = nvme_setup_prp_pools(dev);
	if (result)
		goto release;

	kref_init(&dev->kref);
	result = nvme_dev_start(dev);
	if (result) {
		if (result == -EBUSY)
			goto create_cdev;
		goto release_pools;
	}

	result = nvme_dev_add(dev);
	if (result)
		goto shutdown;

 create_cdev:
	scnprintf(dev->name, sizeof(dev->name), "nvme%d", dev->instance);
	dev->miscdev.minor = MISC_DYNAMIC_MINOR;
	dev->miscdev.parent = &pdev->dev;
	dev->miscdev.name = dev->name;
	dev->miscdev.fops = &nvme_dev_fops;
	result = misc_register(&dev->miscdev);
	if (result)
		goto remove;

	dev->initialized = 1;
	return 0;

 remove:
	nvme_dev_remove(dev);
	nvme_free_namespaces(dev);
 shutdown:
	nvme_dev_shutdown(dev);
 release_pools:
	nvme_free_queues(dev, 0);
	nvme_release_prp_pools(dev);
 release:
	nvme_release_instance(dev);
 free:
	free_percpu(dev->io_queue);
	kfree(dev->queues);
	kfree(dev->entry);
	kfree(dev);
	return result;
}

static void nvme_shutdown(struct pci_dev *pdev)
{
	struct nvme_dev *dev = pci_get_drvdata(pdev);
	nvme_dev_shutdown(dev);
}

static void nvme_remove(struct pci_dev *pdev)
{
	struct nvme_dev *dev = pci_get_drvdata(pdev);

	spin_lock(&dev_list_lock);
	list_del_init(&dev->node);
	spin_unlock(&dev_list_lock);

	pci_set_drvdata(pdev, NULL);
	flush_work(&dev->reset_work);
	flush_work(&dev->cpu_work);
	misc_deregister(&dev->miscdev);
	nvme_dev_remove(dev);
	nvme_dev_shutdown(dev);
	nvme_free_queues(dev, 0);
	rcu_barrier();
	nvme_release_instance(dev);
	nvme_release_prp_pools(dev);
	kref_put(&dev->kref, nvme_free_dev);
}

/* These functions are yet to be implemented */
#define nvme_error_detected NULL
#define nvme_dump_registers NULL
#define nvme_link_reset NULL
#define nvme_slot_reset NULL
#define nvme_error_resume NULL

static int nvme_suspend(struct device *dev)
{
	struct pci_dev *pdev = to_pci_dev(dev);
	struct nvme_dev *ndev = pci_get_drvdata(pdev);

	nvme_dev_shutdown(ndev);
	return 0;
}

static int nvme_resume(struct device *dev)
{
	struct pci_dev *pdev = to_pci_dev(dev);
	struct nvme_dev *ndev = pci_get_drvdata(pdev);

	if (nvme_dev_resume(ndev) && !work_busy(&ndev->reset_work)) {
		PREPARE_WORK(&ndev->reset_work, nvme_reset_failed_dev);
		queue_work(nvme_workq, &ndev->reset_work);
	}
	return 0;
}

static SIMPLE_DEV_PM_OPS(nvme_dev_pm_ops, nvme_suspend, nvme_resume);

static const struct pci_error_handlers nvme_err_handler = {
	.error_detected	= nvme_error_detected,
	.mmio_enabled	= nvme_dump_registers,
	.link_reset	= nvme_link_reset,
	.slot_reset	= nvme_slot_reset,
	.resume		= nvme_error_resume,
};

/* Move to pci_ids.h later */
#define PCI_CLASS_STORAGE_EXPRESS	0x010802

static const struct pci_device_id nvme_id_table[] = {
	{ PCI_DEVICE_CLASS(PCI_CLASS_STORAGE_EXPRESS, 0xffffff) },
	{ 0, }
};
MODULE_DEVICE_TABLE(pci, nvme_id_table);

static struct pci_driver nvme_driver = {
	.name		= "nvme",
	.id_table	= nvme_id_table,
	.probe		= nvme_probe,
	.remove		= nvme_remove,
	.shutdown	= nvme_shutdown,
	.driver		= {
		.pm	= &nvme_dev_pm_ops,
	},
	.err_handler	= &nvme_err_handler,
};

static int __init nvme_init(void)
{
	int result;

	init_waitqueue_head(&nvme_kthread_wait);

	nvme_workq = create_singlethread_workqueue("nvme");
	if (!nvme_workq)
		return -ENOMEM;

	result = register_blkdev(nvme_major, "nvme");
	if (result < 0)
		goto kill_workq;
	else if (result > 0)
		nvme_major = result;

	nvme_nb.notifier_call = &nvme_cpu_notify;
	result = register_hotcpu_notifier(&nvme_nb);
	if (result)
		goto unregister_blkdev;

	result = pci_register_driver(&nvme_driver);
	if (result)
		goto unregister_hotcpu;

	result = aio_service_init();
	if (result)
		goto unregister_pcidev;
	return 0;

unregister_pcidev:
	pci_unregister_driver(&nvme_driver);
unregister_hotcpu:
	unregister_hotcpu_notifier(&nvme_nb);
unregister_blkdev:
	unregister_blkdev(nvme_major, "nvme");
kill_workq:
	destroy_workqueue(nvme_workq);
	return result;
}

static void __exit nvme_exit(void)
{
	pci_unregister_driver(&nvme_driver);
	unregister_hotcpu_notifier(&nvme_nb);
	unregister_blkdev(nvme_major, "nvme");
	destroy_workqueue(nvme_workq);
	BUG_ON(nvme_thread && !IS_ERR(nvme_thread));
	_nvme_check_size();
	aio_service_exit();
}

MODULE_AUTHOR("Matthew Wilcox <willy@linux.intel.com>");
MODULE_LICENSE("GPL");
MODULE_VERSION("0.9");
module_init(nvme_init);
module_exit(nvme_exit);
