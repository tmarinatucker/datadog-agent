#ifndef _UNLINK_H_
#define _UNLINK_H_

#include "syscalls.h"
#include "process.h"

struct unlink_event_t {
    struct kevent_t event;
    struct process_context_t process;
    struct container_context_t container;
    struct syscall_t syscall;
    struct file_t file;
    u32 flags;
    u32 padding;
};

int __attribute__((always_inline)) trace__sys_unlink(int flags) {
    struct syscall_cache_t syscall = {
        .type = SYSCALL_UNLINK,
        .unlink = {
            .flags = flags,
        }
    };

    cache_syscall(&syscall, EVENT_UNLINK);

    return 0;
}

SYSCALL_KPROBE0(unlink) {
    return trace__sys_unlink(0);
}

SYSCALL_KPROBE3(unlinkat, int, dirfd, const char*, filename, int, flags) {
    return trace__sys_unlink(flags);
}

SEC("kprobe/vfs_unlink")
int kprobe__vfs_unlink(struct pt_regs *ctx) {
    struct syscall_cache_t *syscall = peek_syscall(SYSCALL_UNLINK);
    if (!syscall)
        return 0;

    if (syscall->unlink.path_key.ino) {
        return 0;
    }

    // we resolve all the information before the file is actually removed
    struct dentry *dentry = (struct dentry *) PT_REGS_PARM2(ctx);

    u64 lower_inode = get_ovl_lower_ino(dentry);
    if (lower_inode) {
        syscall->unlink.ovl.vfs_lower_inode = lower_inode;
    }

    u64 upper_inode = get_ovl_upper_ino(dentry);
    if (upper_inode) {
        syscall->unlink.ovl.vfs_upper_inode = upper_inode;
    }

    syscall->unlink.path_key.ino = get_dentry_ino(dentry);
    syscall->unlink.overlay_numlower = get_overlay_numlower(dentry);

    if (!syscall->unlink.path_key.path_id)
        syscall->unlink.path_key.path_id = get_path_id(1);

    if (discarded_by_process(syscall->policy.mode, EVENT_UNLINK)) {
        pop_syscall(SYSCALL_UNLINK);
        return 0;
    }

    // the mount id of path_key is resolved by kprobe/mnt_want_write. It is already set by the time we reach this probe.
    int ret = resolve_dentry(dentry, syscall->unlink.path_key, syscall->policy.mode != NO_FILTER ? EVENT_UNLINK : 0);
    if (ret < 0) {
        pop_syscall(SYSCALL_UNLINK);
    }

    return 0;
}

int __attribute__((always_inline)) trace__sys_unlink_ret(struct pt_regs *ctx) {
    struct syscall_cache_t *syscall = pop_syscall(SYSCALL_UNLINK);
    if (!syscall)
        return 0;

    int retval = PT_REGS_RC(ctx);
    if (IS_UNHANDLED_ERROR(retval)) {
        return 0;
    }

    // use the inode retrieved in the vfs_unlink call
    u64 inode = syscall->unlink.path_key.ino;

    bpf_printk("trace__sys_unlink_ret: %d %d %d\n", syscall->unlink.ovl.lower_inode, syscall->unlink.ovl.upper_inode, syscall->unlink.ovl.real_inode);

    // set the entry dentry key
    if (syscall->unlink.ovl.vfs_lower_inode) {
        inode = syscall->unlink.ovl.vfs_lower_inode;
        link_dentry_inode(syscall->unlink.path_key, inode);   
    } else if (syscall->unlink.ovl.vfs_upper_inode) {
        inode = syscall->unlink.ovl.vfs_upper_inode;
        link_dentry_inode(syscall->unlink.path_key, inode);  
    }

    bpf_printk("trace__sys_unlink_ret: %d\n", inode);

    u64 enabled_events = get_enabled_events();
    int enabled = mask_has_event(enabled_events, EVENT_UNLINK) ||
                  mask_has_event(enabled_events, EVENT_RMDIR);
    if (enabled) {
        struct unlink_event_t event = {
            .event.type = syscall->unlink.flags&AT_REMOVEDIR ? EVENT_RMDIR : EVENT_UNLINK,
            .event.timestamp = bpf_ktime_get_ns(),
            .syscall.retval = retval,
            .file = {
                .mount_id = syscall->unlink.path_key.mount_id,
                .inode = inode,
                .overlay_numlower = syscall->unlink.overlay_numlower,
                .path_id = syscall->unlink.path_key.path_id,
            },
            .flags = syscall->unlink.flags,
        };

        struct proc_cache_t *entry = fill_process_context(&event.process);
        fill_container_context(entry, &event.container);

        send_event(ctx, event);
    }

    invalidate_inode(ctx, syscall->unlink.path_key.mount_id, inode, !enabled);

    return 0;
}

SYSCALL_KRETPROBE(unlink) {
    return trace__sys_unlink_ret(ctx);
}

SYSCALL_KRETPROBE(unlinkat) {
    return trace__sys_unlink_ret(ctx);
}

#endif
