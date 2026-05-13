//! UltraSQL storage engine.
//!
//! Layers (bottom-up):
//!
//! 1. Page (8 KiB, 64-bit aligned). On-disk format with header + slotted body.
//! 2. Segment / file manager (mmap + direct IO toggle).
//! 3. Buffer pool (CLOCK-Pro, sharded page table).
//! 4. Heap access method (PostgreSQL-style HOT-update-friendly tuple layout).
//! 5. B+ tree index access method.
//! 6. Free-space map + visibility map.

#![forbid(unsafe_op_in_unsafe_fn)]

pub mod checksum;
pub mod page;

pub use page::{ItemId, ItemIdFlags, Page, PageError, PageHeader, PageKind, SlotIndex};
