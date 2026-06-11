//! Checked tuple-header byte-field helpers for heap hot paths.

use std::ops::Range;

use ultrasql_core::{CommandId, TupleId, Xid};
use ultrasql_mvcc::tuple_header::TUPLE_HEADER_SIZE;

use crate::buffer_pool::PageLoader;

use super::{HeapAccess, HeapError};

const XMAX_OFFSET: usize = 8;
const CMAX_OFFSET: usize = 20;
const INFOMASK_OFFSET: usize = 24;
const N_ATTS_OFFSET: usize = 26;
const CTID_RELATION_OFFSET: usize = 32;
const CTID_BLOCK_SLOT_OFFSET: usize = 36;
const U16_WIDTH: usize = 2;
const U32_WIDTH: usize = 4;
const U64_WIDTH: usize = 8;
const BLOCK_NUMBER_MASK: u32 = 0x00FF_FFFF;
const CTID_SLOT_SHIFT: u32 = 24;

impl<L: PageLoader> HeapAccess<L> {
    #[inline]
    pub(super) fn tuple_header_range(slot_offset: usize) -> Result<Range<usize>, HeapError> {
        Self::tuple_field_range(
            slot_offset,
            0,
            TUPLE_HEADER_SIZE,
            "tuple header range overflow",
        )
    }

    #[inline]
    pub(super) fn tuple_field_range(
        slot_offset: usize,
        relative_offset: usize,
        width: usize,
        error: &'static str,
    ) -> Result<Range<usize>, HeapError> {
        let start = slot_offset
            .checked_add(relative_offset)
            .ok_or(HeapError::MalformedHeader(error))?;
        let end = start
            .checked_add(width)
            .ok_or(HeapError::MalformedHeader(error))?;
        Ok(start..end)
    }

    #[inline]
    pub(super) fn tuple_header_bytes<'a>(
        bytes: &'a [u8],
        slot_offset: usize,
        error: &'static str,
    ) -> Result<&'a [u8], HeapError> {
        let range = Self::tuple_header_range(slot_offset)?;
        bytes.get(range).ok_or(HeapError::MalformedHeader(error))
    }

    #[inline]
    pub(super) fn tuple_header_bytes_mut<'a>(
        bytes: &'a mut [u8],
        slot_offset: usize,
        error: &'static str,
    ) -> Result<&'a mut [u8], HeapError> {
        let range = Self::tuple_header_range(slot_offset)?;
        bytes
            .get_mut(range)
            .ok_or(HeapError::MalformedHeader(error))
    }

    #[inline]
    pub(super) fn read_xmax(bytes: &[u8], slot_offset: usize) -> Result<u64, HeapError> {
        Self::read_u64_field(bytes, slot_offset, XMAX_OFFSET, "xmax field outside tuple")
    }

    #[inline]
    pub(super) fn read_n_atts(bytes: &[u8], slot_offset: usize) -> Result<u16, HeapError> {
        Self::read_u16_field(
            bytes,
            slot_offset,
            N_ATTS_OFFSET,
            "n_atts field outside tuple",
        )
    }

    #[inline]
    pub(super) fn read_infomask(bytes: &[u8], slot_offset: usize) -> Result<u16, HeapError> {
        Self::read_u16_field(
            bytes,
            slot_offset,
            INFOMASK_OFFSET,
            "infomask field outside tuple",
        )
    }

    #[inline]
    pub(super) fn write_xmax(
        bytes: &mut [u8],
        slot_offset: usize,
        xid: Xid,
    ) -> Result<(), HeapError> {
        Self::write_u64_field(
            bytes,
            slot_offset,
            XMAX_OFFSET,
            xid.raw(),
            "xmax field outside tuple",
        )
    }

    #[inline]
    pub(super) fn write_cmax(
        bytes: &mut [u8],
        slot_offset: usize,
        command_id: CommandId,
    ) -> Result<(), HeapError> {
        Self::write_u32_field(
            bytes,
            slot_offset,
            CMAX_OFFSET,
            command_id.raw(),
            "cmax field outside tuple",
        )
    }

    #[inline]
    pub(super) fn write_infomask(
        bytes: &mut [u8],
        slot_offset: usize,
        infomask: u16,
    ) -> Result<(), HeapError> {
        Self::write_u16_field(
            bytes,
            slot_offset,
            INFOMASK_OFFSET,
            infomask,
            "infomask field outside tuple",
        )
    }

    #[inline]
    pub(super) fn write_ctid(
        bytes: &mut [u8],
        slot_offset: usize,
        tid: TupleId,
    ) -> Result<(), HeapError> {
        Self::write_u32_field(
            bytes,
            slot_offset,
            CTID_RELATION_OFFSET,
            tid.page.relation.0.raw(),
            "ctid relation field outside tuple",
        )?;
        Self::write_u32_field(
            bytes,
            slot_offset,
            CTID_BLOCK_SLOT_OFFSET,
            Self::block_slot_packed(tid)?,
            "ctid block-slot field outside tuple",
        )
    }

    #[inline]
    fn field_bytes<'a>(
        bytes: &'a [u8],
        slot_offset: usize,
        relative_offset: usize,
        width: usize,
        error: &'static str,
    ) -> Result<&'a [u8], HeapError> {
        let range = Self::tuple_field_range(slot_offset, relative_offset, width, error)?;
        bytes.get(range).ok_or(HeapError::MalformedHeader(error))
    }

    #[inline]
    fn field_bytes_mut<'a>(
        bytes: &'a mut [u8],
        slot_offset: usize,
        relative_offset: usize,
        width: usize,
        error: &'static str,
    ) -> Result<&'a mut [u8], HeapError> {
        let range = Self::tuple_field_range(slot_offset, relative_offset, width, error)?;
        bytes
            .get_mut(range)
            .ok_or(HeapError::MalformedHeader(error))
    }

    #[inline]
    fn read_u16_field(
        bytes: &[u8],
        slot_offset: usize,
        relative_offset: usize,
        error: &'static str,
    ) -> Result<u16, HeapError> {
        let field = Self::field_bytes(bytes, slot_offset, relative_offset, U16_WIDTH, error)?;
        Ok(u16::from_le_bytes(
            field
                .try_into()
                .map_err(|_| HeapError::MalformedHeader(error))?,
        ))
    }

    #[inline]
    fn read_u64_field(
        bytes: &[u8],
        slot_offset: usize,
        relative_offset: usize,
        error: &'static str,
    ) -> Result<u64, HeapError> {
        let field = Self::field_bytes(bytes, slot_offset, relative_offset, U64_WIDTH, error)?;
        Ok(u64::from_le_bytes(
            field
                .try_into()
                .map_err(|_| HeapError::MalformedHeader(error))?,
        ))
    }

    #[inline]
    fn write_u16_field(
        bytes: &mut [u8],
        slot_offset: usize,
        relative_offset: usize,
        value: u16,
        error: &'static str,
    ) -> Result<(), HeapError> {
        Self::field_bytes_mut(bytes, slot_offset, relative_offset, U16_WIDTH, error)?
            .copy_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline]
    fn write_u32_field(
        bytes: &mut [u8],
        slot_offset: usize,
        relative_offset: usize,
        value: u32,
        error: &'static str,
    ) -> Result<(), HeapError> {
        Self::field_bytes_mut(bytes, slot_offset, relative_offset, U32_WIDTH, error)?
            .copy_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline]
    fn write_u64_field(
        bytes: &mut [u8],
        slot_offset: usize,
        relative_offset: usize,
        value: u64,
        error: &'static str,
    ) -> Result<(), HeapError> {
        Self::field_bytes_mut(bytes, slot_offset, relative_offset, U64_WIDTH, error)?
            .copy_from_slice(&value.to_le_bytes());
        Ok(())
    }

    #[inline]
    fn block_slot_packed(tid: TupleId) -> Result<u32, HeapError> {
        let slot_bits = u32::from(tid.slot)
            .checked_shl(CTID_SLOT_SHIFT)
            .ok_or(HeapError::MalformedHeader("ctid slot shift overflow"))?;
        Ok((tid.page.block.raw() & BLOCK_NUMBER_MASK) | slot_bits)
    }
}
