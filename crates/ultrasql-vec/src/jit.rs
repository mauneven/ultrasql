//! Runtime code-generation helpers for hot vector kernels.
//!
//! This module is deliberately narrow: it compiles one production kernel
//! shape, `SUM(int_col) WHERE int_col > literal`, through Cranelift and
//! falls back to the existing scalar/SIMD kernels if native JIT setup is
//! unavailable. The generated code is process-lifetime cached; each
//! backing [`cranelift_jit::JITModule`] is intentionally leaked so the
//! returned function pointers can stay valid for the life of the server.

use std::sync::OnceLock;

use cranelift_codegen::Context;
use cranelift_codegen::ir::{
    AbiParam, Function, InstBuilder, MemFlags, Signature, UserFuncName, condcodes::IntCC, types,
};
use cranelift_codegen::isa::CallConv;
use cranelift_codegen::settings::{self, Configurable};
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module, default_libcall_names};

/// Default row threshold before a lowerer considers JIT code.
pub const DEFAULT_JIT_ABOVE_ROWS: usize = 262_144;

/// Per-statement JIT controls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JitConfig {
    /// Whether JIT paths may run.
    pub enabled: bool,
    /// Minimum input rows before the lowerer pays compile / dispatch cost.
    pub above_rows: usize,
}

impl JitConfig {
    /// JIT disabled, PostgreSQL-compatible surface default until
    /// benchmark gates prove the compiled paths win broadly.
    pub const OFF: Self = Self {
        enabled: false,
        above_rows: DEFAULT_JIT_ABOVE_ROWS,
    };

    /// JIT enabled with the default threshold.
    pub const ON: Self = Self {
        enabled: true,
        above_rows: DEFAULT_JIT_ABOVE_ROWS,
    };

    /// Returns true when this statement should try a compiled kernel.
    #[inline]
    pub const fn should_jit(self, rows: usize) -> bool {
        self.enabled && rows >= self.above_rows
    }
}

impl Default for JitConfig {
    fn default() -> Self {
        Self::OFF
    }
}

type FilterSumI32GtFn = unsafe extern "C" fn(*const i32, usize, i32) -> i64;
type FilterSumI64GtFn = unsafe extern "C" fn(*const i64, usize, i64) -> i64;

static FILTER_SUM_I32_GT: OnceLock<Option<FilterSumI32GtFn>> = OnceLock::new();
static FILTER_SUM_I64_GT: OnceLock<Option<FilterSumI64GtFn>> = OnceLock::new();

/// Run the Cranelift-compiled `SUM(i32) WHERE i32 > threshold` kernel.
///
/// Returns `None` if native Cranelift setup fails on the host. Callers
/// must then use the normal scalar/SIMD kernel to preserve correctness.
#[must_use]
pub fn filter_sum_i32_widening_gt_jit(data: &[i32], threshold: i32) -> Option<i64> {
    let func = (*FILTER_SUM_I32_GT.get_or_init(build_filter_sum_i32_gt))?;
    // SAFETY: `func` is generated with ABI
    // `extern "C" fn(*const i32, usize, i32) -> i64`; `data.as_ptr()`
    // is valid for `data.len()` contiguous `i32` reads for the duration
    // of the call, and the JIT code never writes through the pointer.
    Some(unsafe { func(data.as_ptr(), data.len(), threshold) })
}

/// Run the Cranelift-compiled `SUM(i64) WHERE i64 > threshold` kernel.
///
/// Returns `None` if native Cranelift setup fails on the host. Callers
/// must then use the normal scalar/SIMD kernel to preserve correctness.
#[must_use]
pub fn filter_sum_i64_gt_jit(data: &[i64], threshold: i64) -> Option<i64> {
    let func = (*FILTER_SUM_I64_GT.get_or_init(build_filter_sum_i64_gt))?;
    // SAFETY: `func` is generated with ABI
    // `extern "C" fn(*const i64, usize, i64) -> i64`; `data.as_ptr()`
    // is valid for `data.len()` contiguous `i64` reads for the duration
    // of the call, and the JIT code never writes through the pointer.
    Some(unsafe { func(data.as_ptr(), data.len(), threshold) })
}

fn build_filter_sum_i32_gt() -> Option<FilterSumI32GtFn> {
    let mut flag_builder = settings::builder();
    flag_builder.set("use_colocated_libcalls", "false").ok()?;
    flag_builder.set("is_pic", "false").ok()?;
    let isa_builder = cranelift_native::builder().ok()?;
    let isa = isa_builder
        .finish(settings::Flags::new(flag_builder))
        .ok()?;
    let mut module = JITModule::new(JITBuilder::with_isa(isa, default_libcall_names()));
    let ptr_ty = module.target_config().pointer_type();

    let sig = Signature {
        params: vec![
            AbiParam::new(ptr_ty),
            AbiParam::new(ptr_ty),
            AbiParam::new(types::I32),
        ],
        returns: vec![AbiParam::new(types::I64)],
        call_conv: CallConv::triple_default(module.isa().triple()),
    };
    let func_id = module
        .declare_function("ultrasql_filter_sum_i32_gt", Linkage::Local, &sig)
        .ok()?;

    let mut ctx = Context::new();
    ctx.func = Function::with_name_signature(UserFuncName::user(0, func_id.as_u32()), sig);
    let mut func_ctx = FunctionBuilderContext::new();
    {
        let mut b = FunctionBuilder::new(&mut ctx.func, &mut func_ctx);
        let entry = b.create_block();
        let header = b.create_block();
        let body = b.create_block();
        let exit = b.create_block();

        b.append_block_params_for_function_params(entry);
        b.append_block_param(header, ptr_ty);
        b.append_block_param(header, types::I64);
        b.append_block_param(body, ptr_ty);
        b.append_block_param(body, types::I64);
        b.append_block_param(exit, types::I64);

        b.switch_to_block(entry);
        let data = b.block_params(entry)[0];
        let len = b.block_params(entry)[1];
        let threshold = b.block_params(entry)[2];
        let zero_idx = b.ins().iconst(ptr_ty, 0);
        let zero_sum = b.ins().iconst(types::I64, 0);
        b.ins().jump(header, &[zero_idx.into(), zero_sum.into()]);

        b.switch_to_block(header);
        let idx = b.block_params(header)[0];
        let sum = b.block_params(header)[1];
        let in_bounds = b.ins().icmp(IntCC::UnsignedLessThan, idx, len);
        b.ins().brif(
            in_bounds,
            body,
            &[idx.into(), sum.into()],
            exit,
            &[sum.into()],
        );

        b.switch_to_block(body);
        let idx = b.block_params(body)[0];
        let sum = b.block_params(body)[1];
        let byte_off = b.ins().imul_imm(idx, 4);
        let addr = b.ins().iadd(data, byte_off);
        let value_i32 = b.ins().load(types::I32, MemFlags::trusted(), addr, 0);
        let pred = b.ins().icmp(IntCC::SignedGreaterThan, value_i32, threshold);
        let value_i64 = b.ins().sextend(types::I64, value_i32);
        let added = b.ins().iadd(sum, value_i64);
        let new_sum = b.ins().select(pred, added, sum);
        let one = b.ins().iconst(ptr_ty, 1);
        let next_idx = b.ins().iadd(idx, one);
        b.ins().jump(header, &[next_idx.into(), new_sum.into()]);

        b.switch_to_block(exit);
        let final_sum = b.block_params(exit)[0];
        b.ins().return_(&[final_sum]);
        b.seal_all_blocks();
        b.finalize();
    }

    module.define_function(func_id, &mut ctx).ok()?;
    module.finalize_definitions().ok()?;
    let code = module.get_finalized_function(func_id);
    // Keep executable memory alive for process lifetime.
    let _leaked = Box::leak(Box::new(module));
    // SAFETY: Cranelift emitted code for the exact `FilterSumI32GtFn`
    // signature declared above. The leaked module keeps the code page
    // alive for all future calls.
    Some(unsafe { std::mem::transmute::<*const u8, FilterSumI32GtFn>(code) })
}

fn build_filter_sum_i64_gt() -> Option<FilterSumI64GtFn> {
    let mut flag_builder = settings::builder();
    flag_builder.set("use_colocated_libcalls", "false").ok()?;
    flag_builder.set("is_pic", "false").ok()?;
    let isa_builder = cranelift_native::builder().ok()?;
    let isa = isa_builder
        .finish(settings::Flags::new(flag_builder))
        .ok()?;
    let mut module = JITModule::new(JITBuilder::with_isa(isa, default_libcall_names()));
    let ptr_ty = module.target_config().pointer_type();

    let sig = Signature {
        params: vec![
            AbiParam::new(ptr_ty),
            AbiParam::new(ptr_ty),
            AbiParam::new(types::I64),
        ],
        returns: vec![AbiParam::new(types::I64)],
        call_conv: CallConv::triple_default(module.isa().triple()),
    };
    let func_id = module
        .declare_function("ultrasql_filter_sum_i64_gt", Linkage::Local, &sig)
        .ok()?;

    let mut ctx = Context::new();
    ctx.func = Function::with_name_signature(UserFuncName::user(1, func_id.as_u32()), sig);
    let mut func_ctx = FunctionBuilderContext::new();
    {
        let mut b = FunctionBuilder::new(&mut ctx.func, &mut func_ctx);
        let entry = b.create_block();
        let header = b.create_block();
        let body = b.create_block();
        let exit = b.create_block();

        b.append_block_params_for_function_params(entry);
        b.append_block_param(header, ptr_ty);
        b.append_block_param(header, types::I64);
        b.append_block_param(body, ptr_ty);
        b.append_block_param(body, types::I64);
        b.append_block_param(exit, types::I64);

        b.switch_to_block(entry);
        let data = b.block_params(entry)[0];
        let len = b.block_params(entry)[1];
        let threshold = b.block_params(entry)[2];
        let zero_idx = b.ins().iconst(ptr_ty, 0);
        let zero_sum = b.ins().iconst(types::I64, 0);
        b.ins().jump(header, &[zero_idx.into(), zero_sum.into()]);

        b.switch_to_block(header);
        let idx = b.block_params(header)[0];
        let sum = b.block_params(header)[1];
        let in_bounds = b.ins().icmp(IntCC::UnsignedLessThan, idx, len);
        b.ins().brif(
            in_bounds,
            body,
            &[idx.into(), sum.into()],
            exit,
            &[sum.into()],
        );

        b.switch_to_block(body);
        let idx = b.block_params(body)[0];
        let sum = b.block_params(body)[1];
        let byte_off = b.ins().imul_imm(idx, 8);
        let addr = b.ins().iadd(data, byte_off);
        let value = b.ins().load(types::I64, MemFlags::trusted(), addr, 0);
        let pred = b.ins().icmp(IntCC::SignedGreaterThan, value, threshold);
        let added = b.ins().iadd(sum, value);
        let new_sum = b.ins().select(pred, added, sum);
        let one = b.ins().iconst(ptr_ty, 1);
        let next_idx = b.ins().iadd(idx, one);
        b.ins().jump(header, &[next_idx.into(), new_sum.into()]);

        b.switch_to_block(exit);
        let final_sum = b.block_params(exit)[0];
        b.ins().return_(&[final_sum]);
        b.seal_all_blocks();
        b.finalize();
    }

    module.define_function(func_id, &mut ctx).ok()?;
    module.finalize_definitions().ok()?;
    let code = module.get_finalized_function(func_id);
    let _leaked = Box::leak(Box::new(module));
    // SAFETY: Cranelift emitted code for the exact `FilterSumI64GtFn`
    // signature declared above. The leaked module keeps the code page
    // alive for all future calls.
    Some(unsafe { std::mem::transmute::<*const u8, FilterSumI64GtFn>(code) })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scalar(data: &[i32], threshold: i32) -> i64 {
        data.iter()
            .filter(|&&v| v > threshold)
            .fold(0_i64, |acc, &v| acc.wrapping_add(i64::from(v)))
    }

    fn scalar_i64(data: &[i64], threshold: i64) -> i64 {
        data.iter()
            .filter(|&&v| v > threshold)
            .fold(0_i64, |acc, &v| acc.wrapping_add(v))
    }

    #[test]
    fn jit_filter_sum_i32_gt_matches_scalar() {
        let data: Vec<i32> = (-257..4099)
            .map(|v| if v % 17 == 0 { -v } else { v })
            .collect();
        for threshold in [-100, 0, 1, 127, 4096] {
            let Some(got) = filter_sum_i32_widening_gt_jit(&data, threshold) else {
                return;
            };
            assert_eq!(got, scalar(&data, threshold), "threshold {threshold}");
        }
    }

    #[test]
    fn jit_filter_sum_i64_gt_matches_scalar() {
        let data: Vec<i64> = (-257..4099)
            .map(|v| {
                let v = i64::from(v);
                if v % 17 == 0 { -v * 11 } else { v * 7 }
            })
            .collect();
        for threshold in [-1000, -1, 0, 127, 4096] {
            let Some(got) = filter_sum_i64_gt_jit(&data, threshold) else {
                return;
            };
            assert_eq!(got, scalar_i64(&data, threshold), "threshold {threshold}");
        }
    }

    #[test]
    fn jit_config_threshold_gate() {
        let cfg = JitConfig {
            enabled: true,
            above_rows: 10,
        };
        assert!(!cfg.should_jit(9));
        assert!(cfg.should_jit(10));
        assert!(!JitConfig::OFF.should_jit(1_000_000));
    }
}
