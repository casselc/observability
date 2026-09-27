#!/usr/bin/env bash
# cargo clippy over otap-rs (lib, bins, tests), warnings denied, except the
# lints the crate already trips at the commit CI was added. They are all
# style findings except await_holding_refcell_ref (src/bin/consume.rs, a
# RefCell borrow held across an .await), which is tracked separately. Most
# sit in src/consumer/ and src/bin/consume.rs, which are under active
# development; fixing them belongs there, not in CI. A lint kind that is not
# on this list fails the job, so the list can only shrink: delete an entry
# once the crate is clean of it.
#
#   (cd otel-chdb/otap-rs && ../../ci/clippy.sh)
set -euo pipefail
allow=(
  collapsible_if                  # 17: consumer/{coord,sql,worker}.rs, ...
  needless_borrow                 # 10: consumer/worker.rs
  manual_is_multiple_of           #  6: consumer/sql.rs
  needless_borrows_for_generic_args  # 6: tests/creds.rs
  doc_lazy_continuation           #  4: src/store.rs
  enum_variant_names              #  2: tests/common/s3inline.rs
  useless_vec                     #  2: consumer/tests.rs
  type_complexity                 #  2: tests/series.rs
  len_without_is_empty            #  1: src/columns.rs
  sliced_string_as_bytes          #  1: src/creds.rs
  await_holding_refcell_ref       #  1: src/bin/consume.rs
  explicit_auto_deref             #  1: src/bin/consume.rs
  duplicated_attributes           #  1: consumer/mod.rs
  excessive_precision             #  1: src/render.rs
  field_reassign_with_default     #  1: tests/determinism.rs
  let_and_return                  #  1: tests/series.rs
)
args=(-D warnings)
for l in "${allow[@]}"; do args+=(-A "clippy::$l"); done
exec cargo clippy --release --all-targets --locked -- "${args[@]}"
