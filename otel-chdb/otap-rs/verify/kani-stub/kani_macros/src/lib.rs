//! Kani's harness attributes as no-ops, for type-checking (see ../kani).

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn proof(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn unwind(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn solver(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}

#[proc_macro_attribute]
pub fn stub(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
