use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, punctuated::Punctuated, DeriveInput, Path, Token};

#[proc_macro_derive(ClientReplicationSystem, attributes(replication))]
pub fn derive_client_replication_system(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let mut components_type = quote!(());
    
    for attr in &input.attrs {
        if attr.path().is_ident("replication") {
            let nested = attr.parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated);

            if let Ok(paths) = nested {
                let paths_vec: Vec<_> = paths.into_iter().collect();
                if paths_vec.len() == 1 {
                    let single = &paths_vec[0];
                    components_type = quote!(#single);
                } else if paths_vec.len() > 1 {
                    components_type = quote!((#(#paths_vec),*));
                }
            }
        }
    }

    let expanded = quote! {
        impl ClientReplicationSystem for #struct_name {
            type Components = #components_type;
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ServerReplicationSystem, attributes(replication))]
pub fn derive_server_replication_system(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let mut components_type = quote!(());

    for attr in &input.attrs {
        if attr.path().is_ident("replication") {
            let nested = attr.parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated);

            if let Ok(paths) = nested {
                let paths_vec: Vec<_> = paths.into_iter().collect();
                if paths_vec.len() == 1 {
                    let single = &paths_vec[0];
                    components_type = quote!(#single);
                } else if paths_vec.len() > 1 {
                    components_type = quote!((#(#paths_vec),*));
                }
            }
        }
    }

    let expanded = quote! {
        impl ServerReplicationSystem for #struct_name {
            type Components = #components_type;
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ServerStateReplicationSystem)]
pub fn derive_server_state_replication_system(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl ServerStateSystem for #struct_name {

        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ClientStateReplicationSystem)]
pub fn derive_client_state_replication_system(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl ClientStateSystem for #struct_name {

        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ServerResourceReplicationSystem)]
pub fn derive_server_resoure_replication_system(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl ServerResourceReplicationSystem for #struct_name {

        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ClientResourceReplicationSystem)]
pub fn derive_client_resoure_replication_system(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = input.ident;

    let expanded = quote! {
        impl ClientResourceReplicationSystem for #struct_name {

        }
    };

    TokenStream::from(expanded)
}