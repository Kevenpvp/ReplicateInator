use proc_macro::TokenStream;
use quote::{format_ident, quote};
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

    let mut paths_vec: Vec<Path> = Vec::new();

    for attr in &input.attrs {
        if attr.path().is_ident("replication")
        && let Ok(paths) = attr.parse_args_with(Punctuated::<Path, Token![,]>::parse_terminated)
        {
            paths_vec.extend(paths);
        }
    }

    let components_type = if paths_vec.is_empty() {
        quote!(())
    } else if paths_vec.len() == 1 {
        let single = &paths_vec[0];
        quote!(#single)
    } else {
        quote!((#(#paths_vec),*))
    };

    let query_data_type = quote! {
        (
            bevy::prelude::Entity,
            #(&'static #paths_vec,)*
            &'static mut Self,
            &'static mut ServerReplicator
        )
    };

    let filter_type = if paths_vec.is_empty() {
        quote!((bevy::prelude::With<Self>, bevy::prelude::With<ServerReplicator>))
    } else if paths_vec.len() == 1 {
        let single = &paths_vec[0];
        quote!((bevy::prelude::With<Self>, bevy::prelude::With<ServerReplicator>, bevy::prelude::Changed<#single>))
    } else {
        quote! {
            (
                bevy::prelude::With<Self>,
                bevy::prelude::With<ServerReplicator>,
                bevy::prelude::Or<(#(bevy::prelude::Changed<#paths_vec>),*)>
            )
        }
    };

    let comp_idents: Vec<_> = (0..paths_vec.len())
        .map(|i| format_ident!("comp_{}", i))
        .collect();

    let spawn_getters = paths_vec.iter().zip(comp_idents.iter()).map(|(path, ident)| {
        quote! {
            let Some(#ident) = world.get::<#path>(entity) else { return; };
        }
    });

    let expanded = quote! {
        impl ServerReplicationSystem for #struct_name {
            type Components = #components_type;
            type UpdateQueryData = #query_data_type;
            type UpdateQueryFilter = #filter_type;

            fn on_spawned(
                mut world: bevy::ecs::world::DeferredWorld,
                context: bevy::ecs::lifecycle::HookContext,
            ) {
                let entity = context.entity;

                #(#spawn_getters)*

                let refs: &[&dyn bevy::reflect::Reflect] = &[#(#comp_idents.as_reflect()),*];

                let Some(server_component_registry) = world.get_resource::<ServerComponentRegistry>() else { return; };

                let system = match world.get::<Self>(entity) {
                    Some(s) => s,
                    None => return,
                };

                let all_bytes = system.prepare_replication_bytes(refs, server_component_registry);

                if let Ok(mut entity_mut) = world.get_entity_mut(entity) {
                    if let Ok((system, mut server_replicator)) =
                        entity_mut.get_components_mut::<(&mut Self, &mut ServerReplicator)>()
                    {
                        system.add_bytes_to_queue(entity, all_bytes, true, &mut server_replicator);
                    }
                }
            }

            fn check_update(
                mut query: bevy::prelude::Query<Self::UpdateQueryData, Self::UpdateQueryFilter>,
                server_component_registry: NetRes<ServerComponentRegistry>
            ) {
                for (entity, #(#comp_idents,)* mut system, mut server_replicator) in query.iter_mut() {
                    let refs: &[&dyn bevy::reflect::Reflect] = &[#(#comp_idents.as_reflect()),*];
                    let all_bytes = system.prepare_replication_bytes(refs,&server_component_registry);

                    system.add_bytes_to_queue(entity, all_bytes, true, &mut server_replicator);
                }
            }
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