use networkinator::shared::plugins::messaging::{MessageTrait, MessageTraitPlugin};
use std::any::{TypeId};
use std::collections::HashMap;
use bevy::app::{App, Plugin};
use bevy::asset::uuid::Uuid;
use bevy::ecs::component::ComponentId;
use bevy::prelude::{Component, Entity, EntityRef, PartialReflect, Resource};
use bevy::reflect::erased_serde::__private::serde::de::DeserializeOwned;
use message_pro_macro::ConnectionMessage;
use networkinator::shared::plugins::network::{CurrentNetworkSides, NetworkType};
use serde::{Deserialize, Serialize};

pub struct ReplicateShared;

pub struct ClientComponentData{
    pub deserialize_fn: fn(bytes: Vec<u8>) -> Box<dyn PartialReflect>
}

pub struct ServerComponentData {
    pub serialize_fn: fn(EntityRef) -> Option<Vec<u8>>
}

pub trait ReplicationSharedTrait {
    fn register_replication_component<T: Component + DeserializeOwned + PartialReflect + Serialize>(&mut self);
}

#[derive(Serialize,Deserialize,ConnectionMessage)]
pub struct ReplicateSystemToClient{
    pub owner: Option<Uuid>,
    pub components_bytes:  HashMap<u32, Vec<u8>>,
    pub entity: Entity,
    pub system_id: u32
}

#[derive(Serialize,Deserialize,ConnectionMessage)]
pub struct SendEntityRemovedForClient{
    pub entity: Entity
}

#[derive(Resource,Default)]
pub struct ServerComponentRegistry(pub(crate) u32, pub(crate) HashMap<TypeId, u32>, pub(crate) HashMap<u32, ServerComponentData>, pub(crate) HashMap<ComponentId, u32>);

#[derive(Resource,Default)]
pub struct ClientComponentRegistry(pub(crate) u32, pub(crate) HashMap<TypeId, u32>, pub(crate) HashMap<u32, ClientComponentData>, pub(crate) HashMap<ComponentId, u32>);

impl Plugin for ReplicateShared {
    fn build(&self, app: &mut App) {
        app.register_message::<ReplicateSystemToClient>();
        app.register_message::<SendEntityRemovedForClient>();

        let (is_client, is_local_server, is_dedicated_server) = {
            let world = app.world_mut();
            let mut sides = world.get_resource_mut::<CurrentNetworkSides>()
                .expect("Insert ServerNetworkPlugin or ClientNetworkPlugin first, if its a LocalServer insert both first");
            (
                sides.side().contains(&NetworkType::Client),
                sides.side().contains(&NetworkType::LocalServer),
                sides.side().contains(&NetworkType::DedicatedServer)
            )
        };

        if is_local_server || is_dedicated_server {
            app.init_resource::<ServerComponentRegistry>();
        }

        if is_client {
            app.init_resource::<ClientComponentRegistry>();
        }
    }
}

pub fn default_deserialize_component<T:Component + DeserializeOwned + PartialReflect>(bytes: Vec<u8>) -> Box<dyn PartialReflect> {
    let component = postcard::from_bytes::<T>(&bytes).expect("Failed to deserialize ServerComponent");

    Box::new(component)
}

pub fn default_serialize_component_fn<T: Component + Serialize>(entity_ref: EntityRef) -> Option<Vec<u8>> {
    let component = entity_ref.get::<T>()?;

    postcard::to_allocvec(component).ok()
}

impl ReplicationSharedTrait for App{
    fn register_replication_component<T: Component + DeserializeOwned + PartialReflect + Serialize>(&mut self) {
        let type_id = TypeId::of::<T>();

        let (is_client, is_local_server, is_dedicated_server) = {
            let world = self.world_mut();
            let mut sides = world.get_resource_mut::<CurrentNetworkSides>()
                .expect("Insert ServerNetworkPlugin or ClientNetworkPlugin first, if its a LocalServer insert both first");
            (
                sides.side().contains(&NetworkType::Client),
                sides.side().contains(&NetworkType::LocalServer),
                sides.side().contains(&NetworkType::DedicatedServer)
            )
        };

        if is_local_server || is_dedicated_server {
            let world_mut = self.world_mut();
            let component_id = world_mut.register_component::<T>();
            let mut server_component_registry = world_mut.resource_mut::<ServerComponentRegistry>();

            if server_component_registry.1.contains_key(&type_id) {
                return;
            }

            let new_index = server_component_registry.0 + 1;

            server_component_registry.0 = new_index;
            server_component_registry.1.insert(type_id,new_index);
            server_component_registry.2.insert(new_index,ServerComponentData{
                serialize_fn: default_serialize_component_fn::<T>
            });
            server_component_registry.3.insert(component_id,new_index);
        }

        if is_client {
            let world_mut = self.world_mut();
            let component_id = world_mut.register_component::<T>();
            let mut client_component_registry = world_mut.resource_mut::<ClientComponentRegistry>();

            if client_component_registry.1.contains_key(&type_id) {
                return;
            }

            let new_index = client_component_registry.0 + 1;

            client_component_registry.0 = new_index;
            client_component_registry.1.insert(type_id,new_index);
            client_component_registry.2.insert(new_index,ClientComponentData{
                deserialize_fn: default_deserialize_component::<T>
            });
            client_component_registry.3.insert(component_id,new_index);
        }
    }
}