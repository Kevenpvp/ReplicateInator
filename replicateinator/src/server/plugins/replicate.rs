use std::any::TypeId;
use networkinator::shared::plugins::messaging::{SendArgs, ServerConnectionParams};
use std::collections::{HashMap, VecDeque};
use bevy::app::App;
use bevy::asset::uuid::Uuid;
use bevy::ecs::lifecycle::HookContext;
use bevy::ecs::schedule::ScheduleLabel;
use bevy::ecs::system::BoxedSystem;
use bevy::ecs::world::DeferredWorld;
use bevy::prelude::{Added, Bundle, Changed, Commands, Component, Entity, IntoScheduleConfigs, IntoSystem, Last, Message, MessageReader, Or, Plugin, Query, Resource, Single, With, World};
use networkinator::NetRes;
use networkinator::shared::plugins::network::{CurrentNetworkSides, LocalPeerUUID, NetworkType};
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::shared::plugins::replicate::{ReplicateSystemToClient, SendEntityRemovedForClient, SendResourceReplicatedForClient, ServerComponentRegistry, ServerResourceData, ServerResourceRegistry};

pub struct ReplicateServer;

pub trait RegisterServerReplicationSystem{
    fn register_server_replication_system<T: ServerReplicationSystem>(&mut self, schedule: impl ScheduleLabel);
    fn register_server_replication_resource<T: ServerResourceReplicationSystem>(&mut self, schedule: impl ScheduleLabel, server_resource_data: ServerResourceData);
}

#[allow(dead_code)]
#[derive(Component)]
#[component(on_add = replicator_added, on_remove = replicator_removed)]
pub struct ServerReplicator{
    pub owner: Option<Uuid>,
    pub replication_owner: Option<Uuid>,
    pub connection_id: u32,
    pub port: u32,
    pub port_to_remove: u32,
    pub just_for_authenticated: bool,
    pub send_args: Option<SendArgs>,
    pub bytes_queue: HashMap<TypeId, VecDeque<HashMap<u32, Vec<u8>>>>,
}

#[derive(Message)]
pub struct ReplicatorRemoved{
    pub entity: Entity,
    pub port_to_remove: u32,
    pub connection_id: u32,
    pub send_args: Option<SendArgs>,
}

#[derive(Resource,Default)]
pub struct ServerSystemRegistry(pub(crate) u32, pub(crate) HashMap<u32, TypeId>, pub(crate) HashMap<TypeId, u32>);

#[derive(Resource,Default)]
pub struct EntitiesPeerList(pub(crate) HashMap<Uuid,HashMap<Entity,bool>>);

pub trait ServerResourceReplicationSystem: Resource + Serialize + DeserializeOwned {
    fn serialize_resource(&self) -> Vec<u8> {
        postcard::to_allocvec(&self).unwrap()
    }

    fn on_resource_changed() -> BoxedSystem<(), ()> {
        Box::new(IntoSystem::into_system(default_resource_changed::<Self>))
    }
}

pub trait ServerReplicationSystem: Component + Sized {
    type Components: Bundle;

    fn check_update(
        query: Query<(Entity, &Self), With<Self>>,
        mut commands: Commands,
    ){
        for (entity, _) in query.iter() {
            commands.queue(move |world: &mut World| {
                let component_ids = world
                    .register_bundle::<Self::Components>()
                    .explicit_components()
                    .to_vec();

                let last_run = world.last_change_tick();
                let this_run = world.change_tick();

                let entity_ref = world.entity(entity);

                let is_any_changed = component_ids.iter().any(|&comp_id| {
                    if let Some(ticks) = entity_ref.get_change_ticks_by_id(comp_id) {
                        ticks.is_changed(last_run, this_run)
                    } else {
                        false
                    }
                });

                if !is_any_changed {
                    return;
                }

                Self::add_bytes_to_queue(world, entity);
            });
        }
    }

    fn add_bytes_to_queue(world: &mut World, entity: Entity) {
        let components_bytes = Self::prepare_replication_bytes(world, entity);
        let replicator = world.get_mut::<ServerReplicator>(entity);
        let type_id = TypeId::of::<Self>();

        if let Some(mut replicator) = replicator {
            if let Some(current_queue) = replicator.bytes_queue.get_mut(&type_id) {
                current_queue.push_back(components_bytes);
            }else {
                let mut current_queue = VecDeque::new();

                current_queue.push_back(components_bytes);

                replicator.bytes_queue.insert(type_id, current_queue);
            }
        }
    }

    fn replicate_to_client(
        mut query: Query<(Entity, &Self, &mut ServerReplicator), (With<Self>, With<ServerReplicator>)>,
        mut server_connection_params: ServerConnectionParams,
        server_system_registry: NetRes<ServerSystemRegistry>,
        local_peer_uuid: Option<NetRes<LocalPeerUUID>>
    ){
        let type_id = TypeId::of::<Self>();

        for (entity, _, mut server_replicator) in query.iter_mut() {
            if let Some(current_queue) = server_replicator.bytes_queue.remove(&type_id) {
                for components_bytes in current_queue {
                    server_connection_params.send_message_for_all(server_replicator.connection_id,server_replicator.port,ReplicateSystemToClient{
                        owner: server_replicator.owner,
                        components_bytes,
                        entity,
                        system_id: *server_system_registry.2.get(&type_id).unwrap()
                    }, server_replicator.just_for_authenticated, server_replicator.send_args.as_ref(), if let Some(local_peer_uuid) = &local_peer_uuid && let Some(local_peer_uuid) = local_peer_uuid.get_peer_uuid() {vec![local_peer_uuid]} else { vec![] } );
                }
            }
        }
    }

    fn prepare_replication_bytes(world: &mut World, entity: Entity) -> HashMap<u32, Vec<u8>> {
        let mut components_bytes = HashMap::new();
        let component_ids = world
            .register_bundle::<Self::Components>()
            .explicit_components()
            .to_vec();

        let server_registry = world.resource::<ServerComponentRegistry>();
        let entity_ref = world.entity(entity);

        for component_id in component_ids {
            if let Some(network_id) = server_registry.3.get(&component_id)
                && let Some(data) = server_registry.2.get(network_id)
                && let Some(bytes) = (data.serialize_fn)(entity_ref)
            {
                components_bytes.insert(*network_id, bytes);
            }
        }

        components_bytes
    }
}

impl RegisterServerReplicationSystem for App {
    fn register_server_replication_system<T: ServerReplicationSystem>(&mut self, schedule: impl ScheduleLabel) {
        let type_id = TypeId::of::<T>();
        let mut server_system_registry = self.world_mut().resource_mut::<ServerSystemRegistry>();

        if server_system_registry.2.get(&type_id).is_some() {
            return;
        }

        let new_index = server_system_registry.0 + 1;

        server_system_registry.0 = new_index;
        server_system_registry.1.insert(new_index,type_id);
        server_system_registry.2.insert(type_id,new_index);

        self.add_systems(schedule,(T::check_update,T::replicate_to_client).chain());
    }

    fn register_server_replication_resource<T: ServerResourceReplicationSystem>(&mut self, schedule: impl ScheduleLabel, server_resource_data: ServerResourceData) {
        let type_id = TypeId::of::<T>();

        let (is_local_server, is_dedicated_server) = {
            let world = self.world_mut();
            let mut sides = world.get_resource_mut::<CurrentNetworkSides>()
                .expect("Insert ServerNetworkPlugin");
            (
                sides.side().contains(&NetworkType::LocalServer),
                sides.side().contains(&NetworkType::DedicatedServer)
            )
        };

        if !is_local_server && !is_dedicated_server {
            return;
        }

        let world_mut = self.world_mut();
        let mut server_resource_registry = world_mut.resource_mut::<ServerResourceRegistry>();

        if server_resource_registry.1.contains_key(&type_id) {
            return;
        }

        let new_index = server_resource_registry.0 + 1;

        server_resource_registry.0 = new_index;
        server_resource_registry.1.insert(type_id,new_index);
        server_resource_registry.2.insert(new_index,server_resource_data);

        self.add_systems(schedule,T::on_resource_changed());
    }
}

impl Plugin for ReplicateServer {
    fn build(&self, app: &mut App) {
        app.add_message::<ReplicatorRemoved>();
        app.init_resource::<EntitiesPeerList>();
        app.init_resource::<ServerSystemRegistry>();
        app.add_systems(Last,send_removed_to_peers);
    }
}

fn replicator_added(
    mut world: DeferredWorld,
    context: HookContext
){
    if let Some(replicator) = world.get_mut::<ServerReplicator>(context.entity)
        && let Some(peer_uuid) = replicator.owner
    {
        let mut entities_peer_list = world.resource_mut::<EntitiesPeerList>();

        if let Some(entities) = entities_peer_list.0.get_mut(&peer_uuid) {
            entities.insert(context.entity,false);
        }else{
            entities_peer_list.0.insert(peer_uuid, HashMap::from(
                [(context.entity,false)]
            ));
        }
    }
}

fn replicator_removed(
    mut world: DeferredWorld,
    context: HookContext
){
    let mut removed = false;
    let mut port_to_remove = 0;
    let mut send_args: Option<SendArgs> = None;
    let mut connection_id = 0;

    if let Some(mut replicator) = world.get_mut::<ServerReplicator>(context.entity)
        && let Some(peer_uuid) = replicator.owner
    {
        port_to_remove = replicator.port_to_remove;
        connection_id = replicator.connection_id;
        send_args = replicator.send_args.take();

        let mut entities_peer_list = world.resource_mut::<EntitiesPeerList>();

        if let Some(entities) = entities_peer_list.0.get_mut(&peer_uuid){
            entities.remove(&context.entity);

            removed = true;

            if entities.len() == 0 {
                entities_peer_list.0.remove(&peer_uuid);
            }
        }
    }

    if removed {
        world.write_message(ReplicatorRemoved{
            entity: context.entity,
            port_to_remove,
            connection_id,
            send_args
        });
    }
}

fn send_removed_to_peers(
    mut replicator_removed: MessageReader<ReplicatorRemoved>,
    mut server_connection_params: ServerConnectionParams,
    local_peer_uuid: Option<NetRes<LocalPeerUUID>>
){
    for ev in replicator_removed.read() {
        server_connection_params.send_message_for_all(ev.connection_id, ev.port_to_remove, SendEntityRemovedForClient{
            entity: ev.entity,
        },false,ev.send_args.as_ref(),if let Some(local_peer_uuid) = &local_peer_uuid && let Some(local_peer_uuid) = local_peer_uuid.get_peer_uuid() {vec![local_peer_uuid]} else { vec![] } );
    }
}

fn default_resource_changed<T: ServerResourceReplicationSystem>(
    query: Single<&T, Or<(Changed<T>, Added<T>)>>,
    mut server_connection_params: ServerConnectionParams,
    server_resource_registry: NetRes<ServerResourceRegistry>,
    local_peer_uuid: Option<NetRes<LocalPeerUUID>>
){
    let type_id = TypeId::of::<T>();

    if let Some(id) = server_resource_registry.1.get(&type_id)
    && let Some(server_resource_data) = server_resource_registry.2.get(id)
    {
        server_connection_params.send_message_for_all(server_resource_data.connection_id, server_resource_data.port_id, SendResourceReplicatedForClient{
            resource_id: *id,
            resource_bytes: query.serialize_resource(),
        }, server_resource_data.just_authenticated, server_resource_data.send_args.as_ref(), if let Some(local_peer_uuid) = &local_peer_uuid && let Some(local_peer_uuid) = local_peer_uuid.get_peer_uuid() {vec![local_peer_uuid]} else { vec![] } );
    }
}