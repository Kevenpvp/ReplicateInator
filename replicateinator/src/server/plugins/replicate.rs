use std::any::TypeId;
use networkinator::shared::plugins::messaging::{SendArgs, ServerConnectionParams};
use std::collections::{HashMap, VecDeque};
use bevy::app::App;
use bevy::asset::uuid::Uuid;
use bevy::ecs::lifecycle::HookContext;
use bevy::ecs::schedule::ScheduleLabel;
use bevy::ecs::world::DeferredWorld;
use bevy::prelude::{Bundle, Commands, Component, Entity, IntoScheduleConfigs, Last, Message, MessageReader, Plugin, Query, Res, ResMut, Resource, With, World};
use networkinator::server::plugins::network::PeersDroppedServer;
use crate::shared::plugins::replicate::{ReplicateSystemToClient, SendEntityRemovedForClient, ServerComponentRegistry};

pub struct ReplicateServer;

pub trait RegisterServerReplicationSystem{
    fn register_server_replication_system<T: ServerReplicationSystem>(&mut self, schedule: impl ScheduleLabel);
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
    pub destroy_when_owner_left: bool,
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
        server_system_registry: Res<ServerSystemRegistry>,
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
                    }, server_replicator.just_for_authenticated, server_replicator.send_args.as_ref(), vec![]);
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
}

impl Plugin for ReplicateServer {
    fn build(&self, app: &mut App) {
        app.add_message::<ReplicatorRemoved>();
        app.init_resource::<EntitiesPeerList>();
        app.init_resource::<ServerSystemRegistry>();
        app.add_systems(Last,(check_peers_left,send_removed_to_peers).chain());
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

fn check_peers_left(
    mut peers_dropped_server: MessageReader<PeersDroppedServer>,
    mut entities_peer_list: ResMut<EntitiesPeerList>,
    mut commands: Commands,
){
    for ev in peers_dropped_server.read() {
        let peers = &ev.peers;

        for (peer_uuid, _) in peers.values() {
            if let Some(peer_uuid) = peer_uuid && let Some(entities) = entities_peer_list.0.get_mut(peer_uuid) {
                for (entity,is_removing) in entities {
                    if *is_removing { continue; }

                    *is_removing = true;

                    let entity = *entity;
                    let peer_uuid = *peer_uuid;

                    commands.queue(move |world: &mut World| {
                        if let Some(replicator) = world.get_mut::<ServerReplicator>(entity)
                        && replicator.destroy_when_owner_left
                        {
                            world.despawn(entity);
                        }else {
                            let mut entities_peer_list = world.resource_mut::<EntitiesPeerList>();

                            if let Some(entities) = entities_peer_list.0.get_mut(&peer_uuid)
                            && let Some(is_removing) = entities.get_mut(&entity)
                            {
                                *is_removing = false;
                            }
                        }
                    });
                }
            }
        }
    }
}

fn send_removed_to_peers(
    mut replicator_removed: MessageReader<ReplicatorRemoved>,
    mut server_connection_params: ServerConnectionParams
){
    for ev in replicator_removed.read() {
        server_connection_params.send_message_for_all(ev.connection_id, ev.port_to_remove, SendEntityRemovedForClient{
            entity: ev.entity,
        },false,ev.send_args.as_ref(),vec![]);
    }
}