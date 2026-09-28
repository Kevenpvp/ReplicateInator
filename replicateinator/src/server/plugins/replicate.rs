use std::any::TypeId;
use networkinator::shared::plugins::messaging::{SendArgs, ServerConnectionParams};
use std::collections::{HashMap, VecDeque};
use bevy::app::App;
use bevy::asset::uuid::Uuid;
use bevy::ecs::schedule::ScheduleLabel;
use bevy::prelude::{Bundle, Commands, Component, Entity, Plugin, Query, Res, Resource, With, World};
use crate::shared::plugins::replicate::{ReplicateSystemToClient, ServerComponentRegistry};

pub struct ServerReplicate;

pub trait RegisterServerReplicationSystem{
    fn register_server_replication_system<T: ServerReplicationSystem>(&mut self, schedule: impl ScheduleLabel);
}

#[allow(dead_code)]
#[derive(Component)]
pub struct ServerReplicator{
    pub(crate) owner: Option<Uuid>,
    pub(crate) replication_owner: Option<Uuid>,
    pub(crate) connection_id: u32,
    pub(crate) port: u32,
    pub(crate) just_for_authenticated: bool,
    pub(crate) send_args: Option<SendArgs>,
    pub(crate) bytes_queue: HashMap<TypeId, VecDeque<HashMap<u32, Vec<u8>>>>,
}

#[derive(Resource,Default)]
pub struct ServerSystemRegistry(pub(crate) u32, pub(crate) HashMap<u32, TypeId>, pub(crate) HashMap<TypeId, u32>);

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

        self.add_systems(schedule,(T::check_update,T::replicate_to_client));
    }
}

impl Plugin for ServerReplicate {
    fn build(&self, app: &mut App) {
        app.init_resource::<ServerSystemRegistry>();
    }
}