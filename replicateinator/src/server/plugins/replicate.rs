use std::any::TypeId;
use networkinator::shared::plugins::messaging::{SendArgs, ServerConnectionParams};
use std::collections::{HashMap, VecDeque};
use bevy::app::App;
use bevy::asset::uuid::Uuid;
use bevy::ecs::bundle::BundleId;
use bevy::ecs::component::Mutable;
use bevy::ecs::lifecycle::HookContext;
use bevy::ecs::query::{IterQueryData, QueryData, QueryFilter};
use bevy::ecs::schedule::ScheduleLabel;
use bevy::ecs::system::BoxedSystem;
use bevy::ecs::world::DeferredWorld;
use bevy::log::warn;
use bevy::prelude::{Added, Bundle, Changed, Component, Entity, FromReflect, IntoScheduleConfigs, IntoSystem, Last, Message, MessageReader, Or, Plugin, Query, Reflect, Resource, Single, Time, With};
use networkinator::NetRes;
use networkinator::shared::plugins::network::{CurrentNetworkSides, LocalPeerUUID, NetworkType};
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::shared::plugins::replicate::{ReplicateSystemSpawnedToClient, ReplicateSystemToClient, SendEntityRemovedForClient, SendResourceReplicatedForClient, ServerComponentRegistry, ServerResourceData, ServerResourceRegistry};

pub struct ReplicateServer;

pub trait RegisterServerReplicationSystem{
    fn register_server_replication_system<T: ServerReplicationSystem>(&mut self, schedule: impl ScheduleLabel);
    fn register_server_replication_resource<T: ServerResourceReplicationSystem>(&mut self, schedule: impl ScheduleLabel, server_resource_data: ServerResourceData);
}

pub struct SpawnedQueueData {
    pub peers: Vec<Uuid>,
    pub bytes: HashMap<u32, Vec<u8>>
}

pub struct ServerSystemData{
    pub type_id: TypeId,
    pub bundle_id: BundleId
}

#[allow(dead_code)]
#[derive(Component,Default)]
#[component(on_add = replicator_added, on_remove = replicator_removed)]
pub struct ServerReplicator{
    pub owner: Option<Uuid>,
    pub replication_owner: Option<Uuid>,
    pub connection_id: u32,
    pub port: u32,
    pub port_to_remove: u32,
    pub port_to_spawn: u32,
    pub just_for_authenticated: bool,
    pub send_args_to_remove: Option<SendArgs>,
    pub send_args_to_replicate: Option<SendArgs>,
    pub send_args_to_spawn: Option<SendArgs>,
    pub bytes_queue: HashMap<TypeId, VecDeque<HashMap<u32, Vec<u8>>>>,
    pub bytes_spawned_queue: HashMap<TypeId, VecDeque<HashMap<u32, Vec<u8>>>>
}

#[derive(Message)]
pub struct ReplicatorRemoved{
    pub entity: Entity,
    pub port_to_remove: u32,
    pub connection_id: u32,
    pub send_args: Option<SendArgs>
}

#[derive(Resource,Default)]
pub struct ServerSystemRegistry(pub u32, pub HashMap<u32, ServerSystemData>, pub HashMap<TypeId, u32>);

#[derive(Resource,Default)]
pub struct EntitiesPeerList(pub HashMap<Uuid,HashMap<Entity,bool>>);

pub trait ServerResourceReplicationSystem: Resource + Serialize + DeserializeOwned  {
    fn serialize_resource(&self) -> Vec<u8> {
        postcard::to_allocvec(&self).unwrap()
    }

    fn on_resource_changed() -> BoxedSystem<(), ()> {
        Box::new(IntoSystem::into_system(default_resource_changed::<Self>))
    }
}

impl ServerSystemRegistry {
    pub fn get_system_id<T: ServerReplicationSystem>(&self) -> u32 {
        *self.2.get(&TypeId::of::<T>()).unwrap()
    }
}

pub trait ServerReplicationSystem: Component + Sized + Component<Mutability = Mutable> {
    type Components: Bundle + FromReflect;
    type UpdateQueryData: QueryData + IterQueryData;
    type UpdateQueryFilter: QueryFilter;

    fn on_spawned(world: DeferredWorld, context: HookContext);

    fn check_update(query: Query<Self::UpdateQueryData,Self::UpdateQueryFilter>, server_component_registry: NetRes<ServerComponentRegistry>);

    fn add_bytes_to_queue(&self, _entity: Entity, components_bytes: HashMap<u32, Vec<u8>>, spawned: bool, replicator: &mut ServerReplicator) {
        let type_id = TypeId::of::<Self>();

        if spawned {
            if let Some(current_spawned_queue) = replicator.bytes_spawned_queue.get_mut(&type_id) {
                current_spawned_queue.push_back(components_bytes);
            }else {
                let mut current_spawned_queue = VecDeque::new();

                current_spawned_queue.push_back(components_bytes);

                replicator.bytes_spawned_queue.insert(type_id, current_spawned_queue);
            }
        }else {
            if let Some(current_queue) = replicator.bytes_queue.get_mut(&type_id) {
                current_queue.push_back(components_bytes);
            }else {
                let mut current_queue = VecDeque::new();

                current_queue.push_back(components_bytes);

                replicator.bytes_queue.insert(type_id, current_queue);
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn replicate_to_client(
        mut query: Query<(Entity, &Self, &mut ServerReplicator), (With<Self>, With<ServerReplicator>)>,
        mut server_connection_params: ServerConnectionParams,
        server_system_registry: NetRes<ServerSystemRegistry>,
        local_peer_uuid: Option<NetRes<LocalPeerUUID>>,
        time: NetRes<Time>
    ){
        let type_id = TypeId::of::<Self>();
        let system_id = *server_system_registry.2.get(&type_id).unwrap();

        for (entity, _, mut server_replicator) in query.iter_mut() {
            if let Some(current_spawned_queue) = server_replicator.bytes_spawned_queue.remove(&type_id) {
                for components_bytes in current_spawned_queue {
                    server_connection_params.send_message_for_all(server_replicator.connection_id,server_replicator.port_to_spawn,ReplicateSystemSpawnedToClient{
                        owner: server_replicator.owner,
                        components_bytes,
                        entity,
                        system_id,
                        time: time.delta_secs_f64(),
                    }, server_replicator.just_for_authenticated, server_replicator.send_args_to_spawn.as_ref(), if let Some(local_peer_uuid) = &local_peer_uuid && let Some(local_peer_uuid) = local_peer_uuid.get_peer_uuid() {vec![local_peer_uuid]} else { vec![] } );
                }
            }

            if let Some(current_queue) = server_replicator.bytes_queue.remove(&type_id) {
                for components_bytes in current_queue {
                    server_connection_params.send_message_for_all(server_replicator.connection_id,server_replicator.port,ReplicateSystemToClient{
                        components_bytes,
                        entity,
                        system_id,
                    }, server_replicator.just_for_authenticated, server_replicator.send_args_to_replicate.as_ref(), if let Some(local_peer_uuid) = &local_peer_uuid && let Some(local_peer_uuid) = local_peer_uuid.get_peer_uuid() {vec![local_peer_uuid]} else { vec![] } );
                }
            }
        }
    }

    fn prepare_replication_bytes(
        &self,
        components: &[&dyn Reflect],
        server_component_registry: &ServerComponentRegistry,
    ) -> HashMap<u32, Vec<u8>> {
        let mut components_bytes = HashMap::new();

        for reflect_comp in components {
            if let Some(type_info) = reflect_comp.get_represented_type_info() {
                let type_id = type_info.type_id();

                if let Some(id) = server_component_registry.1.get(&type_id)
                    && let Some(data) = server_component_registry.2.get(id)
                {
                    let bytes = (data.serialize_fn)(*reflect_comp); // Repare no *reflect_comp
                    components_bytes.insert(*id, bytes.unwrap());
                } else {
                    warn!("Componente com TypeId {:?} não registrado", type_id);
                }
            }
        }

        components_bytes
    }
}

impl RegisterServerReplicationSystem for App {
    fn register_server_replication_system<T: ServerReplicationSystem>(&mut self, schedule: impl ScheduleLabel) {
        let type_id = TypeId::of::<T>();

        let is_registered = {
            let server_system_registry = self.world().resource::<ServerSystemRegistry>();

            server_system_registry.2.contains_key(&type_id)
        };

        if is_registered {
            return;
        }

        let bundle_id = self.world_mut().register_bundle::<T::Components>().id();
        let mut server_system_registry = self.world_mut().resource_mut::<ServerSystemRegistry>();

        let new_index = server_system_registry.0 + 1;

        server_system_registry.0 = new_index;
        server_system_registry.1.insert(new_index,ServerSystemData{
            type_id,
            bundle_id,
        });
        server_system_registry.2.insert(type_id,new_index);

        self.world_mut().register_component_hooks::<T>().on_insert(T::on_spawned);

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
        send_args = replicator.send_args_to_remove.take();

        let mut entities_peer_list = world.resource_mut::<EntitiesPeerList>();

        if let Some(entities) = entities_peer_list.0.get_mut(&peer_uuid){
            entities.remove(&context.entity);

            removed = true;

            if entities.is_empty() {
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

#[allow(clippy::type_complexity)]
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