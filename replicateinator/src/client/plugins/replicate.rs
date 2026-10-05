use std::any::{TypeId};
use std::collections::{HashMap, VecDeque};
use std::ops::DerefMut;
use bevy::app::App;
use bevy::ecs::component::Mutable;
use bevy::ecs::lifecycle::HookContext;
use bevy::ecs::schedule::ScheduleLabel;
use bevy::ecs::world::DeferredWorld;
use bevy::log::warn;
use bevy::prelude::{Bundle, Commands, Component, Entity, First, FromReflect, IntoScheduleConfigs, MessageReader, PartialReflect, Plugin, Query, Resource, With};
use bevy::reflect::erased_serde::__private::serde::de::DeserializeOwned;
use bevy::reflect::erased_serde::__private::serde::Serialize;
use bevy::reflect::ReflectMut;
use networkinator::{NetRes, NetResMut};
use networkinator::shared::plugins::messaging::{check_messages_from_server, MessageReceivedFromServer, SendArgs};
use networkinator::shared::plugins::network::{CurrentNetworkSides, NetworkType};
use crate::shared::plugins::replicate::{ClientComponentRegistry, ClientResourceRegistry, ReplicateSystemSpawnedToClient, ReplicateSystemToClient, SendEntityRemovedForClient, SendResourceReplicatedForClient};

pub struct ReplicateClient;

#[allow(clippy::type_complexity)]
pub struct SystemDataFunctions{
    pub replicated_spawned: fn(commands: &mut Commands, entity: Entity, spawned: f64, components_bytes: &HashMap<u32,Vec<u8>>, client_component_registry: &ClientComponentRegistry),
    pub type_id: TypeId
}

pub trait RegisterClientReplicationSystem{
    fn register_client_replication_system<T: ClientReplicationSystem>(&mut self, schedule: impl ScheduleLabel);
    fn register_client_replication_resource<T: ClientResourceReplicationSystem>(&mut self);
}

#[derive(Resource,Default)]
pub struct ClientSystemRegistry(pub u32, pub HashMap<u32, SystemDataFunctions>, pub HashMap<TypeId, u32>);

#[derive(Resource,Default)]
pub struct EntitiesServerRefs(pub(crate) HashMap<Entity, Entity>);

#[allow(clippy::type_complexity)]
#[derive(Resource,Default)]
pub struct BytesQueueSystems(pub HashMap<Entity,HashMap<TypeId,VecDeque<HashMap<u32, Vec<u8>>>>>);

#[derive(Component)]
#[component(on_remove = replicated_removed)]
pub struct Replicated{
    ref_server: Entity,
    pub connection_id: u32,
    pub port_id: u32,
    pub send_args: Option<SendArgs>
}

#[warn(dead_code)]
impl Replicated {
    pub fn get_ref_server(&self) -> Entity {
        self.ref_server
    }
}

impl ClientSystemRegistry {
    pub fn get_system_id<T: ClientReplicationSystem>(&self) -> u32 {
        *self.2.get(&TypeId::of::<T>()).unwrap()
    }
}

pub trait ClientReplicationSystem: Default + Component + Sized + Component<Mutability = Mutable> {
    type Components: Bundle + Default + FromReflect;

    fn bytes_to_partials(components_bytes: &HashMap<u32,Vec<u8>>, client_component_registry: &ClientComponentRegistry) -> Vec<Box<dyn PartialReflect>> {
        let mut reflects: Vec<Box<dyn PartialReflect>> = Vec::new();

        for (id,bytes) in components_bytes {
            if let Some(client_component_data) = client_component_registry.2.get(id) {
                reflects.push((client_component_data.deserialize_fn)(bytes));
            }else {
                warn!("Couldnt find component id {}", id);
                continue
            }
        }

        reflects
    }

    fn partials_to_bundle(&self, reflects: Vec<Box<dyn PartialReflect>>) -> Self::Components {
        let mut bundle = Self::Components::default();

        if reflects.len() == 1 {
            let single_incoming = &reflects[0];
            if let Some(info) = single_incoming.get_represented_type_info()
                && info.type_id() == TypeId::of::<Self::Components>()
            {
                bundle.apply(&**single_incoming);
                return bundle;
            }
        }

        if let ReflectMut::Tuple(tuple_reflect) = bundle.reflect_mut() {
            for i in 0..tuple_reflect.field_len() {
                let field = tuple_reflect.field_mut(i).unwrap();

                if let Some(type_info) = field.get_represented_type_info() {
                    let field_type_id = type_info.type_id();

                    let matched_reflect = reflects.iter().find(|r| {
                        r.get_represented_type_info()
                            .map(|info| info.type_id() == field_type_id)
                            .unwrap_or(false)
                    });

                    if let Some(incoming_reflect) = matched_reflect {
                        field.apply(&**incoming_reflect);
                    }
                }
            }
        }

        bundle
    }

    fn partials_to_bundle_no_self(reflects: Vec<Box<dyn PartialReflect>>) -> Self::Components {
        let mut bundle = Self::Components::default();

        if reflects.len() == 1 {
            let single_incoming = &reflects[0];
            if let Some(info) = single_incoming.get_represented_type_info()
                && info.type_id() == TypeId::of::<Self::Components>()
            {
                bundle.apply(&**single_incoming);
                return bundle;
            }
        }

        if let ReflectMut::Tuple(tuple_reflect) = bundle.reflect_mut() {
            for i in 0..tuple_reflect.field_len() {
                let field = tuple_reflect.field_mut(i).unwrap();

                if let Some(type_info) = field.get_represented_type_info() {
                    let field_type_id = type_info.type_id();

                    let matched_reflect = reflects.iter().find(|r| {
                        r.get_represented_type_info()
                            .map(|info| info.type_id() == field_type_id)
                            .unwrap_or(false)
                    });

                    if let Some(incoming_reflect) = matched_reflect {
                        field.apply(&**incoming_reflect);
                    }
                }
            }
        }

        bundle
    }

    fn apply_replication(&self, commands: &mut Commands, entity: Entity, client_component_registry: &ClientComponentRegistry, components_bytes: &HashMap<u32, Vec<u8>>) {
        let reflects = Self::bytes_to_partials(components_bytes, client_component_registry);
        let bundle = self.partials_to_bundle(reflects);

        commands.entity(entity).insert(bundle);
    }

    fn apply_replication_from_components(&self, commands: &mut Commands, entity: Entity, components: Self::Components) {
        commands.entity(entity).insert(components);
    }

    fn insert_on_unit_spawned(commands: &mut Commands, entity: Entity, _spawned: f64, components_bytes: &HashMap<u32,Vec<u8>>, client_component_registry: &ClientComponentRegistry) {
        let new_system = Self::default();
        let reflects = Self::bytes_to_partials(components_bytes, client_component_registry);
        let bundle = new_system.partials_to_bundle(reflects);

        commands.entity(entity).insert((new_system,bundle));
    }

    #[allow(clippy::type_complexity)]
    fn new_bytes_from_server(
        mut query: Query<(Entity, &Self), (With<Self>, With<Replicated>)>,
        mut commands: Commands,
        client_component_registry: NetRes<ClientComponentRegistry>,
        mut bytes_queue_systems: NetResMut<BytesQueueSystems>,
    ){
        let type_id = TypeId::of::<Self>();
        for (entity, system) in query.iter_mut() {
            if let Some(mut entity_list) = bytes_queue_systems.0.remove(&entity)
            && let Some(system_queue) = entity_list.get_mut(&type_id)
                && let Some(component_bytes) = system_queue.pop_back()
            {
                system.apply_replication(&mut commands, entity, &client_component_registry, &component_bytes);
            }
        }
    }
}

pub trait ClientResourceReplicationSystem: Resource + Component<Mutability = Mutable> + Serialize + DeserializeOwned {
    fn deserialize_resource(bytes: &[u8]) -> Self {
        postcard::from_bytes::<Self>(bytes).unwrap()
    }

    fn new_bytes_from_server(
        mut resource: Option<NetResMut<Self>>,
        mut send_resource_replicated_for_client: MessageReader<MessageReceivedFromServer<SendResourceReplicatedForClient>>,
        client_resource_registry: NetRes<ClientResourceRegistry>,
        mut commands: Commands
    ) {
        let type_id = TypeId::of::<Self>();

        for ev in send_resource_replicated_for_client.read() {
            let send_resource_replicated_for_client_message = &ev.message;

            if let Some(resource_type_id) = client_resource_registry.2.get(&send_resource_replicated_for_client_message.resource_id) {
                if resource_type_id != &type_id { continue; }

                let new_self = Self::deserialize_resource(&send_resource_replicated_for_client_message.resource_bytes);

                if let Some(resource) = &mut resource {
                    *resource.deref_mut() = new_self;
                }else {
                    commands.insert_resource(new_self);
                }
            }
        }
    }
}

fn bytes_from_server(
    mut replicate_system_to_client: MessageReader<MessageReceivedFromServer<ReplicateSystemToClient>>,
    mut replicate_system_spawned_to_client: MessageReader<MessageReceivedFromServer<ReplicateSystemSpawnedToClient>>,
    mut commands: Commands,
    client_system_registry: NetRes<ClientSystemRegistry>,
    mut entities_server_refs: NetResMut<EntitiesServerRefs>,
    mut bytes_queue_systems: NetResMut<BytesQueueSystems>,
    client_component_registry: NetRes<ClientComponentRegistry>
){
    for ev in replicate_system_spawned_to_client.read() {
        let replicate_system_to_client_message = &ev.message;
        let entity_ref = replicate_system_to_client_message.entity;

        if let Some(system_data_functions) = client_system_registry.1.get(&replicate_system_to_client_message.system_id)
        && !entities_server_refs.0.contains_key(&entity_ref)
        {
            let new_entity = commands.spawn(Replicated{
                ref_server: entity_ref,
                connection_id: ev.connection_id,
                port_id: ev.port_id,
                send_args: None
            });
            let entity = new_entity.id();
            let current_entity = entity;
            let replicated_spawned = system_data_functions.replicated_spawned;
            let time = replicate_system_to_client_message.time;

            replicated_spawned(&mut commands, current_entity, time, &replicate_system_to_client_message.components_bytes, &client_component_registry);

            entities_server_refs.0.insert(entity_ref,entity);
        }
    }

    for ev in replicate_system_to_client.read() {
        let replicate_system_to_client_message = &ev.message;
        let entity_ref = replicate_system_to_client_message.entity;

        if let Some(system_data_functions) = client_system_registry.1.get(&replicate_system_to_client_message.system_id)
            && let Some(current_entity) = entities_server_refs.0.get(&entity_ref)
        {
            let component_bytes = replicate_system_to_client_message.components_bytes.clone();
            let type_id = system_data_functions.type_id;

            if let Some(entity_bytes) = bytes_queue_systems.0.get_mut(current_entity) {
                if let Some(system_queue) = entity_bytes.get_mut(&type_id) {
                    system_queue.push_back(component_bytes);
                }else {
                    entity_bytes.insert(type_id, VecDeque::from(vec![component_bytes]));
                }
            }else {
                bytes_queue_systems.0.insert(entity_ref,HashMap::from([
                    (type_id,VecDeque::from(vec![component_bytes]))
                ]));
            }
        }
    }
}

impl Plugin for ReplicateClient{
    fn build(&self, app: &mut App) {
        app.init_resource::<BytesQueueSystems>();
        app.init_resource::<ClientSystemRegistry>();
        app.init_resource::<EntitiesServerRefs>();
        app.add_systems(First,bytes_from_server.after(check_messages_from_server));
        app.add_systems(First,entities_removed_from_server);
    }
}

impl RegisterClientReplicationSystem for App {
    fn register_client_replication_system<T: ClientReplicationSystem>(&mut self, schedule: impl ScheduleLabel) {
        let type_id = TypeId::of::<T>();
        let mut client_system_registry = self.world_mut().resource_mut::<ClientSystemRegistry>();

        if client_system_registry.2.contains_key(&type_id) {
            return;
        }

        let new_index = client_system_registry.0 + 1;

        client_system_registry.0 = new_index;

        client_system_registry.1.insert(new_index,SystemDataFunctions{
            replicated_spawned: T::insert_on_unit_spawned,
            type_id
        });

        client_system_registry.2.insert(type_id,new_index);

        self.add_systems(schedule,T::new_bytes_from_server.after(check_messages_from_server));
    }

    fn register_client_replication_resource<T: ClientResourceReplicationSystem>(&mut self) {
        let type_id = TypeId::of::<T>();

        let is_client = {
            let world = self.world_mut();
            let mut sides = world.get_resource_mut::<CurrentNetworkSides>()
                .expect("Insert ClientNetworkPlugin first");

            sides.side().contains(&NetworkType::Client)
        };

        if !is_client {
            return;
        }

        let world_mut = self.world_mut();
        let mut client_resource_registry = world_mut.resource_mut::<ClientResourceRegistry>();

        if client_resource_registry.1.contains_key(&type_id) {
            return;
        }

        let new_index = client_resource_registry.0 + 1;

        client_resource_registry.0 = new_index;
        client_resource_registry.1.insert(type_id,new_index);
        client_resource_registry.2.insert(new_index,type_id);

        self.add_systems(First,T::new_bytes_from_server.after(check_messages_from_server).before(entities_removed_from_server));
    }
}

fn entities_removed_from_server(
    entities_server_refs: NetRes<EntitiesServerRefs>,
    mut send_entity_removed_for_client: MessageReader<MessageReceivedFromServer<SendEntityRemovedForClient>>,
    mut commands: Commands,
){
    for ev in send_entity_removed_for_client.read() {
        let send_entity_removed_for_client_message = &ev.message;

        if let Some(entity) = entities_server_refs.0.get(&send_entity_removed_for_client_message.entity) {
            commands.entity(*entity).despawn();
        }
    }
}

fn replicated_removed(
    mut world: DeferredWorld,
    context: HookContext
){
    if let Some(ref_server) = world.get::<Replicated>(context.entity).map(|r| r.ref_server) {
        let mut entities_server_refs = world.resource_mut::<EntitiesServerRefs>();

        entities_server_refs.0.remove(&ref_server);
    }
}