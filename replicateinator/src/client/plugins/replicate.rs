use std::any::{TypeId};
use std::collections::{HashMap, VecDeque};
use bevy::app::App;
use bevy::ecs::component::Mutable;
use bevy::ecs::lifecycle::HookContext;
use bevy::ecs::schedule::ScheduleLabel;
use bevy::ecs::world::DeferredWorld;
use bevy::log::warn;
use bevy::prelude::{AppTypeRegistry, Bundle, Commands, Component, Entity, First, FromReflect, IntoScheduleConfigs, MessageReader, PartialReflect, Plugin, Query, ReflectComponent, Resource, With, World};
use bevy::reflect::erased_serde::__private::serde::de::DeserializeOwned;
use bevy::reflect::erased_serde::__private::serde::Serialize;
use bevy::reflect::ReflectMut;
use networkinator::{NetRes, NetResMut};
use networkinator::shared::plugins::messaging::{check_messages_from_server, MessageReceivedFromServer};
use networkinator::shared::plugins::network::{CurrentNetworkSides, NetworkType};
use crate::shared::plugins::replicate::{ClientComponentRegistry, ClientResourceData, ClientResourceRegistry, ReplicateSystemToClient, SendEntityRemovedForClient, SendResourceReplicatedForClient};

pub struct ReplicateClient;

pub struct SystemDataFunctions{
    pub replicated_spawned: fn(world: &mut World, entity: Entity, time: f64),
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

#[derive(Component)]
#[component(on_remove = replicated_removed)]
pub struct Replicated{
    pub bytes_queue: HashMap<TypeId, VecDeque<HashMap<u32, Vec<u8>>>>,
    ref_server: Entity,
    pub connection_id: u32,
    pub port_id: u32
}

pub trait ClientReplicationSystem: Default + Component + Sized + Component<Mutability = Mutable> {
    type Components: Bundle + Default + FromReflect;

    fn get_current_components(world: &World, entity: Entity) -> Option<Self::Components> {
        let entity_ref = world.get_entity(entity).ok()?;
        let type_registry = world.resource::<AppTypeRegistry>().read();

        let mut bundle = Self::Components::default();

        if let Some(type_info) = bundle.get_represented_type_info()
            && let Some(reflect_comp) = type_registry.get_type_data::<ReflectComponent>(type_info.type_id())
            && let Some(comp_reflect) = reflect_comp.reflect(entity_ref)
        {
            bundle.apply(comp_reflect);
            return Some(bundle);
        }

        match bundle.reflect_mut() {
            ReflectMut::Tuple(tuple_reflect) => {
                for i in 0..tuple_reflect.field_len() {
                    let field = tuple_reflect.field_mut(i).unwrap();

                    if let Some(type_info) = field.get_represented_type_info()
                        && let Some(reflect_comp) = type_registry.get_type_data::<ReflectComponent>(type_info.type_id())
                        && let Some(comp_reflect) = reflect_comp.reflect(entity_ref)

                    {
                        field.apply(comp_reflect);
                    }
                }
            }
            ReflectMut::Struct(struct_reflect) => {
                for i in 0..struct_reflect.field_len() {
                    if let Some(field) = struct_reflect.field_at_mut(i)
                        && let Some(type_info) = field.get_represented_type_info()
                        && let Some(reflect_comp) = type_registry.get_type_data::<ReflectComponent>(type_info.type_id())
                        && let Some(comp_reflect) = reflect_comp.reflect(entity_ref)
                    {
                        field.apply(comp_reflect);
                    }
                }
            }
            _ => {}
        }

        Some(bundle)
    }

    fn bytes_to_partials(world: &mut World, components_bytes: HashMap<u32,Vec<u8>>) -> Vec<Box<dyn PartialReflect>> {
        let mut reflects: Vec<Box<dyn PartialReflect>> = Vec::new();
        let client_component_registry = world.resource_mut::<ClientComponentRegistry>();

        for (id,bytes) in components_bytes {
            if let Some(client_component_data) = client_component_registry.2.get(&id) {
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

    fn apply_replication(world: &mut World, entity: Entity, components_bytes: HashMap<u32, Vec<u8>>) {
        let reflects = Self::bytes_to_partials(world, components_bytes);
        let self_system = world.get::<Self>(entity).unwrap();
        let bundle = self_system.partials_to_bundle(reflects);

        if let Ok(mut entity_mut) = world.get_entity_mut(entity) {
            entity_mut.insert(bundle);
        } else {
            warn!("Entity {:?} não encontrada ao aplicar replicação", entity);
        }
    }

    fn apply_replication_from_components(world: &mut World, entity: Entity, components: Self::Components) {
        if let Ok(mut entity_mut) = world.get_entity_mut(entity) {
            entity_mut.insert(components);
        }
    }

    fn insert_on_unit_spawned(world: &mut World, entity: Entity, _spawned: f64) {
        let mut entity_mut = world.get_entity_mut(entity).unwrap();

        entity_mut.insert(Self::default());
    }

    #[allow(clippy::type_complexity)]
    fn new_bytes_from_server(
        mut query: Query<(Entity, &Self, &mut Replicated), (With<Self>, With<Replicated>)>,
        mut commands: Commands,
    ){
        for (entity, _, mut replicated) in query.iter_mut() {
            if let Some(system_queue) = replicated.bytes_queue.remove(&TypeId::of::<Self>()) {
                commands.queue(move |world: &mut World| {
                    for component_bytes in system_queue {
                        Self::apply_replication(world, entity, component_bytes);
                    }
                });
            }
        }
    }
}

pub trait ClientResourceReplicationSystem: Resource + Component<Mutability = Mutable> + Serialize + DeserializeOwned {
    fn deserialize_resource(bytes: Vec<u8>) -> Self {
        postcard::from_bytes::<Self>(&bytes).unwrap()
    }

    fn new_bytes_from_server(bytes: Vec<u8>, world: &mut World) {
        if let Some(mut resource) = world.get_resource_mut::<Self>() {
            let new_self = Self::deserialize_resource(bytes);

            *resource = new_self;
        }else{
            world.insert_resource(Self::deserialize_resource(bytes));
        }
    }
}

fn bytes_from_server(
    mut replicate_system_to_client: MessageReader<MessageReceivedFromServer<ReplicateSystemToClient>>,
    mut commands: Commands,
    client_system_registry: NetRes<ClientSystemRegistry>,
    mut entities_server_refs: NetResMut<EntitiesServerRefs>
){
    for ev in replicate_system_to_client.read() {
        let replicate_system_to_client_message = &ev.message;
        let entity_ref = replicate_system_to_client_message.entity;

        if let Some(system_data_functions) = client_system_registry.1.get(&replicate_system_to_client_message.system_id) {
            if let Some(current_entity) = entities_server_refs.0.get(&entity_ref) {
                let component_bytes = replicate_system_to_client_message.components_bytes.clone();
                let current_entity = *current_entity;
                let type_id = system_data_functions.type_id;

                commands.queue(move |world: &mut World| {
                    let mut replicated = world.get_mut::<Replicated>(current_entity).unwrap();

                    if let Some(system_queue) = replicated.bytes_queue.get_mut(&type_id) {
                        system_queue.push_back(component_bytes);
                    }else {
                        replicated.bytes_queue.insert(type_id, VecDeque::from(vec![component_bytes]));
                    }
                });
            }else {
                let new_vec_dequeue = VecDeque::from(vec![replicate_system_to_client_message.components_bytes.clone()]);
                let new_entity = commands.spawn(Replicated{
                    bytes_queue: HashMap::from([
                        (system_data_functions.type_id,new_vec_dequeue)
                    ]),
                    ref_server: entity_ref,
                    connection_id: ev.connection_id,
                    port_id: ev.port_id
                });
                let entity = new_entity.id();
                let current_entity = entity;
                let replicated_spawned = system_data_functions.replicated_spawned;
                let time = replicate_system_to_client_message.spawned;

                commands.queue(move |world: &mut World| {
                    replicated_spawned(world, current_entity, time);
                });

                entities_server_refs.0.insert(entity_ref,entity);
            }
        }
    }
}

impl Plugin for ReplicateClient{
    fn build(&self, app: &mut App) {
        app.init_resource::<ClientSystemRegistry>();
        app.init_resource::<EntitiesServerRefs>();
        app.add_systems(First,(bytes_from_server,resources_bytes_from_server).after(check_messages_from_server));
        app.add_systems(First,entities_removed_from_server.after(resources_bytes_from_server));
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

        self.add_systems(schedule,T::new_bytes_from_server);
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
        client_resource_registry.2.insert(new_index,ClientResourceData{
            new_bytes_from_server: T::new_bytes_from_server,
        });
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

fn resources_bytes_from_server(
    mut send_resource_replicated_for_client: MessageReader<MessageReceivedFromServer<SendResourceReplicatedForClient>>,
    client_resource_registry: NetRes<ClientResourceRegistry>,
    mut commands: Commands
){
    for ev in send_resource_replicated_for_client.read() {
        let send_resource_replicated_for_client_message = &ev.message;

        if let Some(client_resource_data) = client_resource_registry.2.get(&send_resource_replicated_for_client_message.resource_id) {
            let bytes = &send_resource_replicated_for_client_message.resource_bytes;
            let vec = bytes.to_vec();
            let new_bytes_from_server = client_resource_data.new_bytes_from_server;

            commands.queue(move |world: &mut World| {
                new_bytes_from_server(vec, world);
            });
        }
    }
}