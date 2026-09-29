use std::any::{TypeId};
use std::collections::{HashMap, VecDeque};
use bevy::app::App;
use bevy::ecs::lifecycle::HookContext;
use bevy::ecs::schedule::ScheduleLabel;
use bevy::ecs::world::DeferredWorld;
use bevy::log::warn;
use bevy::prelude::{Bundle, Commands, Component, Entity, First, FromReflect, MessageReader, PartialReflect, Plugin, PreUpdate, Query, Res, ResMut, Resource, With, World};
use bevy::reflect::ReflectMut;
use networkinator::shared::plugins::messaging::MessageReceivedFromServer;
use crate::shared::plugins::replicate::{ClientComponentRegistry, ReplicateSystemToClient, SendEntityRemovedForClient};

pub struct ReplicateClient;

pub struct SystemDataFunctions{
    pub replicated_spawned: fn(world: &mut World, entity: Entity),
    pub type_id: TypeId
}

pub trait RegisterClientReplicationSystem{
    fn register_client_replication_system<T: ClientReplicationSystem>(&mut self, schedule: impl ScheduleLabel);
}

#[derive(Resource,Default)]
pub struct ClientSystemRegistry(pub(crate) u32, pub(crate) HashMap<u32, SystemDataFunctions>, pub(crate) HashMap<TypeId, u32>);

#[derive(Resource,Default)]
pub struct EntitiesServerRefs(pub(crate) HashMap<Entity, Entity>);

#[derive(Component)]
#[component(on_remove = replicated_removed)]
pub struct Replicated{
    bytes_queue: HashMap<TypeId, VecDeque<HashMap<u32, Vec<u8>>>>,
    ref_server: Entity
}

pub trait ClientReplicationSystem: Default + Component + Sized{
    type Components: Bundle + Default + FromReflect;

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
            if let Some(info) = single_incoming.get_represented_type_info() {
                if info.type_id() == TypeId::of::<Self::Components>() {
                    bundle.apply(&**single_incoming);
                    return bundle;
                }
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

    fn insert_on_unit_spawned(world: &mut World, entity: Entity) {
        let mut entity_mut = world.get_entity_mut(entity).unwrap();

        entity_mut.insert(Self::default());
    }

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

fn bytes_from_server(
    mut replicate_system_to_client: MessageReader<MessageReceivedFromServer<ReplicateSystemToClient>>,
    mut commands: Commands,
    client_system_registry: Res<ClientSystemRegistry>,
    mut entities_server_refs: ResMut<EntitiesServerRefs>
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
                });
                let entity = new_entity.id();
                let current_entity = *&entity;
                let replicated_spawned = system_data_functions.replicated_spawned;

                commands.queue(move |world: &mut World| {
                    replicated_spawned(world, current_entity);
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
        app.add_systems(PreUpdate,bytes_from_server);
        app.add_systems(First,entities_removed_from_server);
    }
}

impl RegisterClientReplicationSystem for App {
    fn register_client_replication_system<T: ClientReplicationSystem>(&mut self, schedule: impl ScheduleLabel) {
        let type_id = TypeId::of::<T>();
        let mut client_system_registry = self.world_mut().resource_mut::<ClientSystemRegistry>();

        if client_system_registry.2.get(&type_id).is_some() {
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
}

fn entities_removed_from_server(
    entities_server_refs: Res<EntitiesServerRefs>,
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
    if let Some(ref_server) = world.get::<Replicated>(context.entity).map(|r| r.ref_server.clone()) {
        let mut entities_server_refs = world.resource_mut::<EntitiesServerRefs>();

        entities_server_refs.0.remove(&ref_server);
    }
}