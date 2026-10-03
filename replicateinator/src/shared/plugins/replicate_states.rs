use networkinator::shared::plugins::messaging::{check_messages_from_server, MessageReceivedFromServer, MessageTrait, MessageTraitPlugin, SendArgs, ServerConnectionParams};
use std::any::TypeId;
use std::collections::HashMap;
use bevy::app::App;
use bevy::ecs::schedule::ScheduleLabel;
use bevy::ecs::system::BoxedSystem;
use bevy::prelude::{state_changed, IntoSystem, Plugin, Res, Resource, State, States, IntoScheduleConfigs, World, NextState, MessageReader, Commands, First};
use bevy::state::state::FreelyMutableState;
use networkinator::ConnectionMessage;
use networkinator::NetRes;
use networkinator::shared::plugins::network::{CurrentNetworkSides, LocalPeerUUID, NetworkType};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

pub struct ReplicateStates;

pub struct ServerStatesData {
    pub connection_id: u32,
    pub port_id: u32,
    pub send_args: Option<SendArgs>,
    pub just_authenticated: bool
}

pub struct ClientStatesData {
    pub bytes_received_fn: fn(bytes: Vec<u8>, world: &mut World)
}

fn default_send_for_peers_when_changed<T: ServerStateSystem>(
    state: Res<State<T>>,
    mut server_connection_params: ServerConnectionParams,
    local_peer_uuid: Option<NetRes<LocalPeerUUID>>,
    server_states_registry: Res<ServerStatesRegistry>
){
    let current_state = state.get();
    let state_id = server_states_registry.2.get(&TypeId::of::<T>()).unwrap();
    let server_state_data = server_states_registry.1.get(state_id).unwrap();

    server_connection_params.send_message_for_all(server_state_data.connection_id,server_state_data.port_id,ReplicateStateForPeer{
        state_id: *state_id,
        bytes: current_state.serialize_state(),
    },server_state_data.just_authenticated,server_state_data.send_args.as_ref(), if let Some(local_peer_uuid) = &local_peer_uuid && let Some(local_peer_uuid) = local_peer_uuid.get_peer_uuid() {vec![local_peer_uuid]} else { vec![] } );
}

pub trait ServerStateSystem: Serialize + DeserializeOwned + States {
    fn serialize_state(&self) -> Vec<u8> {
        postcard::to_allocvec(&self).unwrap()
    }

    fn on_state_changed() -> BoxedSystem<(), ()> {
        Box::new(IntoSystem::into_system(default_send_for_peers_when_changed::<Self>))
    }
}

pub trait ClientStateSystem: Serialize + DeserializeOwned + States + FreelyMutableState {
    fn deserialize_state(bytes: Vec<u8>) -> Self {
        postcard::from_bytes::<Self>(&bytes).unwrap()
    }

    fn new_bytes_from_server(bytes: Vec<u8>, world: &mut World) {
        if let Some(mut next_state) = world.get_resource_mut::<NextState<Self>>() {
            let state = Self::deserialize_state(bytes);
            next_state.set(state);
        }
    }
}

pub trait ReplicatedStateSharedTrait {
    fn register_server_replication_state<T: ServerStateSystem>(&mut self, schedule: impl ScheduleLabel, server_states_data: ServerStatesData);
    fn register_client_replication_state<T: ClientStateSystem>(&mut self);
}

#[derive(Resource,Default)]
pub struct ServerStatesRegistry(pub(crate) u32, pub(crate) HashMap<u32, ServerStatesData>, pub(crate) HashMap<TypeId,u32>);

#[derive(Resource,Default)]
pub struct ClientStatesRegistry(pub(crate) u32, pub(crate) HashMap<u32, ClientStatesData>, pub(crate) HashMap<TypeId,u32>);

#[derive(States, Clone, PartialEq, Eq, Hash, Debug, Serialize)]
pub enum NormalStates{
    Paused,
    Go
}

#[derive(Serialize,Deserialize,ConnectionMessage)]
pub struct ReplicateStateForPeer{
    pub state_id: u32,
    pub bytes: Vec<u8>
}

impl Plugin for ReplicateStates {
    fn build(&self, app: &mut App) {
        app.register_message::<ReplicateStateForPeer>();

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
            app.init_resource::<ServerStatesRegistry>();
        }

        if is_client {
            app.init_resource::<ClientStatesRegistry>();

            app.add_systems(First,state_bytes_from_server.after(check_messages_from_server));
        }
    }
}

impl ReplicatedStateSharedTrait for App {
    fn register_server_replication_state<T: ServerStateSystem>(&mut self, schedule: impl ScheduleLabel, server_states_data: ServerStatesData) {
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
        let mut server_states_registry = world_mut.resource_mut::<ServerStatesRegistry>();

        if server_states_registry.2.contains_key(&type_id) {
            return;
        }

        let new_index = server_states_registry.0 + 1;

        server_states_registry.0 = new_index;
        server_states_registry.1.insert(new_index, server_states_data);
        server_states_registry.2.insert(type_id, new_index);

        self.add_systems(schedule,T::on_state_changed().run_if(state_changed::<T>));
    }

    fn register_client_replication_state<T: ClientStateSystem>(&mut self) {
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
        let mut client_states_registry = world_mut.resource_mut::<ClientStatesRegistry>();

        if client_states_registry.2.contains_key(&type_id) {
            return;
        }

        let new_index = client_states_registry.0 + 1;

        client_states_registry.0 = new_index;
        client_states_registry.1.insert(new_index, ClientStatesData{
            bytes_received_fn: T::new_bytes_from_server,
        });
        client_states_registry.2.insert(type_id, new_index);
    }
}

pub fn state_bytes_from_server(
    mut replicate_state_for_peer: MessageReader<MessageReceivedFromServer<ReplicateStateForPeer>>,
    client_states_registry: NetRes<ClientStatesRegistry>,
    mut commands: Commands,
){
    for ev in replicate_state_for_peer.read() {
        let replicate_state_for_peer_message = &ev.message;
        let bytes = Vec::from(&*replicate_state_for_peer_message.bytes);
        let client_states_data = client_states_registry.1.get(&replicate_state_for_peer_message.state_id).unwrap();
        let bytes_received_fn = client_states_data.bytes_received_fn;

        commands.queue(move |world: &mut World| {
            bytes_received_fn(bytes, world);
        });
    }
}