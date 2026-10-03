use bevy::prelude::{App};
pub(crate) use bevy::DefaultPlugins;

#[cfg(target_arch = "wasm32")]
use bevy::log::warn;

#[cfg(not(target_arch = "wasm32"))]
pub mod not_wasm_uses {
    pub(crate) use networkinator::shared::plugins::messaging::{MessageReceivedFromPeer, MessageTrait, MessageTraitPlugin, MessagingPlugin};
    pub(crate) use bevy::app::{Update,PreUpdate};
    pub(crate) use bevy::prelude::{MessageReader, Startup, NextState, Resource};
    pub(crate) use serde::{Deserialize, Serialize};
    pub(crate) use networkinator::ConnectionMessage;
    pub(crate) use networkinator::NetResMut;
    pub(crate) use networkinator::server::plugins::network::ServerNetworkPlugin;
    pub(crate) use networkinator::server::ports::tcp::TcpServerSettings;
    pub(crate) use networkinator::server::ports::udp::UdpServerSettings;
    pub(crate) use networkinator::shared::plugins::authentication::AuthenticationPlugin;
    pub(crate) use networkinator::shared::plugins::network::{DefaultNetworkPortSharedInfosServer, NetworkConnection, NetworkPlugin, ServerConnection};
    pub(crate) use replicateinator::shared::plugins::replicate::{ReplicateShared, ReplicationSharedTrait};
    pub(crate) use replication_pro_macro::{ServerReplicationSystem, ServerStateReplicationSystem};
    pub(crate) use networkinator::shared::plugins::authentication::ClientAuthenticatedOnServer;
    pub(crate) use replicateinator::shared::plugins::replicate_states::ServerStateSystem;
    pub(crate) use replicateinator::server::plugins::replicate::{RegisterServerReplicationSystem, ReplicateServer, ServerReplicationSystem, ServerReplicator};
    pub(crate) use bevy::prelude::{Commands, Component, Last, PostUpdate, Reflect, States, AppExtStates};
    pub(crate) use replicateinator::shared::plugins::replicate_states::ReplicateStates;
    pub(crate) use replicateinator::shared::plugins::replicate_states::ReplicatedStateSharedTrait;
    pub(crate) use replicateinator::server::plugins::replicate::ServerResourceReplicationSystem;
    pub(crate) use replication_pro_macro::ServerResourceReplicationSystem;
    pub(crate) use replicateinator::shared::plugins::replicate::ServerResourceData;
    pub(crate) use replicateinator::shared::plugins::replicate_states::ServerStatesData;
}

#[cfg(not(target_arch = "wasm32"))]
use not_wasm_uses::*;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Serialize,Deserialize,ConnectionMessage)]
pub struct HiMessage(String);

#[derive(Component,Serialize,Deserialize,Reflect)]
pub struct Health{
    health: f32,
    max_health: f32,
}

#[derive(Component,Serialize,Deserialize,Reflect)]
pub struct Mana{
    mana: f32,
    max_mana: f32,
}

#[derive(Component,ServerReplicationSystem)]
#[replication(Health, Mana)]
pub struct DefaultManaHealthSystem;

#[cfg(not(target_arch = "wasm32"))]
fn start_connection(
    mut network_connection: NetResMut<NetworkConnection<ServerConnection>>,
) {
    network_connection.start_connection::<DefaultNetworkPortSharedInfosServer>(0, 0, Box::new(TcpServerSettings::default()),true);
    network_connection.open_secondary_port(0, Box::new(UdpServerSettings::default().with_port(8070)));
}

#[cfg(not(target_arch = "wasm32"))]
fn read_hi_message(
    mut client_port_connected: MessageReader<MessageReceivedFromPeer<HiMessage>>,
){
    for event in client_port_connected.read() {
        println!("Message from client: {:?}, on port {}, from connection {}", event.message.0, event.port_id, event.connection_id);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn client_authenticated(
    mut client_authenticated_on_server: MessageReader<ClientAuthenticatedOnServer>,
    mut commands: Commands,
    mut change_state: NetResMut<NextState<NormalStates>>,
    mut test_resource: NetResMut<TestResource>
){
    for ev in client_authenticated_on_server.read() {
        if ev.connection_id != 0 { continue; }

        commands.spawn((ServerReplicator{
            owner: Some(ev.peer_uuid),
            replication_owner: None,
            connection_id: 0,
            port: 0,
            port_to_remove: 0,
            just_for_authenticated: true,
            send_args: None,
            bytes_queue: Default::default(),
        },DefaultManaHealthSystem, Health{
            health: 10.0,
            max_health: 10.0,
        },Mana{
            mana: 20.0,
            max_mana: 25.0,
        }));

        change_state.set(NormalStates::Go);
        test_resource.0 = 10
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(ServerStateReplicationSystem, Default, States, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum NormalStates{
    #[default]
    Paused,
    Go
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Resource ,Serialize, Deserialize, Default, ServerResourceReplicationSystem)]
pub struct TestResource(pub u32);

fn main() {
    let mut app = App::new();

    #[cfg(not(target_arch = "wasm32"))] {
        app.add_plugins((DefaultPlugins,ServerNetworkPlugin,NetworkPlugin,MessagingPlugin,AuthenticationPlugin,ReplicateShared,ReplicateServer,ReplicateStates));
        app.init_resource::<TestResource>();
        app.add_systems(Startup,start_connection);
        app.add_systems(Update,read_hi_message);
        app.add_systems(PostUpdate,client_authenticated);
        app.register_message::<HiMessage>();
        app.init_state::<NormalStates>();
        app.register_server_replication_state::<NormalStates>(Update,ServerStatesData{
            connection_id: 0,
            port_id: 0,
            send_args: None,
            just_authenticated: true
        });
        app.register_replication_component::<Health>();
        app.register_replication_component::<Mana>();
        app.register_server_replication_system::<DefaultManaHealthSystem>(Last);
        app.register_server_replication_resource::<TestResource>(PreUpdate,ServerResourceData{
            connection_id: 0,
            port_id: 0,
            send_args: None,
            just_authenticated: true
        });
    }

    #[cfg(target_arch = "wasm32")] {
        warn!("Server doesn't work on WASM");
        app.add_plugins(DefaultPlugins);
    }

    app.run();
}
