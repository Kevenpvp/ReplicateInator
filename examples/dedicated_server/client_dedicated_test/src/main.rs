use replicateinator::client::plugins::replicate::ClientResourceReplicationSystem;
use replicateinator::shared::plugins::replicate_states::ClientStateSystem;
use replicateinator::client::plugins::replicate::{ClientReplicationSystem, RegisterClientReplicationSystem, ReplicateClient};
use networkinator::shared::plugins::messaging::{ClientConnectionParams, MessageTrait, MessageTraitPlugin, MessagingPlugin};
use bevy::DefaultPlugins;
use bevy::prelude::{Added, App, AppExtStates, Changed, Component, Entity, IntoScheduleConfigs, MessageReader, OnEnter, Or, PostUpdate, Query, Reflect, Res, Resource, Single, Startup, State, States, Update};
use serde::{Deserialize, Serialize};
use message_pro_macro::ConnectionMessage;
#[cfg(not(target_arch = "wasm32"))]
use networkinator::client::ports::tcp::TcpClientSettings;
#[cfg(not(target_arch = "wasm32"))]
use networkinator::client::ports::udp::UdpClientSettings;
use networkinator::{NetRes, NetResMut};
use networkinator::client::plugins::network::ClientNetworkPlugin;
#[cfg(target_arch = "wasm32")]
use networkinator::client::ports::wasm_websocket::WasmWebSocketClientSettings;
use networkinator::shared::plugins::authentication::{AuthenticationPlugin, ClientPortAuthenticated};
use networkinator::shared::plugins::network::{ClientConnection, DefaultNetworkPortSharedInfosClient, LocalSessionUUID, NetworkConnection, NetworkPlugin};
use replicateinator::shared::plugins::replicate::{ReplicateShared, ReplicationSharedTrait};
use replicateinator::shared::plugins::replicate_states::{ReplicatedStateSharedTrait, ReplicateStates};
use replication_pro_macro::{ClientReplicationSystem, ClientResourceReplicationSystem, ClientStateReplicationSystem};

#[derive(Serialize,Deserialize,ConnectionMessage)]
pub struct HiMessage(String);

#[cfg(not(target_arch = "wasm32"))]
fn start_connection(
    mut network_connection: NetResMut<NetworkConnection<ClientConnection>>,
) {
    network_connection.start_connection::<DefaultNetworkPortSharedInfosClient>(0, Box::new(TcpClientSettings::default()),true);
    network_connection.open_secondary_port(0, Box::new(UdpClientSettings::default().with_server_port(8070)));
}

#[cfg(target_arch = "wasm32")]
fn start_connection(
    mut network_connection: NetResMut<NetworkConnection<ClientConnection>>,
) {
    network_connection.start_connection::<DefaultNetworkPortSharedInfosClient>(0, Box::new(WasmWebSocketClientSettings::default()),true);
}

#[derive(Component, Default, Serialize, Deserialize, Reflect, Debug)]
pub struct Health{
    health: f32,
    max_health: f32,
}

#[derive(Component, Default, Serialize, Deserialize, Reflect, Debug)]
pub struct Mana{
    mana: f32,
    max_mana: f32,
}

#[derive(ClientStateReplicationSystem, Default, States, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum NormalStates{
    #[default]
    Paused,
    Go
}

#[derive(Component,Default,ClientReplicationSystem)]
#[replication(Health, Mana)]
pub struct DefaultManaHealthSystem;

#[derive(Resource ,Serialize, Deserialize, Default, ClientResourceReplicationSystem)]
pub struct TestResource(pub u32);

fn send_hi_message(
    mut client_port_authenticated: MessageReader<ClientPortAuthenticated>,
    mut client_connection_params: ClientConnectionParams,
    local_session_uuid: NetRes<LocalSessionUUID>,
){
    for event in client_port_authenticated.read() {
        client_connection_params.send_message::<HiMessage>(event.connection_id, event.port_id, HiMessage("Hi server".parse().unwrap()), local_session_uuid.get_session_uuid(), None);
    }
}

fn check_health_and_mana(
    query: Query<(Entity, &Health, &Mana), (Added<Health>, Added<Mana>)>,
){
    for (_, health, mana) in query.iter() {
        println!("Health: {:?}", health);
        println!("Mana: {:?}", mana);
    }
}

pub fn check_state_changed(
    state: Res<State<NormalStates>>,
){
    println!("State: {:?}", state.get());
}

pub fn check_resource_changed(
    query: Single<&TestResource, Or<(Changed<TestResource>, Added<TestResource>)>>,
){
    println!("Resource new value is {} ",query.0);
}

fn main() {
    let mut app = App::new();

    app.add_plugins((DefaultPlugins,ClientNetworkPlugin,NetworkPlugin,MessagingPlugin,AuthenticationPlugin,ReplicateShared,ReplicateClient,ReplicateStates));
    app.init_resource::<TestResource>();
    app.add_systems(Startup,start_connection);
    app.add_systems(Update,send_hi_message);
    app.add_systems(PostUpdate,(check_health_and_mana,check_resource_changed).chain());
    app.init_state::<NormalStates>();
    app.add_systems(OnEnter(NormalStates::Go),check_state_changed);
    app.register_client_replication_state::<NormalStates>();
    app.register_message::<HiMessage>();
    app.register_replication_component::<Health>();
    app.register_replication_component::<Mana>();
    app.register_client_replication_system::<DefaultManaHealthSystem>(Update);
    app.register_client_replication_resource::<TestResource>();
    app.run();
}
