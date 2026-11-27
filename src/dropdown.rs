// SPDX-License-Identifier: GPL-3.0-only

use std::any::TypeId;
use std::sync::LazyLock;
use std::time::{Duration, UNIX_EPOCH};

use cosmic::cctk::sctk::reexports::client::protocol::wl_output::WlOutput;
use cosmic::cctk::sctk::shell::wlr_layer::{Anchor, KeyboardInteractivity, Layer};
use cosmic::iced::event::wayland::OutputEvent;
use cosmic::iced::futures::{SinkExt, StreamExt, channel::mpsc, stream as futures_stream};
use cosmic::iced::platform_specific::runtime::wayland::layer_surface::{
    IcedOutput, SctkLayerSurfaceSettings,
};
use cosmic::iced::platform_specific::shell::commands::layer_surface;
use cosmic::iced::{Limits, Size, Subscription, Task, stream, window};
use zbus::fdo::{DBusProxy, NameLostStream, RequestNameFlags, RequestNameReply};

use crate::config::{DropdownAnchor, DropdownConfig, DropdownFocus, DropdownLayer};
use crate::fl;

pub const DBUS_NAME: &str = "com.system76.CosmicTerm.Dropdown";
pub const DBUS_PATH: &str = "/com/system76/CosmicTerm/Dropdown";
pub const DBUS_INTERFACE: &str = "com.system76.CosmicTerm.Dropdown";
const NAMESPACE: &str = "cosmic-term-dropdown";
pub const FOCUS_LOSS_GRACE: Duration = Duration::from_millis(150);
pub const REFOCUS_TIMEOUT: Duration = Duration::from_millis(250);

static BUILD_ID: LazyLock<String> = LazyLock::new(|| {
    std::env::current_exe()
        .and_then(|exe| {
            let modified = std::fs::metadata(&exe)?.modified()?;
            let since_epoch = modified
                .duration_since(UNIX_EPOCH)
                .map_err(std::io::Error::other)?;
            Ok(format!("{}@{}", exe.display(), since_epoch.as_nanos()))
        })
        .unwrap_or_else(|err| {
            log::warn!("failed to identify executable, falling back to version: {err}");
            env!("CARGO_PKG_VERSION").to_string()
        })
});

pub fn toggle_existing() -> bool {
    zbus::block_on(async {
        let conn = zbus::Connection::session().await?;
        let reply = conn
            .call_method(
                Some(DBUS_NAME),
                DBUS_PATH,
                Some(DBUS_INTERFACE),
                "Toggle",
                &(BUILD_ID.as_str(),),
            )
            .await?;
        reply.body().deserialize::<bool>()
    })
    .inspect(|&toggled| {
        if !toggled {
            log::info!("running dropdown instance is a different build, replacing it");
        }
    })
    .inspect_err(|err| log::debug!("no running dropdown instance: {err}"))
    .unwrap_or(false)
}

#[derive(Clone, Copy, Debug)]
pub enum Signal {
    Toggle,
    Replaced,
}

struct Server(mpsc::Sender<Signal>);

#[zbus::interface(name = "com.system76.CosmicTerm.Dropdown")]
impl Server {
    async fn toggle(&mut self, build_id: &str) -> bool {
        if build_id != BUILD_ID.as_str() {
            return false;
        }
        let _ = self.0.send(Signal::Toggle).await;
        true
    }
}

async fn serve(tx: mpsc::Sender<Signal>) -> zbus::Result<(zbus::Connection, NameLostStream)> {
    let conn = zbus::connection::Builder::session().build().await?;
    conn.object_server().at(DBUS_PATH, Server(tx)).await?;
    let name_lost = DBusProxy::new(&conn).await?.receive_name_lost().await?;
    let flags = RequestNameFlags::ALLOW_REPLACEMENT
        | RequestNameFlags::REPLACE_EXISTING
        | RequestNameFlags::DO_NOT_QUEUE;
    match conn.request_name_with_flags(DBUS_NAME, flags).await? {
        RequestNameReply::PrimaryOwner | RequestNameReply::AlreadyOwner => Ok((conn, name_lost)),
        RequestNameReply::InQueue | RequestNameReply::Exists => Err(zbus::Error::NameTaken),
    }
}

pub fn subscription() -> Subscription<Signal> {
    struct DropdownDbus;
    Subscription::run_with(TypeId::of::<DropdownDbus>(), |_| {
        stream::channel(10, |mut output: mpsc::Sender<Signal>| async move {
            let (tx, rx) = mpsc::channel(10);
            match serve(tx).await {
                Ok((_conn, name_lost)) => {
                    let mut signals =
                        futures_stream::select(rx, name_lost.map(|_| Signal::Replaced));
                    while let Some(signal) = signals.next().await {
                        if output.send(signal).await.is_err() {
                            break;
                        }
                    }
                }
                Err(err) => {
                    log::error!("failed to own D-Bus name {DBUS_NAME}: {err}");
                    std::process::exit(1);
                }
            }

            std::future::pending::<()>().await;
        })
    })
}

#[derive(Clone, Debug)]
struct Output {
    wl: WlOutput,
    name: Option<String>,
}

#[derive(Clone, Debug, Default)]
enum State {
    #[default]
    Pending,
    Hidden,
    Measuring {
        id: window::Id,
        output: Option<WlOutput>,
    },
    Shown {
        id: window::Id,
        output: Option<WlOutput>,
        available: Size,
    },
}

pub enum ConfigChange<M> {
    None,
    Apply(Task<M>),
    Recreate,
}

pub enum OutputChange {
    None,
    ShowPending,
    SurfaceLost,
}

#[derive(Debug, Default)]
pub struct Dropdown {
    state: State,
    outputs: Vec<Output>,
    output_choices: Vec<Option<String>>,
    output_names: Vec<String>,
    focus_generation: u64,
    refocusing: bool,
}

impl Dropdown {
    pub fn begin_refocus<M: Send + 'static>(&mut self, config: &DropdownConfig) -> Option<Task<M>> {
        match self.state {
            State::Shown { id, .. } if config.focus == DropdownFocus::OnDemand => {
                self.refocusing = true;
                Some(layer_surface::set_keyboard_interactivity(
                    id,
                    KeyboardInteractivity::Exclusive,
                ))
            }
            _ => None,
        }
    }

    pub fn end_refocus<M: Send + 'static>(&mut self, config: &DropdownConfig) -> Option<Task<M>> {
        if !std::mem::take(&mut self.refocusing) {
            return None;
        }
        match self.state {
            State::Shown { id, .. } => Some(layer_surface::set_keyboard_interactivity(
                id,
                keyboard_interactivity(config.focus),
            )),
            _ => None,
        }
    }

    pub fn focus_lost(&mut self, id: window::Id, config: &DropdownConfig) -> Option<u64> {
        match self.state {
            State::Shown { id: shown, .. } if shown == id && config.hide_on_focus_loss => {
                self.focus_generation += 1;
                Some(self.focus_generation)
            }
            _ => None,
        }
    }

    pub fn focus_gained(&mut self) {
        self.focus_generation += 1;
    }

    pub fn focus_timed_out(&self, generation: u64) -> bool {
        generation == self.focus_generation && matches!(self.state, State::Shown { .. })
    }

    pub fn is_visible(&self) -> bool {
        self.surface_id().is_some()
    }

    pub fn is_measuring(&self, id: window::Id) -> bool {
        matches!(self.state, State::Measuring { id: measuring, .. } if measuring == id)
    }

    fn surface_id(&self) -> Option<window::Id> {
        match self.state {
            State::Measuring { id, .. } | State::Shown { id, .. } => Some(id),
            State::Pending | State::Hidden => None,
        }
    }

    fn find_output(&self, name: Option<&str>) -> Option<WlOutput> {
        let name = name?;
        self.outputs
            .iter()
            .find(|output| output.name.as_deref() == Some(name))
            .map(|output| output.wl.clone())
    }

    pub fn begin_show(&mut self, config: &DropdownConfig) -> Option<SctkLayerSurfaceSettings> {
        if self.is_visible() {
            return None;
        }
        let id = window::Id::unique();
        let output = self.find_output(config.output.as_deref());
        let settings = probe_settings(config, id, output.clone());
        self.state = State::Measuring { id, output };
        Some(settings)
    }

    pub fn finish_show(
        &mut self,
        probe: window::Id,
        available: Size,
        config: &DropdownConfig,
    ) -> Option<SctkLayerSurfaceSettings> {
        let State::Measuring { id, output } = &self.state else {
            return None;
        };
        if *id != probe {
            return None;
        }
        let id = window::Id::unique();
        let output = output.clone();
        let settings = shown_settings(config, id, output.clone(), available);
        self.state = State::Shown {
            id,
            output,
            available,
        };
        Some(settings)
    }

    pub fn hide(&mut self) -> Option<window::Id> {
        let id = self.surface_id();
        self.state = State::Hidden;
        self.refocusing = false;
        self.focus_gained();
        id
    }

    pub fn config_changed<M: Send + 'static>(
        &mut self,
        config: &DropdownConfig,
    ) -> ConfigChange<M> {
        self.refresh_choices(config);
        match &self.state {
            State::Shown {
                id,
                output,
                available,
            } => {
                if self.find_output(config.output.as_deref()) == *output {
                    ConfigChange::Apply(resize(config, *id, *available))
                } else {
                    ConfigChange::Recreate
                }
            }
            State::Pending | State::Hidden | State::Measuring { .. } => ConfigChange::None,
        }
    }

    pub fn output_event(
        &mut self,
        event: OutputEvent,
        wl: WlOutput,
        config: &DropdownConfig,
    ) -> OutputChange {
        let change = match event {
            OutputEvent::Created(info) => {
                self.output_named(wl, info.and_then(|info| info.name), config)
            }
            OutputEvent::InfoUpdate(info) => self.output_named(wl, info.name, config),
            OutputEvent::Removed => {
                self.outputs.retain(|output| output.wl != wl);
                match &self.state {
                    State::Measuring {
                        output: Some(output),
                        ..
                    }
                    | State::Shown {
                        output: Some(output),
                        ..
                    } if *output == wl => {
                        self.state = State::Hidden;
                        OutputChange::SurfaceLost
                    }
                    _ => OutputChange::None,
                }
            }
        };
        self.refresh_choices(config);
        change
    }

    fn output_named(
        &mut self,
        wl: WlOutput,
        name: Option<String>,
        config: &DropdownConfig,
    ) -> OutputChange {
        let show = matches!(self.state, State::Pending)
            && wants_output(config.output.as_deref(), name.as_deref());
        match self.outputs.iter_mut().find(|output| output.wl == wl) {
            Some(output) => output.name = name,
            None => self.outputs.push(Output { wl, name }),
        }
        if show {
            OutputChange::ShowPending
        } else {
            OutputChange::None
        }
    }

    fn refresh_choices(&mut self, config: &DropdownConfig) {
        let connected: Vec<&str> = self
            .outputs
            .iter()
            .filter_map(|output| output.name.as_deref())
            .collect();
        self.output_choices = output_choices(&connected, config.output.as_deref());
        self.output_names = self
            .output_choices
            .iter()
            .map(|choice| match choice {
                None => fl!("dropdown-output-focused"),
                Some(name) if connected.contains(&name.as_str()) => name.clone(),
                Some(name) => fl!("dropdown-output-disconnected", name = name.as_str()),
            })
            .collect();
    }

    pub fn output_choices(&self) -> &[Option<String>] {
        &self.output_choices
    }

    pub fn output_names(&self) -> &[String] {
        &self.output_names
    }
}

fn wants_output(configured: Option<&str>, name: Option<&str>) -> bool {
    configured.is_none_or(|configured| name == Some(configured))
}

fn output_choices(connected: &[&str], configured: Option<&str>) -> Vec<Option<String>> {
    let disconnected = configured.filter(|configured| !connected.contains(configured));
    std::iter::once(None)
        .chain(
            connected
                .iter()
                .copied()
                .chain(disconnected)
                .map(|name| Some(name.to_string())),
        )
        .collect()
}

fn percent_of(length: f32, percent: u8) -> u32 {
    (length * f32::from(percent.clamp(1, 100)) / 100.0)
        .round()
        .max(1.0) as u32
}

fn edge(anchor: DropdownAnchor) -> Anchor {
    match anchor {
        DropdownAnchor::Top => Anchor::TOP,
        DropdownAnchor::Bottom => Anchor::BOTTOM,
        DropdownAnchor::Left => Anchor::LEFT,
        DropdownAnchor::Right => Anchor::RIGHT,
    }
}

fn layer(layer: DropdownLayer) -> Layer {
    match layer {
        DropdownLayer::Top => Layer::Top,
        DropdownLayer::Overlay => Layer::Overlay,
    }
}

fn keyboard_interactivity(focus: DropdownFocus) -> KeyboardInteractivity {
    match focus {
        DropdownFocus::Exclusive => KeyboardInteractivity::Exclusive,
        DropdownFocus::OnDemand => KeyboardInteractivity::OnDemand,
    }
}

fn base_settings(
    config: &DropdownConfig,
    id: window::Id,
    output: Option<WlOutput>,
) -> SctkLayerSurfaceSettings {
    SctkLayerSurfaceSettings {
        id,
        layer: layer(config.layer),
        keyboard_interactivity: keyboard_interactivity(config.focus),
        output: output.map_or(IcedOutput::Active, IcedOutput::Output),
        namespace: NAMESPACE.to_string(),
        exclusive_zone: 0,
        size_limits: Limits::NONE.min_width(1.0).min_height(1.0),
        ..Default::default()
    }
}

fn probe_settings(
    config: &DropdownConfig,
    id: window::Id,
    output: Option<WlOutput>,
) -> SctkLayerSurfaceSettings {
    SctkLayerSurfaceSettings {
        keyboard_interactivity: KeyboardInteractivity::None,
        input_zone: Some(Vec::new()),
        anchor: Anchor::all(),
        size: Some((None, None)),
        ..base_settings(config, id, output)
    }
}

fn shown_settings(
    config: &DropdownConfig,
    id: window::Id,
    output: Option<WlOutput>,
    available: Size,
) -> SctkLayerSurfaceSettings {
    let (width, height) = size(config, available);
    SctkLayerSurfaceSettings {
        anchor: edge(config.anchor),
        size: Some((Some(width), Some(height))),
        ..base_settings(config, id, output)
    }
}

fn size(config: &DropdownConfig, available: Size) -> (u32, u32) {
    (
        percent_of(available.width, config.width_percent),
        percent_of(available.height, config.height_percent),
    )
}

fn resize<M: Send + 'static>(config: &DropdownConfig, id: window::Id, available: Size) -> Task<M> {
    let (width, height) = size(config, available);
    Task::batch([
        layer_surface::set_anchor(id, edge(config.anchor)),
        layer_surface::set_size(id, Some(width), Some(height)),
        layer_surface::set_layer(id, layer(config.layer)),
        layer_surface::set_keyboard_interactivity(id, keyboard_interactivity(config.focus)),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_fills_the_output_without_taking_input() {
        let s = probe_settings(&DropdownConfig::default(), window::Id::unique(), None);
        assert_eq!(s.anchor, Anchor::all());
        assert_eq!(s.size, Some((None, None)));
        assert!(matches!(s.output, IcedOutput::Active));
        assert_eq!(s.keyboard_interactivity, KeyboardInteractivity::None);
        assert_eq!(s.input_zone, Some(Vec::new()));
    }

    #[test]
    fn shown_surface_is_created_at_final_size() {
        let config = DropdownConfig {
            anchor: DropdownAnchor::Left,
            width_percent: 30,
            height_percent: 100,
            ..Default::default()
        };
        let s = shown_settings(
            &config,
            window::Id::unique(),
            None,
            Size::new(1920.0, 1040.0),
        );
        assert_eq!(s.anchor, Anchor::LEFT);
        assert_eq!(s.size, Some((Some(576), Some(1040))));
        assert_eq!(s.keyboard_interactivity, KeyboardInteractivity::Exclusive);
    }

    fn shown(config: &DropdownConfig) -> (Dropdown, window::Id) {
        let mut dropdown = Dropdown::default();
        let probe = dropdown.begin_show(config).unwrap().id;
        let id = dropdown
            .finish_show(probe, Size::new(800.0, 600.0), config)
            .unwrap()
            .id;
        (dropdown, id)
    }

    #[test]
    fn focus_loss_is_ignored_unless_enabled() {
        let config = DropdownConfig::default();
        let (mut dropdown, id) = shown(&config);
        assert_eq!(dropdown.focus_lost(id, &config), None);
    }

    #[test]
    fn focus_loss_hides_after_grace_unless_regained() {
        let config = DropdownConfig {
            hide_on_focus_loss: true,
            ..Default::default()
        };
        let (mut dropdown, id) = shown(&config);
        assert_eq!(dropdown.focus_lost(window::Id::unique(), &config), None);

        let generation = dropdown.focus_lost(id, &config).unwrap();
        assert!(dropdown.focus_timed_out(generation));

        dropdown.focus_gained();
        assert!(!dropdown.focus_timed_out(generation));

        let generation = dropdown.focus_lost(id, &config).unwrap();
        dropdown.hide();
        assert!(!dropdown.focus_timed_out(generation));
    }

    #[test]
    fn refocus_only_applies_to_on_demand_focus() {
        let config = DropdownConfig::default();
        let (mut dropdown, _) = shown(&config);
        assert!(dropdown.begin_refocus::<()>(&config).is_none());
        assert!(dropdown.end_refocus::<()>(&config).is_none());
    }

    #[test]
    fn refocus_restores_focus_mode_once() {
        let config = DropdownConfig {
            focus: DropdownFocus::OnDemand,
            ..Default::default()
        };
        let (mut dropdown, _) = shown(&config);
        assert!(dropdown.begin_refocus::<()>(&config).is_some());
        assert!(dropdown.end_refocus::<()>(&config).is_some());
        assert!(dropdown.end_refocus::<()>(&config).is_none());
    }

    #[test]
    fn hiding_cancels_refocus() {
        let config = DropdownConfig {
            focus: DropdownFocus::OnDemand,
            ..Default::default()
        };
        let (mut dropdown, _) = shown(&config);
        assert!(dropdown.begin_refocus::<()>(&config).is_some());
        dropdown.hide();
        assert!(dropdown.end_refocus::<()>(&config).is_none());
        assert!(dropdown.begin_refocus::<()>(&config).is_none());
    }

    #[test]
    fn toggle_rejects_other_builds() {
        use cosmic::iced::futures::executor::block_on;
        let (tx, mut rx) = mpsc::channel(1);
        let mut server = Server(tx);
        assert!(!block_on(server.toggle("other-build")));
        assert!(block_on(server.toggle(&BUILD_ID)));
        assert!(matches!(rx.try_recv(), Ok(Signal::Toggle)));
    }

    #[test]
    fn finish_show_replaces_the_probe() {
        let config = DropdownConfig::default();
        let mut dropdown = Dropdown::default();
        let probe = dropdown.begin_show(&config).unwrap().id;
        assert!(dropdown.is_measuring(probe));
        assert!(
            dropdown
                .finish_show(window::Id::unique(), Size::new(800.0, 600.0), &config)
                .is_none()
        );
        let shown = dropdown
            .finish_show(probe, Size::new(800.0, 600.0), &config)
            .unwrap();
        assert_ne!(shown.id, probe);
        assert!(!dropdown.is_measuring(probe));
        assert_eq!(dropdown.hide(), Some(shown.id));
    }

    #[test]
    fn percent_is_rounded_and_clamped() {
        assert_eq!(percent_of(1920.0, 100), 1920);
        assert_eq!(percent_of(1080.0, 40), 432);
        assert_eq!(percent_of(1001.0, 50), 501);
        assert_eq!(percent_of(1000.0, 0), 10);
        assert_eq!(percent_of(1000.0, 200), 1000);
    }

    #[test]
    fn focused_monitor_accepts_any_output() {
        assert!(wants_output(None, Some("DP-1")));
        assert!(wants_output(None, None));
        assert!(wants_output(Some("DP-1"), Some("DP-1")));
        assert!(!wants_output(Some("DP-1"), Some("HDMI-A-1")));
        assert!(!wants_output(Some("DP-1"), None));
    }

    #[test]
    fn choices_list_focused_then_connected() {
        assert_eq!(
            output_choices(&["DP-1", "HDMI-A-1"], Some("DP-1")),
            [None, Some("DP-1".into()), Some("HDMI-A-1".into())]
        );
    }

    #[test]
    fn choices_keep_disconnected_configured_output() {
        assert_eq!(
            output_choices(&["DP-1"], Some("DP-2")),
            [None, Some("DP-1".into()), Some("DP-2".into())]
        );
    }
}
