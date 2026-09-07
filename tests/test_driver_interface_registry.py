"""Driver interface registry and manifest export."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from cyberwave.driver import (
    CallbackGroup,
    CommandArgs,
    DriverInterfaceRegistry,
    DriverOperationMode,
    TopicSpec,
    default_management_commands,
)
from cyberwave.driver.interface.args import PublisherArgs, effective_publish_mode
from cyberwave.driver.interface.registry_mixin import InterfaceRegistryMixin
from cyberwave.manifest.driver_config import TWIN_IMU_TOPIC_SLUG


def test_async_mqtt_handler_runs_on_driver_loop() -> None:
    received: list[dict[str, object]] = []

    async def _run() -> None:
        async def async_handler(envelope: dict[str, object]) -> None:
            received.append(envelope)

        loop = asyncio.get_running_loop()
        wrapper = InterfaceRegistryMixin._adapt_async_mqtt_handler(
            async_handler, loop=loop, path="cyberwave/twin/uuid/command"
        )
        wrapper({"command": "rotate"})
        await asyncio.sleep(0.05)

    asyncio.run(_run())
    assert received == [{"command": "rotate"}]


def test_add_listener_command_exports_cw_driver_root() -> None:
    registry = DriverInterfaceRegistry()
    cmd = TopicSpec(
        namespace="twin",
        leaf="command",
        payload_schema_ref="TwinCommandPayload",
        description="Commands",
    )
    registry.add_listener(
        cmd,
        CallbackGroup(lambda _e: None),
        command=CommandArgs(name="stop"),
    )
    raw = registry.to_cw_driver_dict(registry_id="acme/test")
    assert raw["mqtt"]["twin"]["command"]["payload_schema_ref"] == "TwinCommandPayload"
    assert "stop" in raw["mqtt"]["commands"]["supported"]


def test_default_management_commands() -> None:
    registry = DriverInterfaceRegistry()
    default_management_commands(
        registry,
        on_controller_changed=CallbackGroup(lambda _e: None),
        on_teleoperate=CallbackGroup(lambda _e: None),
    )
    table = registry.command_dispatch_table(DriverOperationMode.NO_OP)
    assert "controller-changed" in table
    assert "teleoperate" in table


def test_default_management_commands_always_hides_teleop_controller_from_catalog() -> None:
    """controller-changed/teleoperate/remoteoperate are internal plumbing, never
    advertised as robot capabilities — even when catalog_hidden defaults to False.
    ``stop`` is unaffected and stays visible (existing behavior)."""
    registry = DriverInterfaceRegistry()
    default_management_commands(
        registry,
        on_controller_changed=CallbackGroup(lambda _e: None),
        on_teleoperate=CallbackGroup(lambda _e: None),
        on_remoteoperate=CallbackGroup(lambda _e: None),
        on_stop=CallbackGroup(lambda _e: None),
    )
    raw = registry.to_cw_driver_dict(registry_id="acme/test")
    commands = raw["mqtt"].get("commands", {})
    supported = commands.get("supported", [])
    names = {e["name"] if isinstance(e, dict) else e for e in supported}
    assert names.isdisjoint({"controller-changed", "teleoperate", "remoteoperate"})
    assert "stop" in names
    # All four still dispatch.
    table = registry.command_dispatch_table(DriverOperationMode.NO_OP)
    for cmd_name in ("controller-changed", "teleoperate", "remoteoperate", "stop"):
        assert cmd_name in table


def test_catalog_hidden_command_dispatches_but_is_not_advertised() -> None:
    registry = DriverInterfaceRegistry()
    cmd = TopicSpec(
        namespace="twin",
        leaf="command",
        payload_schema_ref="TwinCommandPayload",
    )
    registry.add_listener(
        cmd,
        CallbackGroup(lambda _e: None),
        command=CommandArgs(name="stop", catalog_hidden=True),
    )
    registry.add_listener(
        cmd,
        CallbackGroup(lambda _e: None),
        command=CommandArgs(name="grab"),
    )
    raw = registry.to_cw_driver_dict(registry_id="acme/test")
    supported = raw["mqtt"]["commands"]["supported"]
    assert "grab" in supported
    assert "stop" not in supported  # hidden from catalog
    # …but still dispatchable on the wire.
    table = registry.command_dispatch_table(DriverOperationMode.NO_OP)
    assert "stop" in table


def test_default_management_commands_catalog_hidden() -> None:
    registry = DriverInterfaceRegistry()
    default_management_commands(
        registry,
        on_controller_changed=CallbackGroup(lambda _e: None),
        on_teleoperate=CallbackGroup(lambda _e: None),
        on_remoteoperate=CallbackGroup(lambda _e: None),
        on_stop=CallbackGroup(lambda _e: None),
        catalog_hidden=True,
    )
    raw = registry.to_cw_driver_dict(registry_id="acme/test")
    commands = raw["mqtt"].get("commands", {})
    supported = commands.get("supported", [])
    names = {e["name"] if isinstance(e, dict) else e for e in supported}
    assert names.isdisjoint(
        {"controller-changed", "teleoperate", "remoteoperate", "stop"}
    )
    # All four still dispatch.
    table = registry.command_dispatch_table(DriverOperationMode.NO_OP)
    for cmd_name in ("controller-changed", "teleoperate", "remoteoperate", "stop"):
        assert cmd_name in table


def test_duplicate_command_raises() -> None:
    registry = DriverInterfaceRegistry()
    cmd = TopicSpec(
        namespace="twin",
        leaf="command",
        payload_schema_ref="TwinCommandPayload",
    )
    registry.add_listener(
        cmd,
        CallbackGroup(lambda _e: None),
        command=CommandArgs(name="stop"),
    )
    registry.add_listener(
        cmd,
        CallbackGroup(lambda _e: None),
        command=CommandArgs(name="stop"),
    )
    try:
        registry.command_dispatch_table(DriverOperationMode.NO_OP)
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "duplicate" in str(exc).lower()


def test_enable_zenoh_exports_zenoh_section() -> None:
    registry = DriverInterfaceRegistry()
    imu = TopicSpec(
        topic_slug=TWIN_IMU_TOPIC_SLUG,
        payload_schema_ref="ImuPayload",
        description="IMU",
        enable_zenoh=True,
        zenoh_channel="imu",
    )
    registry.add_publisher(
        imu,
        CallbackGroup(lambda: {"sensor_id": "x"}),
        publisher=PublisherArgs(rate_hz=50.0),
    )
    raw = registry.to_cw_driver_dict(registry_id="acme/test")
    assert "zenoh" in raw
    assert raw["zenoh"]["channels"]["imu"]["payload_schema_ref"] == "ImuPayload"


def test_inferred_publish_mode_for_dual_spec() -> None:
    dual = TopicSpec(
        topic_slug=TWIN_IMU_TOPIC_SLUG,
        payload_schema_ref="ImuPayload",
        enable_zenoh=True,
        zenoh_channel="imu",
    )
    assert effective_publish_mode(dual, PublisherArgs()) == "dual"


def test_registry_zenoh_enabled_without_data_backend_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cyberwave.driver.interface.registry_mixin import InterfaceRegistryMixin

    monkeypatch.delenv("CYBERWAVE_DATA_BACKEND", raising=False)

    class _Probe(InterfaceRegistryMixin):
        def __init__(self) -> None:
            self._interface = DriverInterfaceRegistry()
            self._registry_zenoh_bus = None
            self._registry_zenoh_subscriptions = []

    probe = _Probe()
    imu = TopicSpec(
        topic_slug=TWIN_IMU_TOPIC_SLUG,
        payload_schema_ref="ImuPayload",
        enable_zenoh=True,
        zenoh_channel="imu",
    )
    probe._interface.add_publisher(
        imu,
        CallbackGroup(lambda: {}),
        publisher=PublisherArgs(rate_hz=1.0),
    )
    assert probe._registry_zenoh_requested() is True
    assert probe._registry_zenoh_enabled() is True


def test_enable_zenoh_forbidden_on_command() -> None:
    with pytest.raises(ValueError, match="MQTT-only"):
        TopicSpec(
            namespace="twin",
            leaf="command",
            payload_schema_ref="TwinCommandPayload",
            enable_zenoh=True,
            zenoh_channel="commands/bad",
        )


def test_enable_zenoh_defaults_channel_from_slug() -> None:
    imu = TopicSpec(
        topic_slug=TWIN_IMU_TOPIC_SLUG,
        payload_schema_ref="ImuPayload",
        enable_zenoh=True,
    )
    assert imu.resolved_zenoh_channel() == "imu"


def test_unwire_interface_without_client_does_not_mask_startup_error() -> None:
    """Teardown after a failed startup (no cloud client) must be a safe no-op.

    Regression: when startup fails before MQTT connects, run_async's finally
    block calls _unwire_interface_from_registry. If that requires a client it
    raises RuntimeError and masks the real startup exception.
    """

    class _Bare(InterfaceRegistryMixin):
        def __init__(self) -> None:
            self._cw = None
            self._wired_mqtt_handlers = [("cyberwave/twin/x/command", lambda _e: None)]

        async def _teardown_zenoh_registry(self) -> None:
            return None

    bare = _Bare()
    asyncio.run(bare._unwire_interface_from_registry())  # must not raise
    assert bare._wired_mqtt_handlers == []


_CMD_TOPIC = "cyberwave/twin/twin-uuid/command"
_JOINT_TOPIC = "cyberwave/joint/twin-uuid/update"


class _FakeMqtt:
    """Just enough of cyberwave.mqtt: one handler slot per topic, replaced in
    place by a repeat subscribe, dropped by unsubscribe."""

    topic_prefix = ""

    def __init__(self) -> None:
        self.handlers: dict[str, Callable[[dict[str, Any]], None]] = {}
        self.events: list[tuple[str, str]] = []
        self.dropped: list[dict[str, Any]] = []
        self.in_flight: dict[str, Any] | None = None

    def subscribe(self, topic, handler=None, qos=0, *, no_local=False, subscriber_key=None):
        self.handlers[topic] = handler
        self.events.append(("subscribe", topic))
        self._land_in_flight()

    def unsubscribe(self, topic, subscriber_key=None):
        self.handlers.pop(topic, None)
        self.events.append(("unsubscribe", topic))
        self._land_in_flight()

    def deliver(self, topic: str, envelope: dict[str, Any]) -> None:
        handler = self.handlers.get(topic)
        if handler is None:
            self.dropped.append(envelope)
        else:
            handler(envelope)

    def _land_in_flight(self) -> None:
        # A command that arrives right after the first (un)subscribe of a rewire.
        if self.in_flight is not None:
            envelope, self.in_flight = self.in_flight, None
            self.deliver(_CMD_TOPIC, envelope)


class _RewireDriver(InterfaceRegistryMixin):
    REGISTRY_ID = "acme/test"
    twin_uuid = "twin-uuid"

    def __init__(self, mqtt: _FakeMqtt) -> None:
        self._cw = SimpleNamespace(mqtt=mqtt)
        self.seen: list[str] = []
        self._init_interface_registry()

    def define_interface(self, iface: DriverInterfaceRegistry) -> None:
        iface.add_listener(
            TopicSpec(
                namespace="twin", leaf="command", payload_schema_ref="TwinCommandPayload"
            ),
            CallbackGroup(lambda envelope: self.seen.append(envelope["command"])),
            command=CommandArgs(name="ping"),
            operation_modes=frozenset(DriverOperationMode),
        )
        # Joint targets only matter while remotely teleoperated.
        iface.add_listener(
            TopicSpec(namespace="joint", leaf="update", payload_schema_ref="JointUpdate"),
            CallbackGroup(lambda _e: None),
            operation_modes=frozenset({DriverOperationMode.TELEOP_REMOTE}),
        )


def test_mode_switch_keeps_the_command_topic_subscribed() -> None:
    """Regression: switching operation mode unsubscribed the command topic before
    resubscribing it, so a command landing in the gap (the first one after a
    controller attach, a stop, a reconnect, startup) was silently lost."""
    mqtt = _FakeMqtt()
    driver = _RewireDriver(mqtt)

    async def _run() -> None:
        await driver._wire_interface_from_registry()
        await driver._set_operation_mode(DriverOperationMode.TELEOP_REMOTE)
        assert set(mqtt.handlers) == {_CMD_TOPIC, _JOINT_TOPIC}
        mqtt.events.clear()

        # `stop` arrives while the switch back to NO_OP is in flight.
        mqtt.in_flight = {"command": "ping"}
        await driver._set_operation_mode(DriverOperationMode.NO_OP)
        await asyncio.sleep(0.05)
        assert driver.seen == ["ping"]

        # The new mode's dispatch table is live on the surviving subscription.
        mqtt.deliver(_CMD_TOPIC, {"command": "ping"})
        await asyncio.sleep(0.05)

    asyncio.run(_run())

    assert mqtt.dropped == []
    assert driver.seen == ["ping", "ping"]
    # Command handler swapped in place; the joint topic dropped only afterwards.
    assert mqtt.events == [("subscribe", _CMD_TOPIC), ("unsubscribe", _JOINT_TOPIC)]
    assert set(mqtt.handlers) == {_CMD_TOPIC}
    assert [path for path, _ in driver._wired_mqtt_handlers] == [_CMD_TOPIC]
