"""Driver interface registry and manifest export."""

from __future__ import annotations

import asyncio

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
from cyberwave.driver.interface.registry import resolve_topic_path
from cyberwave.driver.interface.registry_mixin import InterfaceRegistryMixin
from cyberwave.manifest.driver_config import (
    TWIN_IMU_TOPIC_SLUG,
    TWIN_POSITION_TOPIC_SLUG,
)

_TWIN_UUID = "d0d9ec45-a85c-4730-99e9-fb5f3755759c"


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


@pytest.mark.parametrize("leaf", ["position", "rotation"])
def test_resolve_topic_path_substitutes_uuid_for_unmapped_leaf(leaf: str) -> None:
    """Regression: pose leaves are not in the slug map and used to publish to the
    literal ``cyberwave/twin/{twin_uuid}/position`` string, so no pose ever
    reached the twin. These paths must match what cyberwave/mqtt publishes."""
    topic = TopicSpec(namespace="twin", leaf=leaf, payload_schema_ref="PosePayload")
    assert (
        resolve_topic_path(topic, _TWIN_UUID)
        == f"cyberwave/twin/{_TWIN_UUID}/{leaf}"
    )
    assert "{twin_uuid}" not in resolve_topic_path(topic, _TWIN_UUID)


@pytest.mark.parametrize(
    ("namespace", "leaf", "expected"),
    [
        ("twin", "telemetry", f"cyberwave/twin/{_TWIN_UUID}/telemetry"),
        ("twin", "command", f"cyberwave/twin/{_TWIN_UUID}/command"),
        ("joint", "update", f"cyberwave/joint/{_TWIN_UUID}/update"),
    ],
)
def test_resolve_topic_path_keeps_mapped_leaves(
    namespace: str, leaf: str, expected: str
) -> None:
    topic = TopicSpec(namespace=namespace, leaf=leaf, payload_schema_ref="X")
    assert resolve_topic_path(topic, _TWIN_UUID) == expected


def test_resolve_topic_path_honours_prefix() -> None:
    by_leaf = TopicSpec(namespace="twin", leaf="position", payload_schema_ref="X")
    by_slug = TopicSpec(topic_slug=TWIN_POSITION_TOPIC_SLUG, payload_schema_ref="X")
    expected = f"dev/cyberwave/twin/{_TWIN_UUID}/position"
    assert resolve_topic_path(by_leaf, _TWIN_UUID, prefix="dev/") == expected
    assert resolve_topic_path(by_slug, _TWIN_UUID, prefix="dev/") == expected


def test_unmapped_leaf_keeps_the_placeholder_in_the_manifest() -> None:
    """The exported cw-driver slug stays templated; only the wire path resolves."""
    registry = DriverInterfaceRegistry()
    registry.add_publisher(
        TopicSpec(namespace="twin", leaf="position", payload_schema_ref="PosePayload"),
        CallbackGroup(lambda: None),
    )
    raw = registry.to_cw_driver_dict(registry_id="acme/test")
    assert raw["mqtt"]["twin"]["position"]["payload_schema_ref"] == "PosePayload"


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
