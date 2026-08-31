"""Smoke tests for the Docker Compose configuration.

These tests parse the repository ``docker-compose.yml`` and static repository
files (``.gitignore``, ``.env.example``) and assert that the Infrastructure as
Code definition satisfies the structural, security, and reliability acceptance
criteria of the specification. They do not start any containers; they only
validate the declared configuration.

Validated requirements:
    - NFR-1.1: each component defined as its own container/image.
    - NFR-1.4: explicit resource limits per service.
    - NFR-1.5: unique container names / network aliases.
    - NFR-2.1: all services defined in the compose file.
    - NFR-2.3: official images used where applicable.
    - NFR-2.4: no hardcoded credentials (values via env vars).
    - NFR-2.6: health checks defined for long-running services.
    - NFR-3.1: on-failure:5 restart policy for long-running services.
    - NFR-5.1: internal data network (internal: true).
    - NFR-5.2: only the Visualization_Service port 3000 exposed to the host.
    - NFR-5.4: .env in .gitignore and .env.example committed.
    - NFR-6.3: semantic version image tags, no "latest".
"""

import os
import re

import pytest
import yaml

# Repository root is two levels up from this file (tests/smoke/).
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
COMPOSE_PATH = os.path.join(REPO_ROOT, "docker-compose.yml")
GITIGNORE_PATH = os.path.join(REPO_ROOT, ".gitignore")
ENV_EXAMPLE_PATH = os.path.join(REPO_ROOT, ".env.example")

# Services that are expected to be defined in the compose file.
EXPECTED_SERVICES = {
    "kafka1",
    "kafka2",
    "kafka-init",
    "ingestion",
    "processing",
    "visualization",
}

# Long-running services that must define a restart policy and a health check.
# ``kafka-init`` is a one-shot init container and is intentionally excluded.
LONG_RUNNING_SERVICES = {
    "kafka1",
    "kafka2",
    "ingestion",
    "processing",
    "visualization",
}

# Semantic version pattern: MAJOR.MINOR.PATCH (with optional suffix).
SEMVER_TAG_PATTERN = re.compile(r"^\d+\.\d+\.\d+")


@pytest.fixture(scope="module")
def compose():
    """Parse and return the docker-compose.yml as a dictionary."""
    assert os.path.isfile(COMPOSE_PATH), f"docker-compose.yml not found at {COMPOSE_PATH}"
    with open(COMPOSE_PATH, encoding="utf-8") as handle:
        return yaml.safe_load(handle)


@pytest.fixture(scope="module")
def services(compose):
    """Return the ``services`` mapping from the compose file."""
    assert "services" in compose, "compose file has no 'services' section"
    return compose["services"]


class TestServiceDefinitions:
    """Verify all required services are defined (NFR-2.1, NFR-1.1)."""

    def test_all_expected_services_defined(self, services):
        """Every expected service must be present in the compose file."""
        missing = EXPECTED_SERVICES - set(services.keys())
        assert not missing, f"missing services in compose file: {sorted(missing)}"

    @pytest.mark.parametrize("service_name", sorted(EXPECTED_SERVICES))
    def test_service_has_image_or_build(self, services, service_name):
        """Each service must define its own image or build context (NFR-1.1)."""
        service = services[service_name]
        assert (
            "image" in service or "build" in service
        ), f"service '{service_name}' defines neither 'image' nor 'build'"

    def test_container_names_are_unique(self, services):
        """No two services may share a container name (NFR-1.5)."""
        names = [
            svc["container_name"]
            for svc in services.values()
            if "container_name" in svc
        ]
        assert len(names) == len(set(names)), f"duplicate container names: {names}"


class TestNetworkConfiguration:
    """Verify network isolation and port exposure (NFR-5.1, NFR-5.2)."""

    def test_data_net_is_internal(self, compose):
        """The data-net network must be declared internal (NFR-5.1)."""
        networks = compose.get("networks", {})
        assert "data-net" in networks, "data-net network not defined"
        assert (
            networks["data-net"].get("internal") is True
        ), "data-net must set internal: true"

    def test_only_visualization_exposes_port_3000(self, services):
        """Only the visualization service may map a host port, and only 3000 (NFR-5.2)."""
        for name, service in services.items():
            ports = service.get("ports")
            if name == "visualization":
                assert ports, "visualization service must expose a port"
                assert (
                    ports == ["3000:3000"]
                ), f"visualization must only expose 3000:3000, got {ports}"
            else:
                assert (
                    not ports
                ), f"service '{name}' must not map any host ports, got {ports}"


class TestImageTags:
    """Verify semantic version tags and no 'latest' tag (NFR-6.3, NFR-2.3)."""

    def test_images_use_semantic_version_tags(self, services):
        """Every image reference must use a MAJOR.MINOR.PATCH tag, never 'latest'."""
        for name, service in services.items():
            image = service.get("image")
            if image is None:
                # Services built locally have no image tag to validate.
                continue
            assert ":" in image, f"service '{name}' image '{image}' has no explicit tag"
            tag = image.rsplit(":", 1)[1]
            assert tag != "latest", f"service '{name}' uses forbidden 'latest' tag"
            assert SEMVER_TAG_PATTERN.match(
                tag
            ), f"service '{name}' image tag '{tag}' is not semantic (MAJOR.MINOR.PATCH)"


class TestResourceLimits:
    """Verify explicit resource limits per service (NFR-1.4)."""

    @pytest.mark.parametrize("service_name", sorted(EXPECTED_SERVICES))
    def test_service_has_resource_limits(self, services, service_name):
        """Each service must declare deploy.resources.limits for cpus and memory."""
        service = services[service_name]
        limits = (
            service.get("deploy", {}).get("resources", {}).get("limits", {})
        )
        assert "cpus" in limits, f"service '{service_name}' missing cpus limit"
        assert "memory" in limits, f"service '{service_name}' missing memory limit"


class TestHealthChecks:
    """Verify health checks are defined for long-running services (NFR-2.6)."""

    @pytest.mark.parametrize("service_name", sorted(LONG_RUNNING_SERVICES))
    def test_long_running_service_has_healthcheck(self, services, service_name):
        """Each long-running service must define a health check with a test."""
        service = services[service_name]
        healthcheck = service.get("healthcheck")
        assert healthcheck, f"service '{service_name}' has no healthcheck"
        assert "test" in healthcheck, f"service '{service_name}' healthcheck has no test"


class TestRestartPolicies:
    """Verify restart policies for long-running services (NFR-3.1)."""

    @pytest.mark.parametrize("service_name", sorted(LONG_RUNNING_SERVICES))
    def test_long_running_service_restart_on_failure(self, services, service_name):
        """Long-running services must use the on-failure:5 restart policy."""
        service = services[service_name]
        assert (
            service.get("restart") == "on-failure:5"
        ), f"service '{service_name}' must set restart: on-failure:5"

    def test_init_container_does_not_restart(self, services):
        """The one-shot kafka-init container must not use an auto-restart policy."""
        assert (
            services["kafka-init"].get("restart") == "no"
        ), "kafka-init must set restart: 'no'"


class TestSecretsHandling:
    """Verify credential handling via .env files (NFR-5.4, NFR-2.4)."""

    def test_env_is_gitignored(self):
        """The .env file must be listed in .gitignore."""
        assert os.path.isfile(GITIGNORE_PATH), ".gitignore not found"
        with open(GITIGNORE_PATH, encoding="utf-8") as handle:
            entries = {line.strip() for line in handle}
        assert ".env" in entries, ".env must be listed in .gitignore"

    def test_env_example_exists(self):
        """A .env.example file must be committed with placeholder values."""
        assert os.path.isfile(
            ENV_EXAMPLE_PATH
        ), ".env.example must exist in the repository root"
        with open(ENV_EXAMPLE_PATH, encoding="utf-8") as handle:
            content = handle.read().strip()
        assert content, ".env.example must not be empty"
