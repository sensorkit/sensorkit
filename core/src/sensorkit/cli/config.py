# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio

import asyncclick as click
import yaml
from pydantic import BaseModel, ValidationError
from rich.text import Text

import sensorkit.api as sk
from sensorkit.backend.base import BackendError, KeyNotFound
from sensorkit.cli.utils import console, with_kit
from sensorkit.config.parser import (
    ConfigError,
    ConfigSectionUnknown,
    ConfigVersionUnsupported,
)


@click.group("config")
async def config_group():
    """Manage unified SensorKit configuration."""


async def get_changed_ekv(
    kit: sk.SensorKit,
    ekv: dict[str, list[BaseModel]],
) -> dict[str, list[BaseModel]]:
    out = {}

    for entity_id, models in ekv.items():
        filtered = []

        for model in models:
            try:
                current = await kit.entity(entity_id).kv_get_model(type(model))

                if current != model:
                    filtered.append(model)
            except KeyNotFound:
                filtered.append(model)
            except BackendError:
                # Other backend errors are fatal.
                raise
            except Exception:
                # Custom validators can raise any exception, so we cannot only catch
                # ValidationError here.
                filtered.append(model)

        if filtered:
            out[entity_id] = filtered

    return out


def print_ekv(
    ekv: dict[str, list[BaseModel]],
    *,
    status: dict[int, str] | None = None,
    show_values: bool = False,
):
    for entity_id, models in ekv.items():
        console.print(f"[red]{entity_id}[/red]")

        for i, model in enumerate(models, 1):
            bar = "├" if i < len(models) else "└"
            stat = status[id(model)] if status is not None else ""
            console.print(f" {bar}── [green]{type(model).__name__:16s}[/green] {stat}")

            if show_values:
                bar = "│" if i < len(models) else " "

                with console.capture() as capture:
                    console.print_json(model.model_dump_json())

                json = "\n".join(f" {bar}   {line}" for line in capture.get().splitlines())
                console.print(Text.from_ansi(json))


@config_group.command("load")
@click.argument("file")
@click.option("-n", "--dry-run", is_flag=True, help="Do not write to configuration backend")
@click.option("-f", "--force", is_flag=True, help="Write configuration even if unchanged")
@click.option("-v", "verbose", count=True, help="Increase output verbosity")
@with_kit
async def config_load(kit, *, file: str | None, force: bool, dry_run: bool, verbose: int):
    """Load a SensorKit unified configuration file."""
    try:
        sk.set_config_location(file)
        config = await sk.load_config()
    except FileNotFoundError:
        console.print(f"[bold red]ERROR: file not found: {file}[/bold red]")
        return
    except yaml.YAMLError as e:
        console.print(f"[bold red]ERROR: invalid YAML in {file}: {e}[/bold red]")
        return
    except ConfigVersionUnsupported as e:
        console.print(f"[bold red]ERROR: unsupported config version: {e}[/bold red]")
        return
    except ConfigSectionUnknown as e:
        console.print(f"[bold red]ERROR: unknown config section: {e}[/bold red]")
        return
    except ConfigError as e:
        console.print(f"[bold red]ERROR: config processing failed: {e}[/bold red]")
        return
    except ValidationError as e:
        console.print(f"[bold red]ERROR: config validation failed: {e}[/bold red]")
        return

    ekv = config.entity_kv
    status = {id(model): "🆗  Unchanged" for models in ekv.values() for model in models}

    if not force:
        ekv = await get_changed_ekv(kit, ekv)

    if not dry_run:
        update_coros = {
            # Write global config keys.
            # TODO: Write global config when available at the backend.
            # Write entity config keys.
            **{
                kit.entity(entity_id).kv_put_model(model): (entity_id, model)
                for entity_id, models in ekv.items()
                for model in models
            },
        }

        results = await asyncio.gather(*update_coros, return_exceptions=True)
        status.update(
            {
                id(model): "✅ Updated"
                if not isinstance(result, Exception)
                else f"❌ Error: {type(result).__name__}"
                for (_, model), result in zip(update_coros.values(), results, strict=True)
            }
        )

        if any(isinstance(result, Exception) for result in results):
            console.print(f"[bold red]ERROR: config update failed with {sum(1 for result in results if isinstance(result, Exception))} exceptions[/bold red]")
            return

    if verbose >= 1:
        print_ekv(config.entity_kv, status=status, show_values=verbose >= 2)
    elif updated := sum(len(models) for models in ekv.values()):
        console.print(f"[green]{updated}[/green] updated keys for [red]{len(ekv)}[/red] entities")
