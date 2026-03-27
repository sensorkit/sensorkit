import glob
import json
import sys
from typing import Any, TextIO

import asyncclick as click
from pydantic import BaseModel, ValidationError
from rich import print_json

from sensorkit.cli.utils import console, entity_option, with_kit


@click.group("kv")
async def kv_group():
    """Manage Key-Value storage operations for SensorKit."""

@kv_group.command("ls")
@entity_option()
@with_kit
async def list_kv(kit, entity: str | None):
    """
    List all key-value pairs, optionally filtered by entity.

    Args:
        entity: Optional entity name to filter the key-value context.
    """
    from sensorkit.backend.base import Entity, KeyValueContext

    context: KeyValueContext
    if entity:
        context = kit.backend.key_value(entity=Entity.at(entity))
    else:
        context = kit.backend.key_value()

    for item in await context.get_all(deep=True):
        console.print("-" * 80)
        console.print(f"[bold white]{item.key}[/bold white]\n")
        data = item.value.decode()
        try:
            json.loads(data)
            print_json(data)
        except json.JSONDecodeError:
            console.print(data)

    console.print("-" * 80)

@kv_group.command("get")
@entity_option(required=True)
@click.argument("key")
@with_kit
async def get_kv(kit, entity: str, key: str):
    """
    Get the value of a single key from the key-value store.

    Args:
        entity: The entity to read from.
        key: The key/property name to retrieve.
    """
    from sensorkit.backend.base import Entity

    context = kit.backend.key_value(entity=Entity.at(entity))
    item = await context.get(key)

    console.print("-" * 80)
    console.print(f'[bold white]{item.key}[/bold white]\n')
    data = item.value.decode()

    try:
        json.loads(data)
        print_json(data)
    except json.JSONDecodeError:
        console.print(data)

    console.print("-" * 80)

@kv_group.command("put")
@entity_option(required=True)
@click.argument("key")
@click.argument("value")
@with_kit
async def put_kv(kit, entity: str, key: str, value: str):
    """
    Put a new key-value pair into the store.

    Args:
        entity: The entity to write to.
        key: The key/property name to update.
        value: The value to store as a string.
    """
    from sensorkit.backend.base import Entity

    context = kit.backend.key_value(entity=Entity.at(entity))
    await context.update(key, value.encode())
    console.print(f'[bold white]SUCCESS: kv put for {entity=} {key=} {value=}[/bold white]')


@kv_group.command("delete")
@entity_option(required=True)
@click.argument("key", required=False)
@with_kit
async def delete_kv(kit, entity: str, key: str | None):
    """
    Delete one or more key-value entries from the SensorKit KV store.

    This command deletes a single key or all keys under a given entity in the key-value store.
    If a key is specified, only that key is deleted.
    If no key is provided, all keys under the specified entity are deleted.

    Args:
        entity (str): The entity namespace to operate on (required).
        key (str | None): The specific key to delete. If omitted, all keys under the entity will be deleted.
    """
    from sensorkit.backend.base import Entity

    context = kit.backend.key_value(entity=Entity.at(entity))

    if not key:
        entries = await context.get_all(deep=True)
        for e in entries:
            await context.delete(e.key.prop, revision=e.revision)
            console.print(f"[bold white]SUCCESS: kv delete on {e.key}[/bold white]")
    else:
        entry = await context.get(key)
        await context.delete(key, revision=entry.revision)
        console.print(f"[bold white]SUCCESS: kv delete on {key}[/bold white]")

class KVRecord(BaseModel):
    """
    A single key-value record with an associated entity.

    Attributes:
        entity (str): The name of the entity.
        key (str): The property/key name.
        value (Any): The value to associate with the key.
    """
    entity: str
    key: str
    value: Any

def load_kv_records(source: TextIO) -> list[KVRecord]:
    """
    Load and validate a list of KVRecord entries from a YAML stream.

    Accepts multiple YAML documents using `yaml.safe_load_all`.

    Args:
        source: A readable stream (file or stdin) containing YAML records.

    Returns:
        A list of validated KVRecord objects.
    """
    import yaml

    configurations: list[KVRecord] = []

    try:
        for doc in yaml.safe_load_all(source):
            if not doc:
                continue
            try:
                record = KVRecord.model_validate(doc)
                configurations.append(record)
            except ValidationError:
                console.print(f'[bold red]ERROR: {doc=} is not a KVRecord.[/bold red]')
    except yaml.YAMLError:
        console.print('[bold red]ERROR: could not load yaml.[/bold red]')

    return configurations

def expand_files(files: tuple[str, ...]) -> list[str]:
    """
    Expand glob patterns into a list of matching file paths.

    Args:
        files: Tuple of filenames or glob patterns.

    Returns:
        List of matching file paths.
    """
    if not files:
        return []

    expanded = []
    for pattern in files:
        expanded.extend(glob.glob(pattern))
    return expanded

@kv_group.command("load")
@entity_option()
@click.option('-n', "--no-clobber", is_flag=True)
@click.argument("files", nargs=-1, required=False)
@with_kit
async def load_kv_command(kit, entity: str | None, no_clobber: bool, files: tuple[str, ...]):
    """
    Load multiple key-value records from YAML files or stdin.

    Supports reading from:
    - One or more file paths or glob patterns
    - Standard input if no files are provided

    Each YAML document should conform to the KVRecord schema.

    Args:
        entity: If set, filters updates to only records matching this entity.
        files: One or more YAML file paths or globs.
    """
    from sensorkit.backend.base import Entity, KVError

    configurations: list[KVRecord]

    if len(files) == 0:
        configurations = load_kv_records(sys.stdin)
    else:
        configurations = []
        for path in expand_files(files):
            try:
                with open(path, "r") as f:
                    configurations.extend(
                        load_kv_records(f)
                    )
            except FileNotFoundError:
                console.print(f"[bold red]ERROR: file not found {path}[/bold red]")

    for item in configurations:
        if entity and entity != item.entity:
            continue

        context = kit.backend.key_value(entity=Entity.at(item.entity))

        if no_clobber:
            try:
                prev_entry = await context.get(item.key)
                prev_value = prev_entry.value.decode()
                try:
                    prev_value = json.loads(prev_value)
                except json.JSONDecodeError:
                    pass

                if prev_value == item.value:
                    console.print(f'[bold yellow]WARNING: kv put skipped for {item.entity}.{item.key}[/bold yellow]')
                    continue
            except KVError: # only errors with kvget
                pass

        await context.update(item.key, json.dumps(item.value).encode())
        console.print(f'[bold white]SUCCESS: kv put for {item.entity=} {item.key=} {item.value=}[/bold white]')
