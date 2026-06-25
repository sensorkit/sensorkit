import asyncio
import io
import os
from typing import TextIO

from loguru import logger
from textual.app import App, ComposeResult
from textual.message import Message
from textual.widgets import Input, Log, Static

import sensorkit.api as sk
from sensorkit.auto.lifecycle import ControllerLifecycle
from sensorkit.core.controller import TaskExecutionState, InternalControllerState


class LogMessage(Message):
    def __init__(self, content: str) -> None:
        self.content = content
        super().__init__()


class AsyncLogSink(io.TextIOBase, TextIO):
    def __init__(self, app: App):
        self.app = app

    def write(self, message: str) -> int:
        if content := message.strip():
            self.app.post_message(LogMessage(content))
            return len(content)
        return 0


class LogView(Log):
    @property
    def can_focus(self):
        return False


class StatusBanner(Static):
    def update_status(self, lifecycle: ControllerLifecycle, controller_event: str):
        enable_state = "yes" if lifecycle._can_operate.is_set() else "no"
        demand_state = (
            f"{lifecycle.demand_state.state if lifecycle.demand_state.state else 'none'}"
            + (
                f" -> {lifecycle._demand.pending_value.state}"
                if lifecycle._demand.pending_change()
                else ""
            )
        )
        self.update(
            "   ".join(
                (
                    f"[bold red]Enabled:[/] {enable_state}",
                    f"[bold green]Demand state:[/] {demand_state}",
                    f"[bold yellow]Belief state:[/] {lifecycle.belief_state or 'unknown'}",
                    f"[bold cyan]Lifecycle step:[/] {lifecycle.step}",
                    f"[bold blue]Controller task:[/] {controller_event}",
                )
            )
        )


class LifecycleApp(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    StatusBanner {
        height: 1;
        padding: 0;
        background: $surface;
        color: $text;
    }
    Input {
        height: 3;
    }
    """

    def compose(self) -> ComposeResult:
        yield LogView(id="log", highlight=True)
        yield StatusBanner(id="status")
        yield Input(id="input", placeholder="command or 'help'")

    def on_mount(self) -> None:
        logger.remove()
        logger.add(
            AsyncLogSink(self),
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
            level="DEBUG",
        )

        self._task = asyncio.create_task(self.driver())

    def on_ready(self):
        self.query_one(Input).focus()

    def on_mouse_down(self):
        self.query_one(Input).focus()

    def on_log_message(self, message: LogMessage) -> None:
        self.query_one(Log).write_line(message.content)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        cmd = tuple(s.strip() for s in event.value.split())

        if not cmd:
            return

        log = self.query_one(LogView)

        match cmd:
            case ["enable"]:
                log.write_line("Enabling lifecycle")
                self.lifecycle.enable()
            case ["operate", _]:
                program = self.client.program(cmd[1])
                log.write_line(f"Setting OPERATE demand state (program={program.entity})")

                if program:
                    self.lifecycle.set_demand_state(InternalControllerState.OPERATE, program=program)
                else:
                    log.write_line("")
            case ["standby"]:
                log.write_line("Setting STANDBY demand state")
                self.lifecycle.set_demand_state(InternalControllerState.STANDBY)
            case ["shutdown"]:
                log.write_line("Setting SHUTDOWN demand state")
                self.lifecycle.set_demand_state(InternalControllerState.SHUTDOWN)
            case ["help"]:
                log.write_line(
                    "Available commands: enable, operate <program>, standby, shutdown, quit"
                )
            case ["quit"]:
                self.call_next(self.action_quit)
            case _:
                log.write_line(f"Invalid command format: {' '.join(cmd)}")

        self.query_one(Input).value = ""

    async def driver(self, controller_name: str = os.getenv("CONTROLLER", "sensor_control")):
        log = self.query_one(LogView)
        log.write_line(f"Constructing ControllerLifecycle for controller '{controller_name}'")

        self.client = await sk.connect()
        controller = self.client.controller(controller_name)
        self.lifecycle = ControllerLifecycle()
        self.lifecycle.start(controller)

        async def _listen_for_controller_events(queue: asyncio.Queue):
            async for _, event in await controller.monitor(TaskExecutionState):
                if event.executing and not event.aborting:
                    await queue.put(f"{event.execution.task.task_type} {event.execution.task_id.hex[:8]}")
                elif event.finished:
                    await queue.put(None)

        queue: asyncio.Queue[str | None] = asyncio.Queue()
        _t = asyncio.create_task(_listen_for_controller_events(queue))
        task_str = None

        while True:
            while not queue.empty():
                task_str = queue.get_nowait()
                queue.task_done()

            self.query_one(StatusBanner).update_status(self.lifecycle, task_str or "none")
            await asyncio.sleep(1)


if __name__ == "__main__":
    LifecycleApp().run()
