import datetime
import os
import socket
import subprocess
from pathlib import Path

import flet as ft


APP_START = datetime.datetime.now()


PALETTE = {
    "bg": "#0B1020",
    "surface": "#121A2D",
    "surface_alt": "#1A243C",
    "graphite": "#2A3248",
    "deep_blue": "#1A2A6C",
    "neon": "#00D9FF",
    "text_main": "#EAF2FF",
    "text_muted": "#8EA4C7",
    "success": "#2ED47A",
}


def get_ip_address() -> str:
    try:
        host = socket.gethostname()
        return socket.gethostbyname(host)
    except Exception:
        return "N/A"


def get_active_processes_count() -> int:
    try:
        if os.name == "nt":
            result = subprocess.run(
                ["tasklist"], capture_output=True, text=True, check=True
            )
            lines = result.stdout.strip().splitlines()
            return max(0, len(lines) - 3)
        result = subprocess.run(["ps", "-e"], capture_output=True, text=True, check=True)
        lines = result.stdout.strip().splitlines()
        return max(0, len(lines) - 1)
    except Exception:
        return 0


def format_uptime() -> str:
    delta = datetime.datetime.now() - APP_START
    total = int(delta.total_seconds())
    hours, rem = divmod(total, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def file_icon_for(path: Path) -> ft.Icon:
    ext = path.suffix.lower()
    if ext == ".txt":
        return ft.Icon(ft.Icons.DESCRIPTION_ROUNDED, color="#9FB7FF")
    if ext == ".zip":
        return ft.Icon(ft.Icons.FOLDER_ZIP_ROUNDED, color="#FFC857")
    if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp"}:
        return ft.Icon(ft.Icons.IMAGE_ROUNDED, color="#73FBD3")
    return ft.Icon(ft.Icons.INSERT_DRIVE_FILE_ROUNDED, color="#AAB7D1")


class HoverButton(ft.Container):
    def __init__(self, text: str, on_click):
        super().__init__()
        self._on_click = on_click
        self.padding = ft.padding.symmetric(horizontal=16, vertical=10)
        self.border_radius = 14
        self.animate = ft.Animation(180, ft.AnimationCurve.EASE_OUT)
        self.animate_scale = ft.Animation(180, ft.AnimationCurve.EASE_OUT)
        self.bgcolor = PALETTE["surface_alt"]
        self.scale = 1.0
        self.ink = True
        self.content = ft.Row(
            [
                ft.Icon(ft.Icons.PLAY_ARROW_ROUNDED, color=PALETTE["neon"], size=18),
                ft.Text(text, color=PALETTE["text_main"], weight=ft.FontWeight.W_600),
            ],
            spacing=8,
            tight=True,
        )
        self.on_hover = self._handle_hover
        self.on_click = self._handle_click

    def _handle_hover(self, e: ft.HoverEvent):
        self.bgcolor = "#223455" if e.data == "true" else PALETTE["surface_alt"]
        self.scale = 1.04 if e.data == "true" else 1.0
        self.update()

    def _handle_click(self, _):
        if self._on_click:
            self._on_click()


def main(page: ft.Page):
    page.title = "Neon Server Console"
    page.padding = 24
    page.bgcolor = PALETTE["bg"]
    page.theme_mode = ft.ThemeMode.DARK
    page.window_min_width = 1000
    page.window_min_height = 700
    page.scroll = ft.ScrollMode.ADAPTIVE

    uptime_value = ft.Text("00:00:00", size=24, weight=ft.FontWeight.BOLD, color=PALETTE["text_main"])
    ip_value = ft.Text(get_ip_address(), size=22, weight=ft.FontWeight.W_600, color=PALETTE["text_main"])
    proc_value = ft.Text(str(get_active_processes_count()), size=24, weight=ft.FontWeight.BOLD, color=PALETTE["text_main"])

    def stat_card(title: str, value_control: ft.Control, icon: str) -> ft.Container:
        return ft.Container(
            expand=1,
            height=145,
            border_radius=22,
            padding=18,
            gradient=ft.LinearGradient(
                begin=ft.Alignment(-1, -1),
                end=ft.Alignment(1, 1),
                colors=[PALETTE["surface"], PALETTE["surface_alt"]],
            ),
            shadow=ft.BoxShadow(
                blur_radius=20,
                spread_radius=1,
                color="#3300D9FF",
                offset=ft.Offset(0, 8),
            ),
            content=ft.Column(
                [
                    ft.Row(
                        [
                            ft.Icon(icon, color=PALETTE["neon"], size=20),
                            ft.Text(title, color=PALETTE["text_muted"], size=14),
                        ],
                        spacing=8,
                    ),
                    ft.Container(height=10),
                    value_control,
                ],
                alignment=ft.MainAxisAlignment.START,
                spacing=2,
            ),
        )

    terminal_output = ft.ListView(
        expand=True,
        spacing=4,
        auto_scroll=True,
    )

    def append_terminal_line(text: str, color: str = PALETTE["text_main"]):
        terminal_output.controls.append(
            ft.Text(text, color=color, font_family="Consolas", size=13)
        )
        page.update()

    command_input = ft.TextField(
        hint_text="Введите команду и нажмите Enter...",
        border_radius=12,
        bgcolor="#0E1528",
        border_color=PALETTE["graphite"],
        color=PALETTE["text_main"],
        cursor_color=PALETTE["neon"],
        text_size=14,
        on_submit=lambda e: run_command(e.control.value),
        autofocus=True,
    )

    def run_command(command: str):
        command = command.strip()
        if not command:
            return

        append_terminal_line(f"$ {command}", PALETTE["neon"])
        command_input.value = ""
        page.update()

        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=20,
            )
            if result.stdout.strip():
                for line in result.stdout.splitlines():
                    append_terminal_line(line, PALETTE["text_main"])
            if result.stderr.strip():
                for line in result.stderr.splitlines():
                    append_terminal_line(line, "#FF7A7A")
        except subprocess.TimeoutExpired:
            append_terminal_line("Команда прервана по таймауту (20 секунд).", "#FF7A7A")
        except Exception as ex:
            append_terminal_line(f"Ошибка выполнения: {ex}", "#FF7A7A")

    def clear_terminal():
        terminal_output.controls.clear()
        append_terminal_line("Terminal cleared.", PALETTE["text_muted"])

    files_column = ft.Column(spacing=6, scroll=ft.ScrollMode.AUTO, expand=True)

    def refresh_files():
        files_column.controls.clear()
        cwd = Path.cwd()
        entries = sorted(cwd.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        for item in entries:
            subtitle = "folder" if item.is_dir() else (item.suffix.lower() or "file")
            icon = ft.Icon(ft.Icons.FOLDER_ROUNDED, color="#8BA8FF") if item.is_dir() else file_icon_for(item)
            files_column.controls.append(
                ft.Container(
                    border_radius=12,
                    bgcolor="#121A30",
                    padding=10,
                    content=ft.Row(
                        [
                            icon,
                            ft.Column(
                                [
                                    ft.Text(item.name, color=PALETTE["text_main"], size=14, no_wrap=True),
                                    ft.Text(subtitle, color=PALETTE["text_muted"], size=11),
                                ],
                                spacing=2,
                                expand=True,
                            ),
                        ],
                        spacing=10,
                    ),
                )
            )
        page.update()

    def build_panel(title: str, content: ft.Control, actions: list[ft.Control] | None = None) -> ft.Container:
        return ft.Container(
            border_radius=22,
            padding=16,
            expand=True,
            bgcolor=PALETTE["surface"],
            shadow=ft.BoxShadow(
                blur_radius=18,
                spread_radius=1,
                color="#2600D9FF",
                offset=ft.Offset(0, 6),
            ),
            content=ft.Column(
                [
                    ft.Row(
                        [
                            ft.Text(title, size=18, weight=ft.FontWeight.W_600, color=PALETTE["text_main"], expand=True),
                            *(actions or []),
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    ),
                    ft.Divider(color="#22314D"),
                    content,
                ],
                spacing=12,
                expand=True,
            ),
        )

    console_panel = build_panel(
        "Управляющая консоль",
        ft.Column(
            [
                ft.Container(
                    height=260,
                    border_radius=14,
                    padding=12,
                    bgcolor="#0A1222",
                    border=ft.border.all(1, "#243552"),
                    content=terminal_output,
                ),
                command_input,
            ],
            spacing=10,
            expand=True,
        ),
        actions=[
            HoverButton("Очистить", clear_terminal),
        ],
    )

    files_panel = build_panel(
        "Файлы сервера",
        ft.Container(
            expand=True,
            border_radius=14,
            bgcolor="#0A1222",
            border=ft.border.all(1, "#243552"),
            padding=10,
            content=files_column,
        ),
        actions=[
            HoverButton("Обновить", refresh_files),
        ],
    )

    page.add(
        ft.Column(
            [
                ft.Text(
                    "Server Control Center",
                    size=34,
                    weight=ft.FontWeight.BOLD,
                    color=PALETTE["text_main"],
                ),
                ft.Text(
                    "Deep Blue + Graphite theme with Neon accents",
                    color=PALETTE["text_muted"],
                ),
                ft.Row(
                    [
                        stat_card("Uptime", uptime_value, ft.Icons.SCHEDULE_ROUNDED),
                        stat_card("IP Address", ip_value, ft.Icons.LAN_ROUNDED),
                        stat_card("Active Processes", proc_value, ft.Icons.MEMORY_ROUNDED),
                    ],
                    spacing=14,
                ),
                ft.Row(
                    [console_panel, files_panel],
                    expand=True,
                    spacing=14,
                    vertical_alignment=ft.CrossAxisAlignment.START,
                ),
            ],
            spacing=16,
            expand=True,
        )
    )

    append_terminal_line("Neon terminal ready. Enter command...", PALETTE["text_muted"])
    refresh_files()

    async def ticker():
        import asyncio

        while True:
            uptime_value.value = format_uptime()
            proc_value.value = str(get_active_processes_count())
            page.update()
            await asyncio.sleep(1)

    page.run_task(ticker)


if __name__ == "__main__":
    ft.run(main)
