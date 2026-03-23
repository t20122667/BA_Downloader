import os
import threading
from pathlib import Path
import tkinter as tk
import tkinter.messagebox as messagebox

import customtkinter as ctk
import requests
from bs4 import BeautifulSoup

from browser import authorize, check_authorization
import downloader
from downloader import (
    download_videos_sequential,
    request_stop_after_current,
    ProgressPanelUI,
    get_worker_count,
)
from utils import (
    open_log_file,
    open_failed_links_file,
    open_blacklist_file,
    select_download_folder,
    set_log_widgets,
    set_gui_root,
    safe_showinfo,
    safe_showerror,
    run_on_ui_thread,
    DOWNLOAD_FOLDER,
    load_config,
    save_config,
    write_log,
    create_secondary_blacklist_from_friends,
    SECONDARY_BLACKLIST_FILE,
    SECONDARY_BLACKLIST_REVIEW_FILE,
)

pause_event = threading.Event()
pause_event.set()

search_pause_event = threading.Event()
search_pause_event.set()

blacklist_pause_event = threading.Event()
blacklist_pause_event.set()

secondary_blacklist_pause_event = threading.Event()
secondary_blacklist_pause_event.set()

blacklist_thread = None
secondary_blacklist_thread = None


def open_download_folder(folder_path: str):
    try:
        os.startfile(folder_path)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Не удалось открыть папку: {e}")


def open_text_file(file_path: str, title: str = "Ошибка"):
    try:
        if os.path.exists(file_path):
            os.startfile(file_path)
        else:
            messagebox.showerror(title, f"Файл не найден:\n{file_path}")
    except Exception as e:
        messagebox.showerror(title, f"Не удалось открыть файл:\n{file_path}\n\n{e}")


def clear_text_file(file_path: str, title: str):
    try:
        display_name = Path(file_path).name
        confirmed = messagebox.askyesno(
            "Подтверждение",
            f"Очистить файл:\n{display_name}?"
        )
        if not confirmed:
            return

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("")

        write_log(f"Файл очищен: {display_name}", log_type="info")
        safe_showinfo(title, f"Файл очищен:\n{display_name}")
    except Exception as e:
        write_log(f"Не удалось очистить файл {file_path}: {e}", log_type="error")
        safe_showerror(title, f"Не удалось очистить файл:\n{file_path}\n\n{e}")


def save_manual_download_folder(var: tk.StringVar):
    folder = var.get().strip()
    if folder:
        config = load_config()
        config["download_folder"] = folder
        save_config(config)


def _parse_positive_int(value, default: int = 1) -> int:
    try:
        return max(1, int(str(value).strip()))
    except Exception:
        return max(1, int(default))


def save_lead_limit(var: tk.StringVar):
    config = load_config()
    normalized = str(_parse_positive_int(var.get(), config.get("lead_limit", 30)))
    if var.get() != normalized:
        var.set(normalized)
    config["lead_limit"] = int(normalized)
    save_config(config)


def _maximize_root(root):
    try:
        root.state("zoomed")
    except Exception:
        try:
            root.attributes("-zoomed", True)
        except Exception:
            screen_w = root.winfo_screenwidth()
            screen_h = root.winfo_screenheight()
            root.geometry(f"{screen_w}x{screen_h}+0+0")


def create_gui():
    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    root = ctk.CTk()
    root.title("Beautiful Agony Video Downloader")
    root.after(100, lambda: _maximize_root(root))

    set_gui_root(root)

    collecting_now = False

    config = load_config()
    initial_lead_limit = _parse_positive_int(config.get("lead_limit", 30), 30)
    lead_limit_var = tk.StringVar(value=str(initial_lead_limit))
    lead_status_var = tk.StringVar(value=f"Опережение сбора: 0 / {initial_lead_limit}")

    root.grid_rowconfigure(0, weight=1)
    root.grid_columnconfigure(0, weight=1)

    panel_gap = 8

    top_frame = ctk.CTkFrame(master=root, corner_radius=0, fg_color="transparent")
    top_frame.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)
    top_frame.grid_rowconfigure(0, weight=1)
    top_frame.grid_columnconfigure(0, weight=1, uniform="main_columns")
    top_frame.grid_columnconfigure(1, weight=1, uniform="main_columns")

    left_panel = ctk.CTkFrame(master=top_frame, fg_color="transparent", border_width=0, corner_radius=0)
    left_panel.grid(row=0, column=0, sticky="nsew", padx=(0, panel_gap // 2), pady=0)
    left_panel.grid_rowconfigure(0, weight=0)
    left_panel.grid_rowconfigure(1, weight=1)
    left_panel.grid_columnconfigure(0, weight=1)

    right_panel = ctk.CTkFrame(master=top_frame, fg_color="transparent", border_width=0, corner_radius=0)
    right_panel.grid(row=0, column=1, sticky="nsew", padx=(panel_gap // 2, 0), pady=0)
    right_panel.grid_rowconfigure(1, weight=1)
    right_panel.grid_columnconfigure(0, weight=1)

    controls_card = ctk.CTkFrame(master=left_panel, border_width=1, corner_radius=12)
    controls_card.grid(row=0, column=0, sticky="nsew", padx=0, pady=(0, panel_gap // 2))
    controls_card.grid_rowconfigure(1, weight=1)
    controls_card.grid_columnconfigure(0, weight=1)

    controls_title = ctk.CTkLabel(
        master=controls_card,
        text="Панель управления",
        anchor="w",
        font=ctk.CTkFont(size=18, weight="bold")
    )
    controls_title.grid(row=0, column=0, sticky="ew", padx=12, pady=(10, 6))

    controls_panel = ctk.CTkFrame(master=controls_card, fg_color="transparent")
    controls_panel.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
    controls_panel.grid_columnconfigure(0, weight=1)

    progress_card = ctk.CTkFrame(master=left_panel, border_width=1, corner_radius=12)
    progress_card.grid(row=1, column=0, sticky="nsew", padx=0, pady=(panel_gap // 2, 0))
    progress_card.grid_rowconfigure(1, weight=1)
    progress_card.grid_columnconfigure(0, weight=1)

    progress_host = ctk.CTkFrame(master=progress_card, fg_color="transparent")
    progress_host.grid(row=1, column=0, sticky="nsew", padx=8, pady=(0, 8))
    progress_host.grid_rowconfigure(0, weight=1)
    progress_host.grid_columnconfigure(0, weight=1)

    progress_min_height = 220

    worker_count = get_worker_count()
    progress_panel = ProgressPanelUI(progress_host, worker_count)

    auth_frame = ctk.CTkFrame(master=controls_panel, fg_color="transparent")
    auth_frame.pack(padx=12, pady=(12, 8), fill="x")

    timer_label = ctk.CTkLabel(master=auth_frame, text="Авторизация")
    timer_label.pack(side="left", padx=5, pady=5)

    settings_frame = ctk.CTkFrame(master=controls_panel, fg_color="transparent")
    settings_frame.pack(padx=12, pady=8, fill="x")

    folder_frame = ctk.CTkFrame(master=settings_frame, fg_color="transparent")
    folder_frame.pack(pady=5, fill="x")
    folder_frame.grid_columnconfigure(1, weight=1)

    folder_label = ctk.CTkLabel(master=folder_frame, text="Выберите папку загрузки:")
    folder_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

    default_folder = config.get("download_folder", DOWNLOAD_FOLDER)
    download_folder_var = tk.StringVar(value=default_folder)

    folder_entry = ctk.CTkEntry(master=folder_frame, textvariable=download_folder_var)
    folder_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
    folder_entry.bind("<FocusOut>", lambda event: save_manual_download_folder(download_folder_var))

    select_folder_button = ctk.CTkButton(
        master=folder_frame,
        text="Выбрать папку",
        command=lambda: select_download_folder(download_folder_var)
    )
    select_folder_button.grid(row=0, column=2, padx=5, pady=5)

    url_frame = ctk.CTkFrame(master=settings_frame, fg_color="transparent")
    url_frame.pack(pady=5, fill="x")
    url_frame.grid_columnconfigure(1, weight=1)

    url_label = ctk.CTkLabel(master=url_frame, text="Введите начальный URL:")
    url_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

    default_url = "https://beautifulagony.com/public/main.php?page=view&mode=latest&offset=0"
    url_var = tk.StringVar(value=default_url)
    url_entry = ctk.CTkEntry(master=url_frame, textvariable=url_var)
    url_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

    collection_frame = ctk.CTkFrame(master=controls_panel, fg_color="transparent")
    collection_frame.pack(padx=12, pady=8, fill="x")
    collection_frame.grid_columnconfigure(1, weight=1)

    stop_empty_pages_var = tk.BooleanVar(value=False)

    lead_limit_label = ctk.CTkLabel(master=collection_frame, text="Макс. опережение сбора:")
    lead_limit_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

    lead_limit_spinbox = ctk.CTkEntry(master=collection_frame, textvariable=lead_limit_var, width=90)
    lead_limit_spinbox.grid(row=0, column=1, padx=5, pady=5, sticky="w")
    lead_limit_spinbox.bind("<FocusOut>", lambda event: save_lead_limit(lead_limit_var))

    lead_status_label = ctk.CTkLabel(master=collection_frame, textvariable=lead_status_var, anchor="w")
    lead_status_label.grid(row=1, column=0, columnspan=2, padx=5, pady=(0, 5), sticky="w")

    download_control_frame = ctk.CTkFrame(master=controls_panel, fg_color="transparent")
    download_control_frame.pack(padx=12, pady=8, fill="x")

    stop_after_skips_var = tk.BooleanVar(value=False)

    def _refresh_lead_status_label(*_args):
        limit = _parse_positive_int(lead_limit_var.get(), initial_lead_limit)
        current_text = lead_status_var.get() or ""
        if not collecting_now or "/" not in current_text:
            lead_status_var.set(f"Опережение сбора: 0 / {limit}")
        else:
            lead_status_var.set(current_text.rsplit('/', 1)[0] + f"/ {limit}")

    def _on_lead_limit_change(*_args):
        _refresh_lead_status_label()
        value = (lead_limit_var.get() or "").strip()
        if value.isdigit():
            config = load_config()
            normalized = _parse_positive_int(value, initial_lead_limit)
            if config.get("lead_limit") != normalized:
                config["lead_limit"] = normalized
                save_config(config)

    lead_limit_var.trace_add("write", _on_lead_limit_change)
    _refresh_lead_status_label()

    def enable_download_controls_after_authorize():
        collect_button.configure(state="normal")
        stop_empty_pages_checkbox.configure(state="normal")
        stop_after_skips_checkbox.configure(state="normal")

    def show_post_auth_controls():
        enable_download_controls_after_authorize()

    def set_idle_collection_ui():
        collect_button.grid()
        search_buttons_frame.grid_remove()
        stop_download_button.grid_remove()

    def set_running_collection_ui():
        collect_button.grid_remove()
        search_buttons_frame.grid()
        stop_download_button.grid()

    def set_pipeline_controls_enabled(enabled: bool):
        state = "normal" if enabled else "disabled"

        auth_button.configure(state=state)
        check_button.configure(state=state if enabled else "disabled")
        select_folder_button.configure(state=state)
        folder_entry.configure(state=state)
        url_entry.configure(state=state)
        stop_empty_pages_checkbox.configure(state=state if enabled else "disabled")
        stop_after_skips_checkbox.configure(state=state if enabled else "disabled")
        lead_limit_spinbox.configure(state=state)
        create_blacklist_button.configure(state=state)
        clear_links_button.configure(state=state)
        clear_successful_button.configure(state=state)

    def finish_pipeline_ui():
        nonlocal collecting_now
        collecting_now = False
        set_pipeline_controls_enabled(True)
        set_idle_collection_ui()
        _refresh_lead_status_label()

    def start_collecting():
        nonlocal collecting_now

        if collecting_now:
            safe_showinfo("Информация", "Сбор/загрузка уже запущены.")
            return

        if downloader.is_collecting_links:
            safe_showinfo("Информация", "Сбор ссылок уже запущен.")
            return

        collecting_now = True
        set_pipeline_controls_enabled(False)
        set_running_collection_ui()

        def worker():
            try:
                download_videos_sequential(
                    root=root,
                    download_folder=download_folder_var.get(),
                    pause_event=pause_event,
                    stop_after_skip=stop_after_skips_var.get(),
                    start_url=url_var.get(),
                    search_pause_event=search_pause_event,
                    stop_on_empty_pages=stop_empty_pages_var.get(),
                    prefetch_limit=lambda: _parse_positive_int(lead_limit_var.get(), initial_lead_limit),
                    progress_panel=progress_panel,
                    lead_status_callback=lambda current, limit: run_on_ui_thread(
                        lead_status_var.set, f"Опережение сбора: {current} / {limit}"
                    ),
                )
            finally:
                run_on_ui_thread(finish_pipeline_ui)

        threading.Thread(target=worker, daemon=True).start()

    collect_button = ctk.CTkButton(
        master=collection_frame,
        text="Собрать ссылки и скачать видео",
        command=start_collecting,
        state="disabled"
    )
    collect_button.grid(row=2, column=0, columnspan=2, padx=5, pady=5, sticky="w")

    search_buttons_frame = ctk.CTkFrame(master=collection_frame, fg_color="transparent")
    search_buttons_frame.grid(row=2, column=0, columnspan=2, padx=5, pady=5, sticky="w")
    search_buttons_frame.grid_remove()

    resume_search_button = ctk.CTkButton(
        master=search_buttons_frame,
        text="Возобновить поиск ссылок",
        command=search_pause_event.set
    )
    resume_search_button.pack(side="left", padx=(0, 10))

    stop_search_button = ctk.CTkButton(
        master=search_buttons_frame,
        text="Остановить поиск ссылок",
        command=search_pause_event.clear
    )
    stop_search_button.pack(side="left")

    stop_empty_pages_checkbox = ctk.CTkCheckBox(
        master=collection_frame,
        text="Остановить поиск, если 3 страницы подряд без новых ссылок",
        variable=stop_empty_pages_var,
        state="disabled"
    )
    stop_empty_pages_checkbox.grid(row=3, column=0, columnspan=2, padx=5, pady=5, sticky="w")

    stop_download_button = ctk.CTkButton(
        master=download_control_frame,
        text="Остановить после текущих активных видео",
        command=request_stop_after_current
    )
    stop_download_button.grid(row=0, column=0, padx=5, pady=5, sticky="w")
    stop_download_button.grid_remove()

    stop_after_skips_checkbox = ctk.CTkCheckBox(
        master=download_control_frame,
        text="Остановить загрузку после 10 подряд пропущенных видео",
        variable=stop_after_skips_var,
        state="disabled"
    )
    stop_after_skips_checkbox.grid(row=1, column=0, padx=5, pady=5, columnspan=2, sticky="w")

    files_frame = ctk.CTkFrame(master=controls_panel, fg_color="transparent")
    files_frame.pack(padx=12, pady=8, fill="x")
    files_frame.grid_columnconfigure(0, weight=1)
    files_frame.grid_columnconfigure(1, weight=1)

    open_links_button = ctk.CTkButton(
        master=files_frame,
        text="Открыть файл со ссылками",
        command=lambda: open_text_file("video_links.txt", "Файл ссылок")
    )
    open_links_button.grid(row=0, column=0, padx=5, pady=5, sticky="ew")

    clear_links_button = ctk.CTkButton(
        master=files_frame,
        text="Очистить файл со ссылками",
        command=lambda: clear_text_file("video_links.txt", "Файл ссылок")
    )
    clear_links_button.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

    open_successful_button = ctk.CTkButton(
        master=files_frame,
        text="Открыть успешные загрузки",
        command=lambda: open_text_file("successful_downloads.txt", "Успешные загрузки")
    )
    open_successful_button.grid(row=1, column=0, padx=5, pady=5, sticky="ew")

    clear_successful_button = ctk.CTkButton(
        master=files_frame,
        text="Очистить успешные загрузки",
        command=lambda: clear_text_file("successful_downloads.txt", "Успешные загрузки")
    )
    clear_successful_button.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

    open_downloads_button = ctk.CTkButton(
        master=files_frame,
        text="Открыть папку загрузок",
        command=lambda: open_download_folder(download_folder_var.get())
    )
    open_downloads_button.grid(row=2, column=0, padx=5, pady=5, sticky="ew")

    open_config_button = ctk.CTkButton(
        master=files_frame,
        text="Открыть config.json",
        command=lambda: open_text_file("config.json", "Конфигурация")
    )
    open_config_button.grid(row=2, column=1, padx=5, pady=5, sticky="ew")

    blacklist_frame = ctk.CTkFrame(master=controls_panel, fg_color="transparent")
    blacklist_frame.pack(padx=12, pady=8, fill="x")
    blacklist_frame.grid_columnconfigure(0, weight=1)
    blacklist_frame.grid_columnconfigure(1, weight=1)

    def finish_blacklist_ui():
        create_blacklist_button.grid()
        open_blacklist_button.grid()
        stop_blacklist_button.grid_remove()
        resume_blacklist_button.grid_remove()

    def create_blacklist_process():
        total_blacklist = set()
        modes = ["males", "transgender"]

        try:
            for mode in modes:
                page = 0
                while True:
                    blacklist_pause_event.wait()

                    offset = page * 20
                    url = f"https://beautifulagony.com/public/main.php?page=view&mode={mode}&offset={offset}"
                    try:
                        write_log(f"Чёрный список [{mode}] – загрузка страницы offset={offset}...", log_type="page")
                        response = requests.get(url, timeout=20)
                        if response.status_code == 200:
                            soup = BeautifulSoup(response.text, "html.parser")
                            elements = soup.find_all("font", class_="agonyid")
                            page_numbers = set()

                            for el in elements:
                                text = el.get_text(strip=True)
                                if text.startswith("#"):
                                    num = text[1:]
                                    if num.isdigit() and len(num) == 4:
                                        page_numbers.add(num)

                            if not page_numbers:
                                write_log(
                                    f"Чёрный список [{mode}] – страница offset={offset} не содержит номеров. Завершаем режим '{mode}'.",
                                    log_type="info"
                                )
                                break

                            write_log(
                                f"Чёрный список [{mode}] – найдено {len(page_numbers)} номеров на странице offset={offset}.",
                                log_type="info"
                            )
                            total_blacklist.update(page_numbers)
                            page += 1
                        else:
                            write_log(
                                f"Чёрный список [{mode}] – не удалось загрузить страницу {url}. Код {response.status_code}.",
                                log_type="error"
                            )
                            break
                    except Exception as e:
                        write_log(f"Чёрный список [{mode}] – ошибка при обработке {url}: {e}", log_type="error")
                        break

            with open("blacklist.txt", "w", encoding="utf-8") as f:
                for num in sorted(total_blacklist):
                    f.write(num + "\n")

            write_log(
                f"Чёрный список создан успешно. Всего найдено {len(total_blacklist)} номеров. Файл: blacklist.txt",
                log_type="info"
            )
            safe_showinfo("Черный список", f"Черный список создан.\nНайдено чисел: {len(total_blacklist)}")
        except Exception as e:
            write_log(f"Ошибка при записи файла blacklist.txt: {e}", log_type="error")
            safe_showerror("Ошибка", f"Не удалось записать blacklist.txt: {e}")
        finally:
            run_on_ui_thread(finish_blacklist_ui)

    def start_blacklist_creation():
        global blacklist_thread

        blacklist_pause_event.set()
        create_blacklist_button.grid_remove()
        open_blacklist_button.grid_remove()
        stop_blacklist_button.grid()
        resume_blacklist_button.grid()

        blacklist_thread = threading.Thread(target=create_blacklist_process, daemon=True)
        blacklist_thread.start()

    def finish_secondary_blacklist_ui():
        create_secondary_blacklist_button.grid()
        open_secondary_blacklist_button.grid()
        stop_secondary_blacklist_button.grid_remove()
        resume_secondary_blacklist_button.grid_remove()

    def create_secondary_blacklist_process():
        try:
            matched_titles = create_secondary_blacklist_from_friends(
                main_blacklist_file="blacklist.txt",
                output_file=SECONDARY_BLACKLIST_FILE,
                pause_event=secondary_blacklist_pause_event,
            )
            safe_showinfo(
                "Второй чёрный список",
                f"Второй чёрный список создан.\nНайдено видео: {len(matched_titles)}\nФайл: {SECONDARY_BLACKLIST_FILE}",
            )
        except Exception as e:
            write_log(f"Ошибка создания второго чёрного списка: {e}", log_type="error")
            safe_showerror("Ошибка", f"Не удалось создать второй чёрный список: {e}")
        finally:
            run_on_ui_thread(finish_secondary_blacklist_ui)

    def start_secondary_blacklist_creation():
        global secondary_blacklist_thread

        secondary_blacklist_pause_event.set()
        create_secondary_blacklist_button.grid_remove()
        open_secondary_blacklist_button.grid_remove()
        stop_secondary_blacklist_button.grid()
        resume_secondary_blacklist_button.grid()

        secondary_blacklist_thread = threading.Thread(target=create_secondary_blacklist_process, daemon=True)
        secondary_blacklist_thread.start()

    create_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Создать черный список",
        command=start_blacklist_creation
    )
    create_blacklist_button.grid(row=0, column=0, padx=5, pady=5, sticky="ew")

    open_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Открыть черный список",
        command=open_blacklist_file
    )
    open_blacklist_button.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

    stop_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Остановить создание чёрного списка",
        command=blacklist_pause_event.clear
    )
    stop_blacklist_button.grid(row=0, column=0, padx=5, pady=5, sticky="ew")
    stop_blacklist_button.grid_remove()

    resume_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Возобновить создание чёрного списка",
        command=blacklist_pause_event.set
    )
    resume_blacklist_button.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
    resume_blacklist_button.grid_remove()

    create_secondary_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Создать второй чёрный список",
        command=start_secondary_blacklist_creation
    )
    create_secondary_blacklist_button.grid(row=1, column=0, padx=5, pady=5, sticky="ew")

    open_secondary_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Открыть второй чёрный список",
        command=lambda: open_text_file(SECONDARY_BLACKLIST_FILE, "Второй чёрный список")
    )
    open_secondary_blacklist_button.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

    stop_secondary_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Остановить второй чёрный список",
        command=secondary_blacklist_pause_event.clear
    )
    stop_secondary_blacklist_button.grid(row=1, column=0, padx=5, pady=5, sticky="ew")
    stop_secondary_blacklist_button.grid_remove()

    resume_secondary_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Возобновить второй чёрный список",
        command=secondary_blacklist_pause_event.set
    )
    resume_secondary_blacklist_button.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
    resume_secondary_blacklist_button.grid_remove()

    progress_title = ctk.CTkLabel(
        master=progress_card,
        text=f"Окно потоков / активные загрузки ({worker_count})",
        anchor="w",
        font=ctk.CTkFont(size=18, weight="bold")
    )
    progress_title.grid(row=0, column=0, sticky="ew", padx=12, pady=(10, 6))

    log_title = ctk.CTkLabel(
        master=right_panel,
        text="Окно активности / логов",
        anchor="w",
        font=ctk.CTkFont(size=18, weight="bold")
    )
    log_title.grid(row=0, column=0, sticky="ew", padx=12, pady=(0, 6))

    log_frame = ctk.CTkFrame(master=right_panel, border_width=1, corner_radius=12)
    log_frame.grid(row=1, column=0, sticky="nsew", padx=0, pady=0)
    log_frame.grid_rowconfigure(0, weight=1)
    log_frame.grid_columnconfigure(0, weight=1)

    try:
        log_textbox = ctk.CTkTextbox(master=log_frame, wrap="word")
    except AttributeError:
        log_textbox = tk.Text(master=log_frame, wrap="word")

    log_textbox.grid(row=0, column=0, padx=8, pady=8, sticky="nsew")

    log_buttons_frame = ctk.CTkFrame(master=log_frame, fg_color="transparent")
    log_buttons_frame.grid(row=1, column=0, pady=(0, 8), padx=8, sticky="ew")

    log_file_button = ctk.CTkButton(master=log_buttons_frame, text="Открыть лог файл", command=open_log_file)
    log_file_button.grid(row=0, column=0, padx=5, pady=5, sticky="w")

    failed_file_button = ctk.CTkButton(
        master=log_buttons_frame,
        text="Открыть файл ошибок",
        command=open_failed_links_file
    )
    failed_file_button.grid(row=0, column=1, padx=5, pady=5, sticky="w")

    show_only_pages_and_errors = tk.BooleanVar(value=False)
    filter_checkbox = ctk.CTkCheckBox(
        master=log_buttons_frame,
        text="Показывать только страницы и ошибки",
        variable=show_only_pages_and_errors
    )
    filter_checkbox.grid(row=0, column=2, padx=5, pady=5, sticky="w")

    progress_card.grid_propagate(True)
    log_frame.grid_propagate(True)
    right_panel.grid_propagate(True)
    left_panel.grid_rowconfigure(1, minsize=progress_min_height)

    set_log_widgets(log_textbox, show_only_pages_and_errors)

    def on_authorize():
        enable_download_controls_after_authorize()
        authorize(timer_label, check_button)

    check_button = ctk.CTkButton(
        master=auth_frame,
        text="Проверить авторизацию",
        state="disabled",
        command=lambda: check_authorization(timer_label, on_success=show_post_auth_controls)
    )
    check_button.pack(side="left", padx=5, pady=5)

    auth_button = ctk.CTkButton(
        master=auth_frame,
        text="Пройти авторизацию",
        command=on_authorize
    )
    auth_button.pack(side="left", padx=5, pady=5)

    set_idle_collection_ui()

    root.mainloop()


if __name__ == "__main__":
    create_gui()