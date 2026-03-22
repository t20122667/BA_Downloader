import customtkinter as ctk
import tkinter.filedialog as filedialog
import tkinter.messagebox as messagebox
import tkinter as tk
import threading

from browser import authorize, check_authorization
from downloader import (
    collect_video_links,
    download_videos_sequential,
    is_processing_links
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
    DOWNLOAD_FOLDER,
    load_config,
    save_config,
    write_log,
    load_blacklist
)

import requests
from bs4 import BeautifulSoup

# Глобальное событие для управления загрузкой видео
pause_event = threading.Event()
pause_event.set()

# Отдельное событие для управления поиском ссылок
search_pause_event = threading.Event()
search_pause_event.set()

# Глобальное событие для управления процессом создания чёрного списка
blacklist_pause_event = threading.Event()
blacklist_pause_event.set()
blacklist_thread = None


def pause_link_processing():
    pause_event.clear()


def resume_link_processing():
    pause_event.set()


def open_download_folder(folder_path):
    import os
    try:
        os.startfile(folder_path)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Не удалось открыть папку: {e}")


def stop_downloading():
    import downloader
    downloader.stop_downloading_flag = True


def save_manual_download_folder(var):
    folder = var.get().strip()
    if folder:
        config = load_config()
        config["download_folder"] = folder
        save_config(config)


def create_gui():
    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")

    root = ctk.CTk()
    root.title("Beautiful Agony Video Downloader")
    root.geometry("800x1100")

    set_gui_root(root)

    main_frame = ctk.CTkFrame(master=root)
    main_frame.pack(padx=20, pady=20, fill="both", expand=True)

    #########################################
    # 1. Блок авторизации
    auth_frame = ctk.CTkFrame(master=main_frame, fg_color="transparent")
    auth_frame.pack(pady=10, fill="x")

    timer_label = ctk.CTkLabel(master=auth_frame, text="Нажмите 'Пройти авторизацию', чтобы начать.")
    timer_label.pack(side="left", padx=5, pady=5)

    check_button = ctk.CTkButton(
        master=auth_frame,
        text="Проверить авторизацию",
        state="disabled",
        command=lambda: check_authorization(timer_label, root)
    )
    check_button.pack(side="left", padx=5, pady=5)

    auth_button = ctk.CTkButton(
        master=auth_frame,
        text="Пройти авторизацию",
        command=lambda: on_authorize()
    )
    auth_button.pack(side="left", padx=5, pady=5)

    #########################################
    # 2. Блок настроек
    settings_frame = ctk.CTkFrame(master=main_frame, fg_color="transparent")
    settings_frame.pack(pady=10, fill="x")

    folder_frame = ctk.CTkFrame(master=settings_frame, fg_color="transparent")
    folder_frame.pack(pady=5, fill="x")
    folder_label = ctk.CTkLabel(master=folder_frame, text="Выберите папку загрузки:")
    folder_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

    config = load_config()
    default_folder = config.get("download_folder", DOWNLOAD_FOLDER)
    download_folder_var = tk.StringVar(value=default_folder)

    folder_entry = ctk.CTkEntry(master=folder_frame, textvariable=download_folder_var, width=300)
    folder_entry.grid(row=0, column=1, padx=5, pady=5)
    folder_entry.bind("<FocusOut>", lambda event: save_manual_download_folder(download_folder_var))

    select_folder_button = ctk.CTkButton(
        master=folder_frame,
        text="Выбрать папку",
        command=lambda: select_download_folder(download_folder_var)
    )
    select_folder_button.grid(row=0, column=2, padx=5, pady=5)

    url_frame = ctk.CTkFrame(master=settings_frame, fg_color="transparent")
    url_frame.pack(pady=5, fill="x")
    url_label = ctk.CTkLabel(master=url_frame, text="Введите начальный URL:")
    url_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

    default_url = "https://beautifulagony.com/public/main.php?page=view&mode=latest&offset=0"
    url_var = tk.StringVar(value=default_url)
    url_entry = ctk.CTkEntry(master=url_frame, textvariable=url_var, width=400)
    url_entry.grid(row=0, column=1, padx=5, pady=5)

    #########################################
    # 3. Блок сбора ссылок
    collection_frame = ctk.CTkFrame(master=main_frame, fg_color="transparent")
    collection_frame.pack(pady=10, fill="x")

    stop_empty_pages_var = tk.BooleanVar(value=False)

    def start_collecting():
        from downloader import is_collecting_links
        if is_collecting_links:
            messagebox.showinfo("Информация", "Сбор ссылок уже запущен!")
            return

        collect_button.grid_remove()
        search_buttons_frame.grid()
        stop_empty_pages_checkbox.grid()

        threading.Thread(
            target=lambda: collect_video_links(
                root,
                url_var.get(),
                download_folder_var.get(),
                search_pause_event,
                stop_empty_pages_var.get()
            ),
            daemon=True
        ).start()

    collect_button = ctk.CTkButton(
        master=collection_frame,
        text="Собрать ссылки на видео",
        command=start_collecting
    )
    collect_button.grid(row=0, column=0, columnspan=2, padx=5, pady=5, sticky="w")
    collect_button.grid_remove()

    search_buttons_frame = ctk.CTkFrame(master=collection_frame, fg_color="transparent")
    search_buttons_frame.grid(row=0, column=0, columnspan=2, padx=5, pady=5, sticky="w")
    search_buttons_frame.grid_remove()

    resume_search_button = ctk.CTkButton(
        master=search_buttons_frame,
        text="Возобновить поиск ссылок",
        command=lambda: search_pause_event.set()
    )
    resume_search_button.pack(side="left", padx=(0, 10))

    stop_search_button = ctk.CTkButton(
        master=search_buttons_frame,
        text="Остановить поиск ссылок",
        command=lambda: search_pause_event.clear()
    )
    stop_search_button.pack(side="left")

    stop_empty_pages_checkbox = ctk.CTkCheckBox(
        master=collection_frame,
        text="Остановить поиск, если 3 страницы подряд без новых ссылок",
        variable=stop_empty_pages_var
    )
    stop_empty_pages_checkbox.grid(row=1, column=0, columnspan=2, padx=5, pady=5, sticky="w")
    stop_empty_pages_checkbox.grid_remove()

    #########################################
    # 4. Блок последовательной загрузки
    download_control_frame = ctk.CTkFrame(master=main_frame, fg_color="transparent")
    download_control_frame.pack(pady=10, fill="x")

    stop_after_skips_var = tk.BooleanVar(value=False)

    def start_downloading():
        import downloader
        downloader.stop_downloading_flag = False
        download_seq_button.grid_remove()
        download_buttons_frame.grid()
        stop_download_button.grid()

        threading.Thread(
            target=lambda: downloader.download_videos_sequential(
                root,
                download_folder_var.get(),
                pause_event,
                stop_after_skips_var.get(),
                direction_var.get()
            ),
            daemon=True
        ).start()

    download_seq_button = ctk.CTkButton(
        master=download_control_frame,
        text="Скачать видео по ссылкам",
        command=start_downloading
    )
    download_seq_button.grid(row=0, column=0, padx=5, pady=5, sticky="w")
    download_seq_button.grid_remove()

    download_buttons_frame = ctk.CTkFrame(master=download_control_frame, fg_color="transparent")
    download_buttons_frame.grid(row=1, column=0, padx=5, pady=5, sticky="w")
    download_buttons_frame.grid_remove()

    resume_button = ctk.CTkButton(
        master=download_buttons_frame,
        text="Возобновить загрузку",
        command=lambda: pause_event.set()
    )
    resume_button.pack(side="left", padx=(0, 10))

    pause_button = ctk.CTkButton(
        master=download_buttons_frame,
        text="Пауза загрузки",
        command=lambda: pause_event.clear()
    )
    pause_button.pack(side="left")

    stop_download_button = ctk.CTkButton(
        master=download_control_frame,
        text="Остановить загрузку после текущего видео",
        command=stop_downloading
    )
    stop_download_button.grid(row=1, column=1, padx=5, pady=5, sticky="w")
    stop_download_button.grid_remove()

    stop_after_skips_checkbox = ctk.CTkCheckBox(
        master=download_control_frame,
        text="Остановить загрузку после 10 подряд пропущенных видео",
        variable=stop_after_skips_var
    )
    stop_after_skips_checkbox.grid(row=2, column=0, padx=5, pady=5, columnspan=2, sticky="w")
    stop_after_skips_checkbox.grid_remove()

    direction_var = tk.StringVar(value="сначала")
    direction_label = ctk.CTkLabel(master=download_control_frame, text="Направление обхода ссылок:")
    direction_label.grid(row=3, column=0, padx=5, pady=(5, 0), sticky="w")
    direction_label.grid_remove()

    first_radio = ctk.CTkRadioButton(
        master=download_control_frame,
        text="Сначала",
        variable=direction_var,
        value="сначала"
    )
    first_radio.grid(row=3, column=1, padx=5, pady=(5, 0), sticky="w")
    first_radio.grid_remove()

    last_radio = ctk.CTkRadioButton(
        master=download_control_frame,
        text="С конца",
        variable=direction_var,
        value="с конца"
    )
    last_radio.grid(row=3, column=2, padx=5, pady=(5, 0), sticky="w")
    last_radio.grid_remove()

    #########################################
    # 5. Файлы и папки
    files_frame = ctk.CTkFrame(master=main_frame, fg_color="transparent")
    files_frame.pack(pady=10, fill="x")

    open_links_button = ctk.CTkButton(
        master=files_frame,
        text="Открыть файл со ссылками",
        command=lambda: __import__("os").startfile("video_links.txt")
    )
    open_links_button.grid(row=0, column=0, padx=5, pady=5, sticky="w")

    open_downloads_button = ctk.CTkButton(
        master=files_frame,
        text="Открыть папку загрузок",
        command=lambda: open_download_folder(download_folder_var.get())
    )
    open_downloads_button.grid(row=0, column=1, padx=5, pady=5, sticky="w")

    #########################################
    # 6. Черный список
    blacklist_frame = ctk.CTkFrame(master=main_frame, fg_color="transparent")
    blacklist_frame.pack(pady=10, fill="x")

    def create_blacklist_process():
        total_blacklist = set()
        modes = ["males", "transgender"]

        for mode in modes:
            page = 0
            while True:
                blacklist_pause_event.wait()

                offset = page * 20
                url = f"https://beautifulagony.com/public/main.php?page=view&mode={mode}&offset={offset}"
                try:
                    write_log(f"Чёрный список [{mode}] – загрузка страницы offset={offset}...", log_type="info")
                    response = requests.get(url)
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

        try:
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

    def start_blacklist_creation():
        global blacklist_thread
        create_blacklist_button.grid_remove()
        stop_blacklist_button.grid()
        resume_blacklist_button.grid()
        blacklist_thread = threading.Thread(target=create_blacklist_process, daemon=True)
        blacklist_thread.start()

    create_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Создать черный список",
        command=start_blacklist_creation
    )
    create_blacklist_button.grid(row=0, column=0, padx=5, pady=5)

    stop_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Остановить создание чёрного списка",
        command=lambda: blacklist_pause_event.clear()
    )
    stop_blacklist_button.grid(row=0, column=0, padx=5, pady=5)
    stop_blacklist_button.grid_remove()

    resume_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Возобновить создание чёрного списка",
        command=lambda: blacklist_pause_event.set()
    )
    resume_blacklist_button.grid(row=0, column=1, padx=5, pady=5)
    resume_blacklist_button.grid_remove()

    open_blacklist_button = ctk.CTkButton(
        master=blacklist_frame,
        text="Открыть черный список",
        command=open_blacklist_file
    )
    open_blacklist_button.grid(row=0, column=2, padx=5, pady=5)

    #########################################
    # 7. Логи
    log_frame = ctk.CTkFrame(master=main_frame, fg_color="transparent")
    log_frame.pack(pady=10, fill="both", expand=True)

    try:
        log_textbox = ctk.CTkTextbox(master=log_frame, wrap="word", width=600, height=200)
    except AttributeError:
        log_textbox = tk.Text(master=log_frame, wrap="word", width=60, height=15)

    log_textbox.pack(pady=5, padx=5, fill="both", expand=True)

    log_buttons_frame = ctk.CTkFrame(master=log_frame, fg_color="transparent")
    log_buttons_frame.pack(pady=5, fill="x")

    log_file_button = ctk.CTkButton(master=log_buttons_frame, text="Открыть лог файл", command=open_log_file)
    log_file_button.grid(row=0, column=0, padx=5, pady=5)

    failed_file_button = ctk.CTkButton(
        master=log_buttons_frame,
        text="Открыть файл ошибок",
        command=open_failed_links_file
    )
    failed_file_button.grid(row=0, column=1, padx=5, pady=5)

    show_only_pages_and_errors = tk.BooleanVar(value=False)
    filter_checkbox = ctk.CTkCheckBox(
        master=log_buttons_frame,
        text="Показывать только страницы и ошибки",
        variable=show_only_pages_and_errors
    )
    filter_checkbox.grid(row=0, column=2, padx=5, pady=5)

    set_log_widgets(log_textbox, show_only_pages_and_errors)

    def on_authorize():
        authorize(timer_label, check_button, root)
        collect_button.grid()
        stop_empty_pages_checkbox.grid()
        download_seq_button.grid()
        stop_after_skips_checkbox.grid()
        direction_label.grid()
        first_radio.grid()
        last_radio.grid()

    root.mainloop()


if __name__ == "__main__":
    create_gui()