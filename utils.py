import ctypes
import datetime
import json
import os
import queue
import subprocess
import pickle
import time
import threading
import re
from urllib.parse import urljoin
import tkinter as tk
import webbrowser
from email.utils import parsedate_to_datetime
import requests
from bs4 import BeautifulSoup
from mutagen.mp4 import MP4
from selenium.webdriver.common.by import By

# ======================= Функции работы с конфигурацией =======================
CONFIG_FILE = "config.json"

# ======================= Константы и пути к файлам =======================
DOWNLOAD_FOLDER = r"S:\Test"
cookies_path = "cookies.pkl"
log_file_path = "logs.txt"
failed_links_path = "failed_links.txt"

# Если лог-файл существует, очищаем его
if os.path.exists(log_file_path):
    with open(log_file_path, "w", encoding="utf-8") as f:
        f.write("")

# Виджет для логирования (будет задан из GUI)
log_text = None
show_only_pages_and_errors = None

# Tkinter root и очередь задач для безопасного обновления GUI из потоков
gui_root = None
_ui_queue = queue.Queue()
_ui_poller_started = False
_video_date_records_lock = threading.Lock()

# Допуск сравнения дат (секунды)
DATE_SYNC_TOLERANCE_SECONDS = 2.0


# ======================= Потокобезопасная работа с GUI =======================
def set_gui_root(root):
    global gui_root
    gui_root = root
    _start_ui_poller()


def _start_ui_poller():
    global _ui_poller_started

    if gui_root is None or _ui_poller_started:
        return

    _ui_poller_started = True

    def process_queue():
        while True:
            try:
                callback, args, kwargs = _ui_queue.get_nowait()
            except queue.Empty:
                break

            try:
                callback(*args, **kwargs)
            except Exception as e:
                print(f"Ошибка при обработке GUI-задачи: {e}")

        if gui_root is not None:
            try:
                gui_root.after(50, process_queue)
            except Exception:
                pass

    gui_root.after(50, process_queue)


def run_on_ui_thread(callback, *args, **kwargs):
    """
    Безопасно выполняет callback в главном потоке Tkinter.
    Если root ещё не установлен ИЛИ вызов уже идёт из главного потока,
    выполняет callback сразу.
    """
    if gui_root is None or threading.current_thread() is threading.main_thread():
        callback(*args, **kwargs)
        return

    _ui_queue.put((callback, args, kwargs))


def safe_showinfo(title, message):
    from tkinter import messagebox
    run_on_ui_thread(messagebox.showinfo, title, message)


def safe_showerror(title, message):
    from tkinter import messagebox
    run_on_ui_thread(messagebox.showerror, title, message)


# ======================= Функции логирования =======================
def set_log_widgets(text_widget, checkbox_var):
    global log_text, show_only_pages_and_errors
    log_text = text_widget
    show_only_pages_and_errors = checkbox_var


def _append_log_to_widget(log_entry):
    if log_text is not None:
        log_text.insert(tk.END, f"{log_entry}\n")
        log_text.see(tk.END)


def write_log(message, log_type="info"):
    """
    Если log_type равен "video", добавляется дата и время.
    Для остальных сообщений пишется только само сообщение.
    Лог в GUI пишется только через главный поток Tkinter.
    """
    if log_type == "video":
        timestamp = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
    else:
        log_entry = message

    with open(log_file_path, "a", encoding="utf-8") as log_file:
        log_file.write(f"{log_entry}\n")

    if show_only_pages_and_errors is not None and show_only_pages_and_errors.get():
        if log_type not in ["page", "error"]:
            return

    if log_text is not None:
        run_on_ui_thread(_append_log_to_widget, log_entry)


# ======================= Функции работы с конфигурацией =======================
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            write_log(f"Ошибка загрузки конфигурации: {e}", log_type="error")
    return {}


def save_config(config):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=4)
    except Exception as e:
        write_log(f"Ошибка сохранения конфигурации: {e}", log_type="error")


def save_failed_link(link):
    with open(failed_links_path, "a", encoding="utf-8") as failed_file:
        failed_file.write(f"{link}\n")


def open_log_file():
    if os.path.exists(log_file_path):
        webbrowser.open(log_file_path)
    else:
        from tkinter import messagebox
        messagebox.showerror("Ошибка", "Файл с логами не найден!")


def open_failed_links_file():
    if os.path.exists(failed_links_path):
        webbrowser.open(failed_links_path)
    else:
        from tkinter import messagebox
        messagebox.showerror("Ошибка", "Файл с ошибками не найден!")


# ======================= Функции для работы с папкой загрузки =======================
def select_download_folder(download_folder_var):
    from tkinter import filedialog, messagebox

    folder = filedialog.askdirectory(initialdir=DOWNLOAD_FOLDER)
    if folder:
        download_folder_var.set(folder)
        config = load_config()
        config["download_folder"] = folder
        save_config(config)
        messagebox.showinfo("Папка загрузок", f"Выбрана папка: {folder}")


# ======================= Функции для работы с чёрным списком =======================

SECONDARY_BLACKLIST_FILE = "friends_video_blacklist.txt"
SECONDARY_BLACKLIST_REVIEW_FILE = "friends_video_not_in_blacklist.txt"


def _load_saved_cookies_into_session(session: requests.Session):
    if not os.path.exists(cookies_path):
        write_log("Cookies не найдены. Для friends-страниц сначала пройдите авторизацию и нажмите 'Проверить авторизацию'.", log_type="error")
        return

    try:
        with open(cookies_path, "rb") as f:
            cookies = pickle.load(f)
    except Exception as e:
        write_log(f"Не удалось прочитать cookies из {cookies_path}: {e}", log_type="error")
        return

    loaded_count = 0
    for cookie in cookies:
        name = cookie.get("name")
        value = cookie.get("value")
        if not name or value is None:
            continue

        domain = (cookie.get("domain") or "beautifulagony.com").lstrip(".")
        path = cookie.get("path") or "/"

        try:
            session.cookies.set(name, value, domain=domain, path=path)
            loaded_count += 1
        except Exception:
            try:
                session.cookies.set(name, value)
                loaded_count += 1
            except Exception:
                pass

    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://beautifulagony.com/",
    })

    write_log(f"Загружено cookies в requests.Session: {loaded_count}", log_type="info")


def _page_looks_like_login_or_guest(soup: BeautifulSoup) -> bool:
    text = soup.get_text(" ", strip=True).lower()
    if "page=login" in text:
        return True
    markers = ["login", "username", "userpass", "captcha"]
    return sum(1 for marker in markers if marker in text) >= 3


def _transform_video_filename_to_name(original_name: str) -> str:
    base_name, ext = os.path.splitext(original_name)
    match = re.match(r"^[A-Za-z](\d{4})[A-Za-z0-9]+-([A-Za-z]\d+)$", base_name)
    if not match:
        return original_name

    participant_number = match.group(1)
    video_part = match.group(2)
    return f"{participant_number}-{video_part}{ext}"


def _extract_four_digit_numbers_from_text(text: str):
    return set(re.findall(r"\b\d{4}\b", text or ""))


def _extract_artist_numbers_from_player_soup(soup: BeautifulSoup):
    numbers = set()

    for artist_block in soup.find_all("div", class_="artistinfo"):
        numbers.update(_extract_four_digit_numbers_from_text(artist_block.get_text(" ", strip=True)))

    if numbers:
        return numbers

    for heading in soup.find_all(["h1", "h2", "h3"]):
        heading_text = heading.get_text(" ", strip=True)
        if "Artist" in heading_text or "Artists" in heading_text:
            numbers.update(_extract_four_digit_numbers_from_text(heading_text))

    return numbers


def _normalize_artist_label(text: str) -> str:
    return " ".join((text or "").split())


def _extract_artist_labels_from_player_soup(soup: BeautifulSoup):
    labels = []
    seen = set()

    for artist_block in soup.find_all("div", class_="artistinfo"):
        text = _normalize_artist_label(artist_block.get_text(" ", strip=True))
        if text and text not in seen:
            seen.add(text)
            labels.append(text)

    if labels:
        return labels

    for heading in soup.find_all(["h1", "h2", "h3"]):
        heading_text = _normalize_artist_label(heading.get_text(" ", strip=True))
        if heading_text and ("Artist" in heading_text or "Artists" in heading_text) and heading_text not in seen:
            seen.add(heading_text)
            labels.append(heading_text)

    return labels


def _extract_player_urls_from_friends_soup(soup: BeautifulSoup, base_url: str):
    player_urls = []
    seen = set()

    for link in soup.find_all("a", href=True):
        href = (link.get("href") or "").strip()
        if "page=player" not in href or "media_id=" not in href:
            continue

        full_url = urljoin(base_url + "/", href)
        if full_url in seen:
            continue

        seen.add(full_url)
        player_urls.append(full_url)

    return player_urls


def _transform_secondary_blacklist_filename_to_name(original_name: str):
    base_name = os.path.splitext(os.path.basename(original_name or ""))[0]
    match = re.match(r"^[A-Za-z](\d{4})[A-Za-z0-9]*-([A-Za-z]\d+)$", base_name)
    if not match:
        return ""

    participant_number = match.group(1)
    video_part = match.group(2).upper()
    return f"{video_part}-{participant_number}"



def _extract_video_title_from_player_soup(soup: BeautifulSoup, player_url: str):
    video_source = soup.select_one("video#baVideoPlayer source[src]")
    if video_source:
        source_url = (video_source.get("src") or "").strip()
        if source_url:
            original_name = os.path.basename(source_url.split("?", 1)[0])
            transformed_name = _transform_secondary_blacklist_filename_to_name(original_name)
            if transformed_name:
                return transformed_name
            if original_name:
                return os.path.splitext(original_name)[0]

    download_link = soup.find("a", class_="download_links_href")
    if download_link and download_link.get("href"):
        original_name = os.path.basename(download_link["href"].split("?", 1)[0])
        transformed_name = _transform_secondary_blacklist_filename_to_name(original_name)
        if transformed_name:
            return transformed_name
        if original_name:
            return os.path.splitext(original_name)[0]

    selectors = [
        ("div", {"id": "playerinfo"}),
        ("div", {"class": "playerinfo"}),
        ("div", {"class": "title"}),
        ("h1", {}),
        ("h2", {}),
    ]

    for tag_name, attrs in selectors:
        elements = soup.find_all(tag_name, attrs=attrs) if attrs else soup.find_all(tag_name)
        for element in elements:
            text = element.get_text(" ", strip=True)
            if not text:
                continue
            if text.lower().startswith("artists "):
                continue
            if "download" in text.lower() and "mark as seen" in text.lower():
                continue
            return text

    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(" ", strip=True)

    return player_url


def create_secondary_blacklist_from_friends(
    main_blacklist_file="blacklist.txt",
    output_file=SECONDARY_BLACKLIST_FILE,
    review_output_file=SECONDARY_BLACKLIST_REVIEW_FILE,
    pause_event=None,
):
    from browser import begin_driver_session, end_driver_session, driver
    main_blacklist = load_blacklist(main_blacklist_file)
    if not main_blacklist:
        write_log(
            f"Основной чёрный список пуст или не найден: {main_blacklist_file}. Второй список будет пустым.",
            log_type="info",
        )

    if not begin_driver_session("secondary_blacklist", quiet=True):
        write_log("Не удалось получить доступ к открытому браузеру для создания второго чёрного списка.", log_type="error")
        return {"matched_titles": [], "review_entries": []}


    base_url = "https://beautifulagony.com"
    current_offset = 0
    processed_player_urls = set()
    matched_titles = []
    matched_titles_seen = set()
    review_entries = []
    review_entries_seen = set()

    try:
        while True:
            if pause_event is not None:
                pause_event.wait()

            page_url = f"{base_url}/public/main.php?page=view&mode=friends&offset={current_offset}"
            write_log(f"Второй чёрный список – загрузка страницы friends offset={current_offset}...", log_type="page")

            try:
                driver.get(page_url)
                soup = BeautifulSoup(driver.page_source, "html.parser")
            except Exception as e:
                write_log(f"Второй чёрный список – ошибка загрузки {page_url}: {e}", log_type="error")
                break

            if _page_looks_like_login_or_guest(soup):
                write_log(
                    "Второй чёрный список – friends-страница открылась как гостевая/логин-страница. "
                    "Сначала выполните авторизацию в открытом браузере.",
                    log_type="error",
                )
                break

            player_urls = _extract_player_urls_from_friends_soup(soup, base_url)
            if not player_urls:
                write_log(
                    f"Второй чёрный список – на странице friends offset={current_offset} ссылки на видео не найдены. Завершаем обход.",
                    log_type="info",
                )
                break

            write_log(
                f"Второй чёрный список – на странице friends offset={current_offset} найдено {len(player_urls)} ссылок на видео.",
                log_type="info",
            )

            page_matches = 0
            page_review = 0

            for player_url in player_urls:
                if pause_event is not None:
                    pause_event.wait()

                if player_url in processed_player_urls:
                    continue
                processed_player_urls.add(player_url)

                try:
                    driver.get(player_url)
                    player_soup = BeautifulSoup(driver.page_source, "html.parser")
                except Exception as e:
                    write_log(f"Второй чёрный список – ошибка загрузки страницы видео {player_url}: {e}", log_type="error")
                    continue

                if _page_looks_like_login_or_guest(player_soup):
                    write_log(
                        f"Второй чёрный список – страница видео требует авторизацию: {player_url}",
                        log_type="error",
                    )
                    continue

                artist_numbers = _extract_artist_numbers_from_player_soup(player_soup)
                artist_labels = _extract_artist_labels_from_player_soup(player_soup)
                if not artist_numbers and not artist_labels:
                    write_log(
                        f"Второй чёрный список – на странице видео не найдены данные участников: {player_url}",
                        log_type="info",
                    )
                    continue

                video_title = _extract_video_title_from_player_soup(player_soup, player_url).strip()
                if not video_title:
                    video_title = player_url

                matched_numbers = sorted(artist_numbers & main_blacklist)
                if matched_numbers:
                    if video_title not in matched_titles_seen:
                        matched_titles_seen.add(video_title)
                        matched_titles.append(video_title)
                        page_matches += 1

                    write_log(
                        f"Второй чёрный список – совпадение для '{video_title}': номера {', '.join(matched_numbers)}.",
                        log_type="info",
                    )
                    continue

                participants_text = "; ".join(artist_labels) if artist_labels else "Участники не определены"
                review_line = f"{video_title} | {participants_text}"
                if review_line not in review_entries_seen:
                    review_entries_seen.add(review_line)
                    review_entries.append(review_line)
                    page_review += 1

                write_log(
                    f"Второй чёрный список – без совпадений: '{video_title}' | {participants_text}.",
                    log_type="info",
                )

            write_log(
                f"Второй чёрный список – friends offset={current_offset}: совпадений на странице {page_matches}, без совпадений {page_review}, всего совпадений {len(matched_titles)}, всего без совпадений {len(review_entries)}.",
                log_type="info",
            )
            current_offset += 20

    finally:
        end_driver_session("secondary_blacklist")

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            for title in matched_titles:
                f.write(title + "\n")
        write_log(
            f"Второй чёрный список создан. Найдено видео: {len(matched_titles)}. Файл: {output_file}",
            log_type="info",
        )
    except Exception as e:
        write_log(f"Ошибка записи второго чёрного списка {output_file}: {e}", log_type="error")

    try:
        with open(review_output_file, "w", encoding="utf-8") as f:
            for line in review_entries:
                f.write(line + "\n")
        write_log(
            f"Список видео без попадания во второй чёрный список создан. Найдено видео: {len(review_entries)}. Файл: {review_output_file}",
            log_type="info",
        )
    except Exception as e:
        write_log(f"Ошибка записи файла {review_output_file}: {e}", log_type="error")

    return {"matched_titles": matched_titles, "review_entries": review_entries}

def create_blacklist_for_mode(mode):
    base_url = "https://beautifulagony.com/public/main.php?page=view&mode={}&offset={}"
    blacklist = set()
    page = 0

    while True:
        offset = page * 20
        url = base_url.format(mode, offset)

        try:
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
                        f"Режим {mode}: страница с offset={offset} не содержит номеров. Завершаем перебор.",
                        log_type="info"
                    )
                    break

                write_log(
                    f"Режим {mode}: найдено {len(page_numbers)} номеров на странице с offset={offset}.",
                    log_type="info"
                )
                blacklist.update(page_numbers)
                page += 1
            else:
                write_log(f"Не удалось загрузить страницу: {url}. Статус: {response.status_code}", log_type="error")
                break
        except Exception as e:
            write_log(f"Ошибка при обработке {url}: {e}", log_type="error")
            break

    return blacklist


def create_blacklist_from_pages(modes=None, output_file="blacklist.txt"):
    if modes is None:
        modes = ["males", "transgender"]

    total_blacklist = set()

    for mode in modes:
        write_log(f"Начало парсинга для режима: {mode}", log_type="info")
        mode_blacklist = create_blacklist_for_mode(mode)
        write_log(f"Режим {mode}: найдено {len(mode_blacklist)} номеров.", log_type="info")
        total_blacklist.update(mode_blacklist)

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            for num in sorted(total_blacklist):
                f.write(num + "\n")
        write_log(f"Черный список создан, найдено всего {len(total_blacklist)} номеров. Файл: {output_file}", log_type="info")
    except Exception as e:
        write_log(f"Ошибка при записи файла черного списка: {e}", log_type="error")

    return total_blacklist


def load_blacklist(filename="blacklist.txt"):
    blacklist = set()

    if not os.path.exists(filename):
        return blacklist

    try:
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                num = line.strip()
                if num:
                    blacklist.add(num)
    except Exception as e:
        write_log(f"Ошибка при загрузке файла черного списка: {e}", log_type="error")

    return blacklist


def open_blacklist_file():
    if os.path.exists("blacklist.txt"):
        webbrowser.open("blacklist.txt")
    else:
        from tkinter import messagebox
        messagebox.showerror("Ошибка", "Файл черного списка не найден!")


# ======================= Функции для работы с датами и метаданными =======================
def parse_release_date(date_text):
    try:
        dt = datetime.datetime.strptime(date_text, "%d %b %Y - %H:%M")
        return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
    except Exception as e:
        write_log(f"Ошибка парсинга даты: {e}", log_type="error")
        return None


def get_media_created(file_path):
    timestamp = os.path.getctime(file_path)
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(timestamp))


def get_data_modified(file_path):
    timestamp = os.path.getmtime(file_path)
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(timestamp))


def set_media_created(file_path, remote_date_str):
    try:
        remote_dt = parsedate_to_datetime(remote_date_str)
    except Exception as e:
        write_log(f"Ошибка при разборе даты '{remote_date_str}' для файла {file_path}: {e}", log_type="error")
        return False

    timestamp = remote_dt.timestamp()
    os.utime(file_path, (timestamp, timestamp))

    if os.name == "nt":
        FILE_WRITE_ATTRIBUTES = 0x100
        handle = ctypes.windll.kernel32.CreateFileW(file_path, FILE_WRITE_ATTRIBUTES, 0, None, 3, 0x80, None)
        if handle == -1:
            write_log(f"Не удалось открыть файл {file_path} для изменения даты создания.", log_type="error")
            return False

        win_time = int((timestamp + 11644473600) * 10000000)
        ctime = ctypes.c_longlong(win_time)
        res = ctypes.windll.kernel32.SetFileTime(handle, ctypes.byref(ctime), None, None)
        ctypes.windll.kernel32.CloseHandle(handle)

        if not res:
            write_log(f"Не удалось установить время создания файла {file_path}.", log_type="error")

        return res

    return True


def set_file_title(file_path, title):
    try:
        video = MP4(file_path)
        video["©nam"] = [title]
        video.save()
        return True
    except Exception as e:
        write_log(f"Ошибка при установке Title для {file_path}: {e}", log_type="error")
        return False


def set_video_id(file_path, person_id):
    return set_file_title(file_path, person_id)


def sizes_match(actual, expected, tolerance_percent=0.003):
    if expected <= 0:
        return False

    diff = abs(actual - expected)
    allowed = tolerance_percent * expected
    write_log(
        f"[DEBUG] Сравнение размеров: actual={actual}, expected={expected}, diff={diff}, allowed={allowed}",
        log_type="info"
    )
    return diff <= allowed


def get_media_created_exiftool(file_path):
    exiftool_path = r"C:\Portable\Exiftool\exiftool.exe"
    file_path = os.path.normpath(file_path)
    command = [exiftool_path, "-s", "-s", "-s", "-MediaCreateDate", file_path]

    try:
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        result = subprocess.run(command, capture_output=True, text=True, creationflags=creationflags)

        if result.returncode == 0:
            media_date_str = result.stdout.strip()
            if media_date_str:
                try:
                    media_dt = datetime.datetime.strptime(media_date_str, "%Y:%m:%d %H:%M:%S")
                    return media_dt.timestamp()
                except Exception as parse_ex:
                    write_log(f"Ошибка парсинга даты из exiftool: {parse_ex}", log_type="error")
                    return None

            write_log("Exiftool вернул пустую строку для MediaCreateDate", log_type="error")
            return None

        write_log(f"Exiftool: ошибка извлечения MediaCreateDate: {result.stderr}", log_type="error")
        return None

    except Exception as e:
        write_log(f"Exiftool: исключение при извлечении MediaCreateDate: {e}", log_type="error")
        return None


def update_mp4_internal_dates(file_path, new_date):
    file_path = os.path.normpath(file_path)
    exiftool_path = r"C:\Portable\Exiftool\exiftool.exe"

    command = [
        exiftool_path,
        "-overwrite_original",
        f"-CreateDate={new_date}",
        f"-ModifyDate={new_date}",
        f"-MediaCreateDate={new_date}",
        file_path
    ]

    try:
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        result = subprocess.run(command, capture_output=True, text=True, creationflags=creationflags)

        if result.returncode != 0:
            write_log(f"Exiftool: ошибка обновления метаданных для '{file_path}': {result.stderr}", log_type="error")
    except Exception as e:
        write_log(f"Exiftool: исключение при обновлении метаданных для '{file_path}': {e}", log_type="error")


def extract_page_release_date(driver, media_id):
    """
    Извлекает дату релиза со страницы плеера и трактует её
    как локальное время текущей системы.
    """
    import datetime

    if not media_id:
        return None

    try:
        blocks = driver.find_elements(
            By.CSS_SELECTOR,
            ".playerthumb_hd, .playerthumb"
        )
    except Exception:
        return None

    target_pattern = f"change_vid('{media_id}'"

    # Локальный timezone текущей системы
    local_tz = datetime.datetime.now().astimezone().tzinfo

    for block in blocks:
        try:
            a_elem = block.find_element(By.TAG_NAME, "a")
            href = (a_elem.get_attribute("href") or "").strip()

            if target_pattern not in href:
                continue

            date_div = block.find_element(By.CLASS_NAME, "playerthumb_release_txt")
            date_text = (date_div.text or "").strip()
            if not date_text:
                continue

            dt = datetime.datetime.strptime(date_text, "%d %b %Y - %H:%M")
            dt += datetime.timedelta(hours=5)
            dt = dt.replace(tzinfo=local_tz)
            return dt.timestamp()

        except Exception:
            continue

    return None


def _timestamps_match(actual_ts, expected_ts, tolerance_seconds=DATE_SYNC_TOLERANCE_SECONDS):
    if actual_ts is None or expected_ts is None:
        return False
    return abs(float(actual_ts) - float(expected_ts)) <= float(tolerance_seconds)


def _set_filesystem_dates(file_path, target_ts):
    os.utime(file_path, (target_ts, target_ts))

    if os.name == "nt":
        from ctypes import wintypes
        kernel32 = ctypes.windll.kernel32

        class FILETIME(ctypes.Structure):
            _fields_ = [
                ("dwLowDateTime", wintypes.DWORD),
                ("dwHighDateTime", wintypes.DWORD)
            ]

        def unix_to_filetime(t):
            ft = int((t + 11644473600) * 10000000)
            low = ft & 0xFFFFFFFF
            high = ft >> 32
            return FILETIME(low, high)

        ft_struct = unix_to_filetime(target_ts)
        FILE_WRITE_ATTRIBUTES = 0x100
        handle = kernel32.CreateFileW(file_path, FILE_WRITE_ATTRIBUTES, 0, None, 3, 0x80, None)

        if handle not in (-1, 0):
            res = kernel32.SetFileTime(
                handle,
                ctypes.byref(ft_struct),
                ctypes.byref(ft_struct),
                ctypes.byref(ft_struct)
            )
            kernel32.CloseHandle(handle)

            if not res:
                write_log("Ошибка при установке времени через Windows API", log_type="error")


def _read_current_file_dates(file_path):
    creation_time = os.path.getctime(file_path)
    modification_time = os.path.getmtime(file_path)
    media_time = get_media_created_exiftool(file_path)
    return creation_time, modification_time, media_time


def _log_date_debug(file_path, page_release_ts, creation_time, modification_time, media_time, creation_matches, modification_matches, media_matches):
    write_log(f"[DEBUG] file={file_path}", log_type="info")
    write_log(
        f"[DEBUG] page_release_ts={page_release_ts} -> {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(page_release_ts))}",
        log_type="info")
    write_log(
        f"[DEBUG] creation_time={creation_time} -> {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(creation_time))}",
        log_type="info")
    write_log(
        f"[DEBUG] modification_time={modification_time} -> {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modification_time))}",
        log_type="info")
    write_log(
        f"[DEBUG] media_time={media_time} -> {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(media_time)) if media_time else 'None'}",
        log_type="info")
    write_log(f"[DEBUG] creation_matches={creation_matches}", log_type="info")
    write_log(f"[DEBUG] modification_matches={modification_matches}", log_type="info")
    write_log(f"[DEBUG] media_matches={media_matches}", log_type="info")


def _build_match_state(file_path, page_release_ts):
    creation_time, modification_time, media_time = _read_current_file_dates(file_path)
    creation_matches = _timestamps_match(creation_time, page_release_ts)
    modification_matches = _timestamps_match(modification_time, page_release_ts)
    media_matches = True if media_time is None else _timestamps_match(media_time, page_release_ts)
    return {
        "creation_time": creation_time,
        "modification_time": modification_time,
        "media_time": media_time,
        "creation_matches": creation_matches,
        "modification_matches": modification_matches,
        "media_matches": media_matches,
    }


def synchronize_file_dates(file_path, page_release_ts=None):
    try:
        if page_release_ts is None:
            write_log(f"Дата страницы не передана для '{file_path}', даты не изменяются.", log_type="info")
            return

        state = _build_match_state(file_path, page_release_ts)

        if state["media_time"] is None:
            write_log(
                f"MediaCreateDate не найден для '{file_path}'. Будет выполнена принудительная запись MP4-метаданных.",
                log_type="info"
            )

        if state["creation_matches"] and state["modification_matches"] and state["media_matches"]:
            write_log(
                f"Даты уже совпадают с датой страницы для '{file_path}', синхронизация не требуется.",
                log_type="info"
            )
            return

        page_key = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(page_release_ts))
        write_log(
            f"Обновление дат для '{file_path}' до даты страницы {page_key}.",
            log_type="info"
        )

        new_date_str_exif = time.strftime("%Y:%m:%d %H:%M:%S", time.localtime(page_release_ts))

        for attempt in range(1, 4):
            _set_filesystem_dates(file_path, page_release_ts)
            update_mp4_internal_dates(file_path, new_date_str_exif)
            _set_filesystem_dates(file_path, page_release_ts)
            time.sleep(0.4)

            state = _build_match_state(file_path, page_release_ts)
            _log_date_debug(
                file_path,
                page_release_ts,
                state["creation_time"],
                state["modification_time"],
                state["media_time"],
                state["creation_matches"],
                state["modification_matches"],
                state["media_matches"],
            )

            if state["creation_matches"] and state["modification_matches"] and state["media_matches"]:
                write_log(
                    f"Синхронизация дат для '{file_path}' завершена успешно (попытка {attempt}/3).",
                    log_type="info"
                )
                return

            write_log(
                f"Даты для '{file_path}' после попытки {attempt}/3 ещё не полностью совпали, повторяем.",
                log_type="info"
            )

        write_log(
            f"После 3 попыток даты для '{file_path}' всё ещё не полностью синхронизированы.",
            log_type="error"
        )

    except Exception as e:
        write_log(f"Ошибка синхронизации дат для '{file_path}': {e}", log_type="error")


# ======================= Функции сохранения дат видео =======================
VIDEO_DATE_RECORDS_FILE = "video_date_records.json"


def load_video_date_records(records_file=VIDEO_DATE_RECORDS_FILE):
    if not os.path.exists(records_file):
        return {}

    with _video_date_records_lock:
        try:
            with open(records_file, "r", encoding="utf-8") as f:
                raw = f.read()

            if not raw.strip():
                return {}

            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError as e:
            write_log(f"Файл дат видео '{records_file}' повреждён или пуст. Будет использован пустой словарь: {e}", log_type="error")
            return {}
        except Exception as e:
            write_log(f"Ошибка загрузки файла дат видео '{records_file}': {e}", log_type="error")
            return {}


def save_video_date_record(video_name, release_ts, records_file=VIDEO_DATE_RECORDS_FILE):
    """
    Сохраняет timestamp даты релиза для конкретного видео в JSON-файл.
    Нужна downloader.py для пометки успешно обработанных файлов.
    """
    video_name = (video_name or "").strip()
    if not video_name:
        return False

    with _video_date_records_lock:
        try:
            records = {}
            if os.path.exists(records_file):
                with open(records_file, "r", encoding="utf-8") as f:
                    raw = f.read()
                if raw.strip():
                    loaded = json.loads(raw)
                    if isinstance(loaded, dict):
                        records = loaded

            records[video_name] = float(release_ts) if release_ts is not None else None

            temp_file = records_file + ".tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            os.replace(temp_file, records_file)
            return True
        except json.JSONDecodeError as e:
            write_log(
                f"Ошибка чтения файла дат видео '{records_file}' перед сохранением '{video_name}': {e}",
                log_type="error"
            )
            return False
        except Exception as e:
            write_log(
                f"Ошибка сохранения даты видео '{video_name}' в файл '{records_file}': {e}",
                log_type="error"
            )
            return False