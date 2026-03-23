import json
import os
import pickle
import queue
import re
import threading
import tkinter as tk
import time
from typing import Any
from urllib.parse import parse_qs, urlparse
from typing import Callable, Union
import customtkinter as ctk
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from browser import begin_driver_session, end_driver_session
from utils import (
    extract_page_release_date,
    load_blacklist,
    run_on_ui_thread,
    save_failed_link,
    save_video_date_record,
    sizes_match,
    write_log,
)

DOWNLOAD_STATUS_DOWNLOADED = "downloaded"
DOWNLOAD_STATUS_SKIPPED = "skipped"
DOWNLOAD_STATUS_FAILED = "failed"

VIDEO_LINKS_FILE = "video_links.txt"
SUCCESSFUL_DOWNLOADS_FILE = "successful_downloads.txt"
CHUNK_SIZE = 1024 * 256
URL_REFRESH_MARGIN_SECONDS = 90
MAX_REFRESH_RETRIES = 3
PREFETCH_LIMIT = 30
MIN_LEAD_LIMIT = 1
QUEUE_GET_TIMEOUT_SECONDS = 0.5
DRIVER_ACQUIRE_RETRY_SECONDS = 0.5

is_downloading_video = False
current_video_url = None
current_video_name = None

is_collecting_links = False
stop_downloading_flag = False
stop_collecting_flag = False

_links_file_lock = threading.Lock()
_driver_refresh_lock = threading.Lock()
_path_locks_guard = threading.Lock()
_path_locks: dict[str, threading.Lock] = {}
_successful_downloads_lock = threading.Lock()

_download_counters_lock = threading.Lock()


def _resolve_lead_limit(lead_limit) -> int:
    try:
        value = lead_limit() if callable(lead_limit) else lead_limit
    except Exception:
        value = PREFETCH_LIMIT
    try:
        value = int(value)
    except Exception:
        value = PREFETCH_LIMIT
    return max(MIN_LEAD_LIMIT, value)


def _log_collected_links_count(total_count: int):
    write_log(f"Собрано ссылок: {total_count}", log_type="info")


def _log_downloaded_videos_count(downloaded_count: int):
    write_log(f"Скачано видео: {downloaded_count}", log_type="info")


def _get_path_lock(video_name: str) -> threading.Lock:
    with _path_locks_guard:
        lock = _path_locks.get(video_name)
        if lock is None:
            lock = threading.Lock()
            _path_locks[video_name] = lock
        return lock


def _remember_successful_download(video_name: str) -> None:
    video_name = (video_name or "").strip()
    if not video_name:
        return

    with _successful_downloads_lock:
        existing_names = set()

        if os.path.exists(SUCCESSFUL_DOWNLOADS_FILE):
            with open(SUCCESSFUL_DOWNLOADS_FILE, "r", encoding="utf-8") as f:
                existing_names = {line.strip() for line in f if line.strip()}

        if video_name in existing_names:
            return

        with open(SUCCESSFUL_DOWNLOADS_FILE, "a", encoding="utf-8") as f:
            f.write(video_name + "\n")


def _mark_existing_file_as_success(output_path: str, release_ts: float | None, video_name: str) -> None:
    save_video_date_record(video_name, release_ts)
    _remember_successful_download(video_name)


def request_stop_after_current():
    global stop_downloading_flag, stop_collecting_flag
    stop_downloading_flag = True
    stop_collecting_flag = True
    write_log(
        "Запрошена остановка: новые задачи выдаваться не будут, активные загрузки завершатся.",
        log_type="info"
    )


def request_stop_collecting():
    global stop_collecting_flag
    stop_collecting_flag = True
    write_log("Запрошено завершение сбора ссылок после текущей страницы.", log_type="info")


def transform_video_name(original_name: str) -> str:
    base_name, ext = os.path.splitext(original_name)
    match = re.match(r"^[A-Za-z](\d{4})[A-Za-z0-9]+-([A-Za-z]\d+)$", base_name)
    if not match:
        return original_name

    participant_number = match.group(1)
    video_part = match.group(2)
    return f"{participant_number}-{video_part}{ext}"


def _format_bytes(size: int | float) -> str:
    size = float(max(size, 0))
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024
        idx += 1
    return f"{size:.2f} {units[idx]}"


def _load_cookies_into_driver(driver):
    from utils import cookies_path

    if not os.path.exists(cookies_path):
        return

    with open(cookies_path, "rb") as file:
        cookies = pickle.load(file)

    for cookie in cookies:
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass

    try:
        driver.refresh()
    except Exception:
        pass


def _requests_head(url: str, timeout: int = 20) -> requests.Response | None:
    try:
        return requests.head(url, allow_redirects=True, timeout=timeout)
    except Exception as e:
        write_log(f"HEAD-запрос завершился ошибкой для ссылки {url}: {e}", log_type="error")
        return None


def _head_size(url: str) -> int:
    response = _requests_head(url)
    if response is None:
        return 0
    try:
        return int(response.headers.get("Content-Length", 0))
    except Exception:
        return 0


def _parse_expires_from_url(url: str) -> int | None:
    try:
        parsed = urlparse(url)
        expires = parse_qs(parsed.query).get("expires", [None])[0]
        if expires is None:
            return None
        return int(expires)
    except Exception:
        return None


def _is_url_expired_or_near_expiry(url: str, margin_seconds: int = URL_REFRESH_MARGIN_SECONDS) -> bool:
    expires_ts = _parse_expires_from_url(url)
    if expires_ts is None:
        return False
    return time.time() >= (expires_ts - margin_seconds)


def _normalize_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "page_url": record.get("page_url") or record.get("video_link") or "",
        "direct_url": record.get("direct_url") or record.get("largest_video_url") or "",
        "video_name": record.get("video_name") or "",
        "original_video_name": record.get("original_video_name") or "",
        "size_bytes": int(record.get("size_bytes") or record.get("largest_video_size") or 0),
        "media_id": str(record.get("media_id") or "") or None,
        "release_ts": record.get("release_ts"),
        "real_number": record.get("real_number") or "",
        "collected_at": int(record.get("collected_at") or time.time()),
    }

    if normalized["release_ts"] is not None:
        try:
            normalized["release_ts"] = float(normalized["release_ts"])
        except Exception:
            normalized["release_ts"] = None

    if not normalized["video_name"] and normalized["direct_url"]:
        original_name = normalized["direct_url"].split("/")[-1].split("?")[0]
        normalized["original_video_name"] = original_name
        normalized["video_name"] = transform_video_name(original_name)

    return normalized


def _parse_record_line(line: str) -> dict[str, Any] | None:
    line = line.strip()
    if not line:
        return None

    if line.startswith("{"):
        try:
            data = json.loads(line)
            return _normalize_record(data)
        except Exception as e:
            write_log(f"Не удалось разобрать JSON-строку из video_links.txt: {e}", log_type="error")
            return None

    return _normalize_record({"page_url": line})


def _load_link_records() -> list[dict[str, Any]]:
    if not os.path.exists(VIDEO_LINKS_FILE):
        return []

    records: list[dict[str, Any]] = []
    with open(VIDEO_LINKS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            record = _parse_record_line(line)
            if record is not None:
                records.append(record)
    return records


def _write_link_records(records: list[dict[str, Any]]) -> None:
    with _links_file_lock:
        with open(VIDEO_LINKS_FILE, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _upsert_record(record: dict[str, Any]) -> None:
    record = _normalize_record(record)

    with _links_file_lock:
        records = _load_link_records()
        replaced = False

        for idx, existing in enumerate(records):
            if existing.get("page_url") == record["page_url"]:
                records[idx] = record
                replaced = True
                break

        if not replaced:
            records.append(record)

        with open(VIDEO_LINKS_FILE, "w", encoding="utf-8") as f:
            for item in records:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")


def _extract_listing_candidates(driver) -> list[dict[str, str]]:
    page_links = driver.find_elements(
        By.XPATH,
        '//a[contains(@href, "page=player&out=bkg&media")]'
    )

    candidates: list[dict[str, str]] = []

    for link_element in page_links:
        page_url = (link_element.get_attribute("href") or "").strip()
        if not page_url:
            continue

        real_number = ""
        try:
            img_elem = link_element.find_element(By.TAG_NAME, "img")
            alt_text = (img_elem.get_attribute("alt") or "").strip()
            real_number = alt_text.lstrip("#")
        except Exception:
            pass

        candidates.append({
            "page_url": page_url,
            "real_number": real_number,
        })

    return candidates


def _collect_direct_record_from_player_page(driver, page_url: str, blacklist: set[str], real_number: str = "") -> dict[str, Any] | None:
    driver.get(page_url)
    _load_cookies_into_driver(driver)

    video_elements = WebDriverWait(driver, 12).until(
        EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@id="playerinfo"]//a[@class="download_links_href"]')
        )
    )

    video_options: list[tuple[str, int]] = []
    for video_element in video_elements:
        vid_url = (video_element.get_attribute("href") or "").strip()
        if not vid_url:
            continue

        size_bytes = _head_size(vid_url)
        video_options.append((vid_url, size_bytes))

    if not video_options:
        write_log(f"Не найдено доступных версий видео для ссылки: {page_url}", log_type="error")
        return None

    largest_video_url, largest_video_size = max(video_options, key=lambda x: x[1])
    original_video_name = largest_video_url.split("/")[-1].split("?")[0]
    video_name = transform_video_name(original_video_name)

    for num in blacklist:
        if num and num in video_name:
            write_log(f"Пропуск {video_name}: имя содержит число {num} из черного списка.", log_type="info")
            return None

    parsed = urlparse(page_url)
    media_id = parse_qs(parsed.query).get("media_id", [None])[0]
    release_ts = extract_page_release_date(driver, media_id) if media_id else None

    record = {
        "page_url": page_url,
        "direct_url": largest_video_url,
        "video_name": video_name,
        "original_video_name": original_video_name,
        "size_bytes": int(largest_video_size),
        "media_id": media_id,
        "release_ts": release_ts,
        "real_number": real_number,
        "collected_at": int(time.time()),
    }
    return _normalize_record(record)


def _acquire_driver_session(task_name: str, allow_stop: bool = True) -> bool:
    while True:
        if begin_driver_session(task_name, quiet=True):
            return True
        if allow_stop and stop_collecting_flag:
            return False
        time.sleep(DRIVER_ACQUIRE_RETRY_SECONDS)


def _load_listing_candidates(current_url: str) -> list[dict[str, str]]:
    if not _acquire_driver_session("collect_links_page"):
        return []

    try:
        from browser import driver
        driver.get(current_url)
        return _extract_listing_candidates(driver)
    finally:
        end_driver_session("collect_links_page")


def _collect_record_from_candidate(candidate: dict[str, str], blacklist: set[str]) -> dict[str, Any] | None:
    page_url = candidate["page_url"]
    real_number = candidate.get("real_number", "")

    if not _acquire_driver_session("collect_links_player"):
        return None

    try:
        from browser import driver
        return _collect_direct_record_from_player_page(driver, page_url, blacklist, real_number)
    finally:
        end_driver_session("collect_links_player")


def collect_video_links(
    start_url,
    search_pause_event,
    stop_on_empty_pages: bool = False,
    output_queue: queue.Queue | None = None,
    start_download_event: threading.Event | None = None,
    collector_done_event: threading.Event | None = None,
    prefetch_limit: int | Callable = PREFETCH_LIMIT,
    lead_state: dict[str, Any] | None = None,
):
    global is_collecting_links, stop_collecting_flag

    if is_collecting_links:
        write_log("Сбор ссылок уже запущен!", log_type="info")
        if collector_done_event is not None:
            collector_done_event.set()
        if start_download_event is not None:
            start_download_event.set()
        return []

    if output_queue is None and not begin_driver_session("collect_links", quiet=True):
        if collector_done_event is not None:
            collector_done_event.set()
        if start_download_event is not None:
            start_download_event.set()
        return []

    is_collecting_links = True
    stop_collecting_flag = False

    try:
        existing_records = _load_link_records()
        existing_page_urls = {item.get("page_url") for item in existing_records if item.get("page_url")}
        seen_video_names = {
            (item.get("video_name") or "").strip()
            for item in existing_records
            if (item.get("video_name") or "").strip()
        }

        parsed_url = urlparse(start_url)
        query_params = parse_qs(parsed_url.query)
        current_offset = int(query_params.get("offset", [0])[0])
        mode = query_params.get("mode", ["latest"])[0]

        base_url = f"https://beautifulagony.com/public/main.php?page=view&mode={mode}&offset={{}}"
        current_url = base_url.format(current_offset)
        empty_page_count = 0
        blacklist = load_blacklist("blacklist.txt")

        while True:
            if stop_collecting_flag:
                write_log("Сбор ссылок остановлен по запросу пользователя.", log_type="info")
                break

            search_pause_event.wait()
            write_log(f"Сбор прямых ссылок, страница: {current_url}", log_type="page")

            if output_queue is None:
                from browser import driver
                driver.get(current_url)
                candidates = _extract_listing_candidates(driver)
            else:
                candidates = _load_listing_candidates(current_url)

            if not candidates:
                write_log("На странице не найдено карточек видео, завершаем сбор.", log_type="info")
                break

            page_new_records: list[dict[str, Any]] = []

            for candidate in candidates:
                if stop_collecting_flag:
                    write_log("Остановка сбора ссылок после текущей страницы.", log_type="info")
                    break

                search_pause_event.wait()

                page_url = candidate["page_url"]
                real_number = candidate["real_number"]

                if page_url in existing_page_urls:
                    write_log(f"Ссылка проигнорирована (уже существует): {page_url}", log_type="info")
                    continue

                if real_number and real_number in blacklist:
                    write_log(
                        f"Ссылка проигнорирована (чёрный список – real_number={real_number}): {page_url}",
                        log_type="info"
                    )
                    continue

                try:
                    if output_queue is None:
                        from browser import driver
                        record = _collect_direct_record_from_player_page(driver, page_url, blacklist, real_number)
                    else:
                        record = _collect_record_from_candidate(candidate, blacklist)
                except Exception as e:
                    write_log(f"Ошибка при подготовке прямой ссылки {page_url}: {e}", log_type="error")
                    save_failed_link(page_url)
                    continue

                if record is None:
                    continue

                video_name = (record.get("video_name") or "").strip()
                if video_name and video_name in seen_video_names:
                    write_log(f"Дубликат по имени файла пропущен при сборе: {video_name}", log_type="info")
                    continue

                existing_records.append(record)
                existing_page_urls.add(page_url)
                if video_name:
                    seen_video_names.add(video_name)
                page_new_records.append(record)

                write_log(
                    f"Добавлена запись: {record['video_name']} | {_format_bytes(record['size_bytes'])} | {record['direct_url']}",
                    log_type="info"
                )
                _log_collected_links_count(len(existing_records))

                if output_queue is not None:
                    while True:
                        if stop_collecting_flag:
                            break
                        try:
                            if lead_state is not None:
                                while True:
                                    if stop_collecting_flag:
                                        break
                                    with lead_state["condition"]:
                                        current_limit = _resolve_lead_limit(prefetch_limit)
                                        lead_state["limit"] = current_limit
                                        lead_state["update_ui"]()
                                        if lead_state["outstanding"] < current_limit:
                                            break
                                        lead_state["condition"].wait(timeout=QUEUE_GET_TIMEOUT_SECONDS)
                                if stop_collecting_flag:
                                    break

                            output_queue.put(record, timeout=QUEUE_GET_TIMEOUT_SECONDS)

                            if lead_state is not None:
                                with lead_state["condition"]:
                                    lead_state["outstanding"] += 1
                                    lead_state["produced"] += 1
                                    lead_state["limit"] = _resolve_lead_limit(prefetch_limit)
                                    lead_state["update_ui"]()
                                    lead_state["condition"].notify_all()

                            if start_download_event is not None:
                                start_download_event.set()
                            break
                        except queue.Full:
                            continue

                    if stop_collecting_flag:
                        break

            if page_new_records:
                _write_link_records(existing_records)

            if stop_collecting_flag:
                write_log("Сбор ссылок завершён по запросу пользователя.", log_type="info")
                break

            empty_page_count = empty_page_count + 1 if not page_new_records else 0
            if stop_on_empty_pages and empty_page_count >= 3:
                write_log("Остановка поиска: 3 страницы подряд без новых записей.", log_type="info")
                break

            current_offset += 20
            current_url = base_url.format(current_offset)

        write_log(f"Сбор ссылок завершён, записей в файле: {len(existing_records)}.", log_type="info")
        return existing_records

    except Exception as e:
        write_log(f"Ошибка при сборе ссылок: {e}", log_type="error")
        return None

    finally:
        if output_queue is None:
            end_driver_session("collect_links")

        if start_download_event is not None:
            start_download_event.set()
        if collector_done_event is not None:
            collector_done_event.set()

        stop_collecting_flag = False
        _emit_lead_status()
        is_collecting_links = False


class ProgressSlotUI:
    def __init__(self, parent, slot_index: int):
        self.parent = parent
        self.slot_index = slot_index
        self.frame = None
        self.title_label = None
        self.progress_bar = None
        self.status_label = None
        self._created_event = threading.Event()
        self._destroyed = False
        self._state_lock = threading.Lock()
        self._last_enqueued_at = 0.0

        run_on_ui_thread(self._create_widgets)
        self._created_event.wait(timeout=5)

    def _create_widgets(self):
        self.frame = ctk.CTkFrame(self.parent, corner_radius=10, border_width=1)
        self.frame.grid_columnconfigure(0, weight=1)

        self.title_label = ctk.CTkLabel(
            self.frame,
            text=f"Поток {self.slot_index + 1}: ожидание",
            anchor="w",
            font=ctk.CTkFont(size=12, weight="bold")
        )
        self.title_label.grid(row=0, column=0, sticky="ew", padx=8, pady=(4, 1))

        self.progress_bar = ctk.CTkProgressBar(self.frame, height=8)
        self.progress_bar.set(0)
        self.progress_bar.grid(row=1, column=0, sticky="ew", padx=8, pady=1)

        self.status_label = ctk.CTkLabel(
            self.frame,
            text="",
            anchor="w",
            justify="left",
            font=ctk.CTkFont(size=10)
        )
        self.status_label.grid(row=2, column=0, sticky="ew", padx=8, pady=(1, 4))

        self._created_event.set()

    def grid(self, row: int):
        def _grid():
            if self.frame is not None and not self._destroyed:
                self.frame.grid(row=row, column=0, sticky="nsew", padx=3, pady=2)
        run_on_ui_thread(_grid)

    def _widget_alive(self, widget) -> bool:
        return widget is not None and bool(getattr(widget, "winfo_exists", lambda: False)())

    def _set_state(self, title: str, progress_percent: int, status: str):
        if self._destroyed:
            return

        try:
            if self._widget_alive(self.title_label):
                self.title_label.configure(text=title)
            if self._widget_alive(self.progress_bar):
                self.progress_bar.set(max(0.0, min(1.0, progress_percent / 100.0)))
            if self._widget_alive(self.status_label):
                self.status_label.configure(text=status)
        except (tk.TclError, RuntimeError) as e:
            self._destroyed = True
            write_log(f"UI-слот потока {self.slot_index + 1} больше недоступен: {e}", log_type="error")
        except Exception as e:
            write_log(f"Ошибка обновления UI-слота потока {self.slot_index + 1}: {e}", log_type="error")

    def set_state(self, title: str, progress_percent: int, status: str, force: bool = False):
        now = time.monotonic()
        with self._state_lock:
            if not force and (now - self._last_enqueued_at) < 0.10 and progress_percent not in (0, 100):
                return
            self._last_enqueued_at = now
        run_on_ui_thread(self._set_state, title, progress_percent, status)

    def set_idle(self):
        self.set_state(f"Поток {self.slot_index + 1}: ожидание", 0, "", force=True)

    def destroy(self):
        def _destroy():
            self._destroyed = True
            if self.frame is not None:
                try:
                    self.frame.destroy()
                except Exception:
                    pass
                self.frame = None
        run_on_ui_thread(_destroy)


class ProgressPanelUI:
    def __init__(self, parent, worker_count: int):
        self.parent = parent
        self.worker_count = worker_count
        self.container = None
        self.slots_host = None
        self.slots: list[ProgressSlotUI] = []
        self._created_event = threading.Event()

        run_on_ui_thread(self._create_widgets)
        self._created_event.wait(timeout=5)
        self.build_slots()

    def _create_widgets(self):
        self.container = ctk.CTkFrame(self.parent, fg_color="transparent")
        self.container.grid(row=0, column=0, sticky="nsew")
        self.container.grid_rowconfigure(0, weight=1)
        self.container.grid_columnconfigure(0, weight=1)

        self.slots_host = ctk.CTkFrame(self.container, fg_color="transparent")
        self.slots_host.grid(row=0, column=0, sticky="nsew")
        self.slots_host.grid_columnconfigure(0, weight=1)

        self._created_event.set()

    def build_slots(self):
        if self.slots_host is None or self.slots:
            return

        for idx in range(self.worker_count):
            self.slots_host.grid_rowconfigure(idx, weight=1, uniform="progress_slots")
            slot = ProgressSlotUI(self.slots_host, idx)
            slot.grid(idx)
            self.slots.append(slot)

    def destroy(self):
        for slot in self.slots:
            slot.destroy()
        self.slots = []

        def _destroy():
            if self.container is not None:
                try:
                    self.container.destroy()
                except Exception:
                    pass
                self.container = None
        run_on_ui_thread(_destroy)


def _get_worker_count() -> int:
    cpu_count = os.cpu_count() or 4
    return max(2, min(8, cpu_count))


def get_worker_count() -> int:
    return _get_worker_count()


def _ensure_progress_slots(progress_panel: ProgressPanelUI, worker_count: int) -> int:
    """
    Гарантирует, что в progress_panel есть хотя бы worker_count слотов.
    Возвращает фактическое число доступных слотов/потоков.
    """
    try:
        progress_panel.worker_count = max(int(worker_count), 1)
    except Exception:
        worker_count = max(int(worker_count), 1)

    try:
        progress_panel.build_slots()
    except Exception as e:
        write_log(f"Не удалось построить панели прогресса: {e}", log_type="error")

    available = len(getattr(progress_panel, "slots", []) or [])
    if available <= 0:
        write_log("Панель прогресса не создала ни одного слота. Используем один поток как безопасный режим.", log_type="error")
        return 1

    if available < worker_count:
        write_log(
            f"Количество UI-слотов прогресса меньше числа потоков ({available} < {worker_count}). "
            f"Число потоков снижено до {available}.",
            log_type="info"
        )

    return min(worker_count, available)


def _direct_url_looks_usable(record: dict[str, Any]) -> bool:
    direct_url = record.get("direct_url") or ""
    if not direct_url:
        return False

    if _is_url_expired_or_near_expiry(direct_url):
        return False

    expires_ts = _parse_expires_from_url(direct_url)
    if expires_ts is not None and (expires_ts - time.time()) > 300:
        return True

    response = _requests_head(direct_url)
    if response is None:
        return False

    if response.status_code not in (200, 206):
        return False

    content_type = (response.headers.get("Content-Type") or "").lower()
    if content_type and "video" not in content_type and "octet-stream" not in content_type:
        return False

    content_length = int(response.headers.get("Content-Length", 0) or 0)
    expected_size = int(record.get("size_bytes") or 0)

    if expected_size > 0 and content_length > 0 and not sizes_match(content_length, expected_size, tolerance_percent=0.003):
        return False

    return True


def _refresh_record_from_page(record: dict[str, Any], blacklist: set[str]) -> dict[str, Any] | None:
    page_url = record.get("page_url")
    if not page_url:
        write_log("Невозможно обновить прямую ссылку: в записи нет page_url.", log_type="error")
        return None

    with _driver_refresh_lock:
        acquired = _acquire_driver_session("refresh_direct_link", allow_stop=False)
        if not acquired:
            write_log(f"Не удалось захватить Selenium для обновления ссылки: {page_url}", log_type="error")
            return None

        try:
            from browser import driver
            refreshed = _collect_direct_record_from_player_page(
                driver,
                page_url,
                blacklist,
                record.get("real_number", "")
            )
            if refreshed is None:
                return None

            if refreshed.get("release_ts") is None:
                refreshed["release_ts"] = record.get("release_ts")

            _upsert_record(refreshed)
            write_log(f"Прямая ссылка обновлена: {refreshed['video_name']}", log_type="info")
            return refreshed
        except Exception as e:
            write_log(f"Ошибка обновления прямой ссылки для {page_url}: {e}", log_type="error")
            return None
        finally:
            end_driver_session("refresh_direct_link")


def _ensure_fresh_record(record: dict[str, Any], blacklist: set[str]) -> dict[str, Any] | None:
    record = _normalize_record(record)

    if _direct_url_looks_usable(record):
        return record

    write_log(
        f"Прямая ссылка отсутствует или устарела для {record.get('page_url')}. Пробуем обновить через страницу видео.",
        log_type="info"
    )
    return _refresh_record_from_page(record, blacklist)


def _final_output_path(output_folder: str, video_name: str) -> str:
    os.makedirs(output_folder, exist_ok=True)
    return os.path.join(output_folder, video_name)


def _temp_output_path(output_path: str) -> str:
    return output_path + ".part"


def _part_file_is_complete(temp_path: str, expected_size: int) -> bool:
    if expected_size <= 0 or not os.path.exists(temp_path):
        return False

    actual_size = os.path.getsize(temp_path)
    return sizes_match(actual_size, expected_size, tolerance_percent=0.003)


def _finalize_completed_download(temp_path: str, output_path: str, expected_size: int, release_ts: float | None, video_name: str) -> bool:
    if not _part_file_is_complete(temp_path, expected_size):
        return False

    last_error = None

    for attempt in range(1, 6):
        try:
            if os.path.exists(output_path):
                try:
                    os.remove(output_path)
                except PermissionError:
                    pass

            os.replace(temp_path, output_path)
            _mark_existing_file_as_success(output_path, release_ts, video_name)
            write_log(f"{video_name} скачано успешно.", log_type="info")
            return True
        except PermissionError as e:
            last_error = e
            write_log(
                f"{video_name}: не удалось завершить файл (попытка {attempt}/5), файл занят другим процессом: {e}",
                log_type="info"
            )
            time.sleep(0.7)
        except FileNotFoundError:
            if os.path.exists(output_path):
                _mark_existing_file_as_success(output_path, release_ts, video_name)
                write_log(f"{video_name} уже был финализирован ранее.", log_type="info")
                return True
            break
        except Exception as e:
            last_error = e
            break

    if os.path.exists(output_path):
        output_size = os.path.getsize(output_path)
        if expected_size > 0 and sizes_match(output_size, expected_size, tolerance_percent=0.003):
            _mark_existing_file_as_success(output_path, release_ts, video_name)
            write_log(f"{video_name} уже существует после финализации, считаем загрузку успешной.", log_type="info")
            return True

    if last_error is not None:
        write_log(f"{video_name}: не удалось финализировать файл: {last_error}", log_type="error")

    return False


def _update_slot_progress(slot: ProgressSlotUI, worker_idx: int, video_name: str, downloaded: int, total_size: int, start_time: float):
    progress_percent = int(downloaded / total_size * 100) if total_size > 0 else 0
    speed = downloaded / 1024 / max(time.time() - start_time, 1)
    title = f"Поток {worker_idx + 1}: {video_name}"
    status = f"{progress_percent}% | {_format_bytes(downloaded)} / {_format_bytes(total_size)} | {speed:.1f} KB/s"
    slot.set_state(title, progress_percent, status)


def _download_record_with_resume(
    worker_idx: int,
    slot: ProgressSlotUI,
    output_folder: str,
    pause_event: threading.Event,
    blacklist: set[str],
    record: dict[str, Any],
) -> str:
    global current_video_name, current_video_url

    record = _normalize_record(record)
    record = _ensure_fresh_record(record, blacklist)
    if record is None:
        return DOWNLOAD_STATUS_FAILED

    video_name = record["video_name"]
    current_video_name = video_name
    current_video_url = record.get("direct_url")

    file_lock = _get_path_lock(video_name)
    if not file_lock.acquire(blocking=False):
        write_log(
            f"{video_name}: уже обрабатывается другим потоком, пропускаем дубликат.",
            log_type="info"
        )
        return DOWNLOAD_STATUS_SKIPPED

    try:
        for num in blacklist:
            if num and num in video_name:
                write_log(f"Пропуск {video_name}: имя содержит число {num} из черного списка.", log_type="info")
                return DOWNLOAD_STATUS_SKIPPED

        output_path = _final_output_path(output_folder, video_name)
        temp_path = _temp_output_path(output_path)
        expected_size = int(record.get("size_bytes") or 0)

        if os.path.exists(output_path):
            existing_size = os.path.getsize(output_path)
            if expected_size > 0 and sizes_match(existing_size, expected_size, tolerance_percent=0.003):
                _mark_existing_file_as_success(output_path, record.get("release_ts"), video_name)
                write_log(f"{video_name} уже скачано.", log_type="info")
                return DOWNLOAD_STATUS_SKIPPED

        retry_count = 0
        last_error = None
        start_time = time.time()

        while retry_count < MAX_REFRESH_RETRIES:
            pause_event.wait()

            if stop_downloading_flag:
                return DOWNLOAD_STATUS_SKIPPED

            if _finalize_completed_download(temp_path, output_path, expected_size, record.get("release_ts"), video_name):
                return DOWNLOAD_STATUS_DOWNLOADED

            record = _ensure_fresh_record(record, blacklist)
            if record is None:
                return DOWNLOAD_STATUS_FAILED

            direct_url = record["direct_url"]
            current_video_url = direct_url

            total_written = os.path.getsize(temp_path) if os.path.exists(temp_path) else 0
            headers = {}
            file_mode = "ab"

            if total_written > 0:
                headers["Range"] = f"bytes={total_written}-"

            try:
                with requests.get(direct_url, headers=headers, stream=True, timeout=(20, 120)) as response:
                    if response.status_code == 416:
                        if _finalize_completed_download(temp_path, output_path, expected_size, record.get("release_ts"), video_name):
                            return DOWNLOAD_STATUS_DOWNLOADED
                        raise RuntimeError("Сервер вернул 416 Requested Range Not Satisfiable, а временный файл не выглядит завершённым")

                    if response.status_code in (401, 403):
                        raise RuntimeError(f"Сервер вернул {response.status_code}, ссылка протухла или недействительна")

                    response.raise_for_status()

                    if total_written > 0 and response.status_code == 200:
                        write_log(f"{video_name}: сервер не поддержал продолжение, начинаем файл заново.", log_type="info")
                        total_written = 0
                        file_mode = "wb"

                    if expected_size <= 0:
                        content_length = int(response.headers.get("Content-Length", 0) or 0)
                        if response.status_code == 206 and total_written > 0:
                            expected_size = total_written + content_length
                        else:
                            expected_size = content_length
                        record["size_bytes"] = expected_size
                        _upsert_record(record)

                    with open(temp_path, file_mode) as file:
                        downloaded_this_attempt = total_written
                        _update_slot_progress(slot, worker_idx, video_name, downloaded_this_attempt, expected_size, start_time)

                        for chunk in response.iter_content(CHUNK_SIZE):
                            if not chunk:
                                continue

                            pause_event.wait()
                            file.write(chunk)
                            downloaded_this_attempt += len(chunk)
                            total_written = downloaded_this_attempt
                            _update_slot_progress(slot, worker_idx, video_name, downloaded_this_attempt, expected_size, start_time)

                    if expected_size > 0 and not sizes_match(total_written, expected_size, tolerance_percent=0.003):
                        raise RuntimeError(
                            f"Размер после загрузки не совпал: скачано={total_written}, ожидалось={expected_size}"
                        )

                    if _finalize_completed_download(temp_path, output_path, expected_size, record.get("release_ts"), video_name):
                        return DOWNLOAD_STATUS_DOWNLOADED

                    raise RuntimeError("Не удалось финализировать полностью скачанный файл")

            except Exception as e:
                if _finalize_completed_download(temp_path, output_path, expected_size, record.get("release_ts"), video_name):
                    return DOWNLOAD_STATUS_DOWNLOADED

                retry_count += 1
                last_error = e
                write_log(f"Ошибка скачивания {video_name}, попытка {retry_count}/{MAX_REFRESH_RETRIES}: {e}", log_type="error")

                if retry_count >= MAX_REFRESH_RETRIES:
                    break

                refreshed = _refresh_record_from_page(record, blacklist)
                if refreshed is not None:
                    record = refreshed
                time.sleep(1.0)

        write_log(f"Окончательная ошибка при скачивании {video_name}: {last_error}", log_type="error")
        save_failed_link(record.get("page_url") or record.get("direct_url") or video_name)
        return DOWNLOAD_STATUS_FAILED
    finally:
        file_lock.release()


def download_videos_sequential(
    root,
    download_folder,
    pause_event,
    stop_after_skip: bool = False,
    direction: str = "сначала",
    start_url: str | None = None,
    search_pause_event: threading.Event | None = None,
    stop_on_empty_pages: bool = False,
    prefetch_limit: int | Callable = PREFETCH_LIMIT,
    progress_parent=None,
    progress_panel: ProgressPanelUI | None = None,
    lead_status_callback=None,
):
    global stop_downloading_flag, stop_collecting_flag, is_downloading_video, current_video_name, current_video_url

    worker_count = _get_worker_count()
    blacklist = load_blacklist("blacklist.txt")
    state_lock = threading.Lock()
    skip_streak = {"value": 0}
    downloaded_count = {"value": 0}

    external_progress_panel = progress_panel is not None
    progress_panel = progress_panel or ProgressPanelUI(progress_parent or root, worker_count)
    worker_count = _ensure_progress_slots(progress_panel, worker_count)

    task_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    collector_done_event = threading.Event()
    start_download_event = threading.Event()

    lead_condition = threading.Condition()
    lead_state = {
        "condition": lead_condition,
        "outstanding": 0,
        "produced": 0,
        "completed": 0,
        "limit": _resolve_lead_limit(prefetch_limit),
        "update_ui": lambda: None,
    }

    def _emit_lead_status():
        if lead_status_callback is None:
            return
        try:
            lead_status_callback(lead_state["outstanding"], lead_state["limit"])
        except Exception as e:
            write_log(f"Не удалось обновить индикатор опережения сбора: {e}", log_type="error")

    lead_state["update_ui"] = _emit_lead_status
    _emit_lead_status()

    stop_downloading_flag = False
    stop_collecting_flag = False
    is_downloading_video = True

    collector_thread = None

    try:
        if start_url:
            if search_pause_event is None:
                search_pause_event = threading.Event()
                search_pause_event.set()

            write_log(
                f"Запуск конвейера: сбор ссылок с допустимым опережением до {_resolve_lead_limit(prefetch_limit)} и параллельная загрузка {worker_count} потоками.",
                log_type="info"
            )

            collector_thread = threading.Thread(
                target=collect_video_links,
                kwargs={
                    "start_url": start_url,
                    "search_pause_event": search_pause_event,
                    "stop_on_empty_pages": stop_on_empty_pages,
                    "output_queue": task_queue,
                    "start_download_event": start_download_event,
                    "collector_done_event": collector_done_event,
                    "prefetch_limit": prefetch_limit,
                    "lead_state": lead_state,
                },
                daemon=True,
            )
            collector_thread.start()
        else:
            records = _load_link_records()
            if not records:
                write_log("Файл video_links.txt пуст или не содержит валидных записей.", log_type="info")
                return

            if direction == "с конца":
                records.reverse()

            seen_video_names = set()
            for record in records:
                video_name = (record.get("video_name") or "").strip()
                if video_name:
                    if video_name in seen_video_names:
                        write_log(f"Дубликат по имени файла пропущен: {video_name}", log_type="info")
                        continue
                    seen_video_names.add(video_name)
                task_queue.put(record)
                with lead_condition:
                    lead_state["outstanding"] += 1
                    lead_state["produced"] += 1
                    lead_state["limit"] = _resolve_lead_limit(prefetch_limit)
                    _emit_lead_status()

            collector_done_event.set()
            start_download_event.set()
            with lead_condition:
                lead_state["condition"].notify_all()
            write_log(
                f"Начало многопоточной загрузки из video_links.txt. CPU={os.cpu_count() or 'unknown'}, используется потоков: {worker_count}.",
                log_type="info"
            )

        def worker_loop(worker_idx: int):
            if worker_idx >= len(progress_panel.slots):
                write_log(
                    f"Пропуск запуска потока {worker_idx + 1}: для него не создан UI-слот прогресса.",
                    log_type="error"
                )
                return

            slot = progress_panel.slots[worker_idx]
            slot.set_idle()

            start_download_event.wait()

            while True:
                if stop_downloading_flag and task_queue.empty():
                    break

                try:
                    record = task_queue.get(timeout=QUEUE_GET_TIMEOUT_SECONDS)
                except queue.Empty:
                    if collector_done_event.is_set():
                        break
                    continue

                video_label = record.get("video_name") or record.get("page_url") or "video"
                slot.set_state(f"Поток {worker_idx + 1}: подготовка", 0, video_label, force=True)

                try:
                    result = _download_record_with_resume(
                        worker_idx=worker_idx,
                        slot=slot,
                        output_folder=download_folder,
                        pause_event=pause_event,
                        blacklist=blacklist,
                        record=record,
                    )

                    with state_lock:
                        if result == DOWNLOAD_STATUS_SKIPPED:
                            skip_streak["value"] += 1
                        elif result == DOWNLOAD_STATUS_DOWNLOADED:
                            skip_streak["value"] = 0
                            downloaded_count["value"] += 1
                            _log_downloaded_videos_count(downloaded_count["value"])

                        if stop_after_skip and skip_streak["value"] >= 10:
                            write_log("Остановка загрузки: 10 подряд завершившихся пропусков.", log_type="info")
                            globals()["stop_downloading_flag"] = True
                            globals()["stop_collecting_flag"] = True

                    with lead_condition:
                        if lead_state["outstanding"] > 0:
                            lead_state["outstanding"] -= 1
                        lead_state["completed"] += 1
                        lead_state["limit"] = _resolve_lead_limit(prefetch_limit)
                        _emit_lead_status()
                        lead_condition.notify_all()
                except Exception as e:
                    write_log(f"Неперехваченная ошибка в потоке {worker_idx + 1}: {e}", log_type="error")
                    save_failed_link(record.get("page_url") or record.get("direct_url") or video_label)
                finally:
                    task_queue.task_done()
                    slot.set_idle()

        threads = [
            threading.Thread(target=worker_loop, args=(idx,), daemon=True)
            for idx in range(worker_count)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        if collector_thread is not None:
            collector_thread.join()

        with lead_condition:
            lead_state["limit"] = _resolve_lead_limit(prefetch_limit)
            _emit_lead_status()
            lead_condition.notify_all()

        if stop_downloading_flag:
            write_log("Загрузка остановлена по запросу пользователя.", log_type="info")
        else:
            write_log("Многопоточная загрузка завершена.", log_type="info")

    finally:
        if not external_progress_panel:
            progress_panel.destroy()
        else:
            for slot in progress_panel.slots:
                slot.set_idle()
        current_video_name = None
        current_video_url = None
        is_downloading_video = False
        stop_downloading_flag = False
        stop_collecting_flag = False
        _emit_lead_status()


def download_video_sequential(driver, root, video_link, download_folder, pause_event, blacklist, progress_parent=None):
    try:
        record = _collect_direct_record_from_player_page(driver, video_link, blacklist)
        if record is None:
            return DOWNLOAD_STATUS_SKIPPED

        _upsert_record(record)
        temp_panel = ProgressPanelUI(progress_parent or root, 1)
        try:
            return _download_record_with_resume(
                worker_idx=0,
                slot=temp_panel.slots[0],
                output_folder=download_folder,
                pause_event=pause_event,
                blacklist=blacklist,
                record=record,
            )
        finally:
            temp_panel.destroy()
    except Exception as e:
        write_log(f"Ошибка при обработке видео {video_link}: {e}", log_type="error")
        save_failed_link(video_link)
        return DOWNLOAD_STATUS_FAILED
