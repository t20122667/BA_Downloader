import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from utils import write_log, cookies_path, load_config

# Глобальная переменная для драйвера
driver = None

def authorize(timer_label, check_button, root):
    def browser_thread():
        global driver
        config = load_config()   # ← ВОТ ЭТА СТРОКА ОБЯЗАТЕЛЬНА
        username = config.get("username")
        password = config.get("password")
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service)
            driver.get("https://beautifulagony.com/public/main.php?page=login")
            driver.find_element(By.NAME, "username").send_keys(username)
            driver.find_element(By.NAME, "userpass").send_keys(password)
            driver.find_element(By.NAME, "userpass").send_keys(Keys.RETURN)
            timer_label.configure(text="Решите капчу и нажмите 'Проверить авторизацию'!")
            check_button.configure(state="normal")
        except Exception as e:
            write_log(f"Ошибка: {e}", log_type="error")
            if driver:
                driver.quit()
    threading.Thread(target=browser_thread, daemon=True).start()

def check_authorization(timer_label, root):
    global driver
    try:
        current_url = driver.current_url
        if current_url == "https://beautifulagony.com/public/main.php":
            write_log("Авторизация завершена.", log_type="info")
        else:
            write_log("Не удалось пройти авторизацию. Проверьте капчу.", log_type="error")
    except Exception as e:
        write_log(f"Ошибка: {e}", log_type="error")

def save_cookies(driver):
    import pickle
    with open(cookies_path, "wb") as file:
        pickle.dump(driver.get_cookies(), file)
