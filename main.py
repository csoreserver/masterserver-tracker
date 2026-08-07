import socket
import time
import pathlib
import json
from os import (
    environ,
)
import dotenv
import requests
from watchdog.events import (
    FileSystemEventHandler,
    FileModifiedEvent,
)
from watchdog.observers.api import (
    ObservedWatch,
)
from watchdog.observers import (
    Observer,
)



DATASTORE = {}
DATASTORE_PATH = "datastore.json"
def load_datastore ():
    global DATASTORE
    ds = pathlib.Path(DATASTORE_PATH)
    if not ds.exists():
        with ds.open("w") as fp:
            json.dump({}, fp)
    with ds.open("r") as fp:
        DATASTORE = json.load(fp)

def save_datastore ():
    global DATASTORE
    ds = pathlib.Path(DATASTORE_PATH)
    with ds.open("r+") as fp:
        json.dump(DATASTORE, fp)


CONFIG_PATH = ".env"
def load_config ():
    global DATASTORE_PATH
    dotenv.load_dotenv(override=True)
    DATASTORE_PATH = environ["CSORSE_MASTERSERVER_DATASTORE_PATH"]


def check_server () -> dict:
    timeout = int(environ["CSORSE_MASTERSERVER_TIMEOUT"])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.settimeout(timeout)
            s.connect((
                environ["CSORSE_MASTERSERVER_IP"],
                int(environ["CSORSE_MASTERSERVER_PORT"])
            ))
            s.settimeout(timeout)
            data = s.recv(1024)
            if data not in [b"~SERVERCONNECTED\n", b"~SERVERCONNECTED\n\x00"]:
                raise RuntimeError("Not a master server instance")
        except (ConnectionRefusedError, TimeoutError):
            return  {
                "success": True,
                "online": False,
            }
        except Exception as error:
            return {
                "success": False,
                "error": f"{type(error).__name__}: {error}"
            }
    return  {
        "success": True,
        "online": True,
    }


def update_discord (check_result):
    ts_cur = int(time.time() // 1)
    ts_next = ts_cur + int(environ["CSORSE_MASTERSERVER_UPDATE_INTERVAL"])

    status = "🔴Offline"
    if check_result["success"]:
        status = "🟢Online" if check_result["online"] else "🔴Offline"
    else:
        status = f"⁉️Error\n{check_result['error']}"

    data=json.dumps({
        "embeds": [
        {
            "title": "CSO Re-Server Master Server Tracker",
            "fields": [
                {
                    "name": "Last Time Checked",
                    "value": f"<t:{ts_cur}:s>",
                    "inline": False
                },
                {
                    "name": "Status",
                    "value": status,
                    "inline": False
                },
                {
                    "name": "Next Check Time",
                    "value": f"<t:{ts_next}:R>",
                    "inline": False
                }
            ]
        }
        ]
    })
    headers = {
        "Content-Type": "application/json",
    }
    wh_url = environ["DISCORD_WEBHOOK_URL"]

    msg_id = DATASTORE.get("ms_discord_msg_id")
    if msg_id:
        r = requests.get(f"{wh_url}/messages/{msg_id}")
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            msg_id = None

    if msg_id:
        r = requests.patch(
            f"{wh_url}/messages/{msg_id}",
            headers=headers,
            data=data,
        )
    else:
        r = requests.post(
            wh_url,
            headers=headers,
            params={
                "wait": "true"
            },
            data=data,
        )
    # Export the data for use in future steps
    return r.json()


def main ():
    load_config()
    load_datastore()

    class ModifiedHandler (FileSystemEventHandler):
        def on_modified(self, event: FileModifiedEvent):
            if type(event) is not FileModifiedEvent:
                return
            src_path = pathlib.Path(event.src_path).resolve(False)
            if src_path == pathlib.Path(CONFIG_PATH).resolve(False):
                print(f"{CONFIG_PATH} has been modified. Reload now...")
                load_config()
            elif src_path == pathlib.Path(DATASTORE_PATH).resolve(False):
                print(f"{DATASTORE_PATH} has been modified. Reload now...")
                load_datastore()

    filewatcher = Observer()
    filewatcher.schedule(ModifiedHandler(), "./", recursive=False)
    filewatcher.start()

    while True:
        print(f"Checking server status: {environ['CSORSE_MASTERSERVER_IP']} {environ['CSORSE_MASTERSERVER_PORT']}")
        status = check_server()
        print(f"Response: {status}")
        print(f"Send Discord Message: {'New' if 'ms_discord_msg_id' not in DATASTORE else DATASTORE['ms_discord_msg_id']}")
        response = update_discord(status)
        print(f"Response: {response}")
        DATASTORE["ms_discord_msg_id"] = response["id"]
        save_datastore()
        interval = float(environ["CSORSE_MASTERSERVER_UPDATE_INTERVAL"])
        print(f"Sleep for {interval} second(s)")
        print(f"==============================")
        time.sleep(interval)

if __name__ == "__main__":
    main()
